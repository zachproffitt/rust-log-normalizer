//! Per-connection processing pipeline.
//!
//! Reads a connection line-by-line (shared framing for both formats), detects
//! the wire format from the first usable line and **locks** it for the lifetime
//! of the connection, then routes every subsequent line to the transform.
//!
//! Errors are non-fatal: an undetectable or malformed line is logged to stderr
//! and dropped, and the connection keeps reading.

use std::net::SocketAddr;

use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::TcpStream;

use crate::format::{self, Format};
use crate::sink::Sink;
use crate::transform;

/// Drives a single accepted connection to completion (EOF or read error).
pub async fn run_connection(
    stream: TcpStream,
    peer: SocketAddr,
    sink: Sink,
) -> std::io::Result<()> {
    eprintln!("accepted connection from {peer}");

    let reader = BufReader::new(stream);
    let mut lines = reader.lines();

    // `None` until the first detectable line locks the format for this connection.
    let mut format: Option<Format> = None;

    while let Some(line) = lines.next_line().await? {
        // Empty/whitespace-only lines carry no event; skip them silently.
        if line.trim().is_empty() {
            continue;
        }

        let fmt = match format {
            Some(fmt) => fmt,
            None => match format::detect(&line) {
                Some(detected) => {
                    eprintln!("connection {peer} locked to format {detected:?}");
                    format = Some(detected);
                    detected
                }
                None => {
                    eprintln!(
                        "connection {peer}: could not detect format, dropping line: {line}"
                    );
                    continue;
                }
            },
        };

        // Normalized records go to the sink (one NDJSON line each); diagnostics to stderr.
        match transform::transform(fmt, &line) {
            Ok(event) => match serde_json::to_string(&event) {
                Ok(json) => {
                    if let Err(err) = sink.write_record(&json).await {
                        eprintln!("connection {peer}: failed to write record ({err})");
                    }
                }
                Err(err) => eprintln!("connection {peer}: failed to serialize event ({err})"),
            },
            Err(err) => {
                eprintln!("connection {peer}: failed to transform event ({err}), dropping line")
            }
        }
    }

    eprintln!("closed connection from {peer}");
    Ok(())
}
