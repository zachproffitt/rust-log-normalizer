//! Per-connection processing pipeline.
//!
//! Frames the connection into lines with a bounded length (so one client can't
//! OOM the process), detects the wire format from the first usable line and
//! **locks** it for the connection, transforms each line, and forwards batches of
//! serialized records to the shared sink.
//!
//! Lines are read in ready-chunks (producer-side batching with a latency bound),
//! and the per-batch `sink.send().await` is where lossless backpressure is felt.
//! Undetectable or malformed lines are logged and dropped while the connection
//! keeps reading. An over-limit line desyncs framing, so it closes the connection
//! (after flushing already-parsed records); a well-behaved client can reconnect.

use std::net::SocketAddr;
use std::time::Duration;

use tokio::net::TcpStream;
use tokio_stream::StreamExt;
use tokio_util::codec::{Framed, LinesCodec, LinesCodecError};

use crate::format::{self, Format};
use crate::sink::SinkHandle;
use crate::transform;

/// Max lines a connection batches before yielding to the sink.
const PRODUCER_BATCH_MAX: usize = 256;
/// Max time a partial batch waits before being flushed to the sink.
const BATCH_TIMEOUT: Duration = Duration::from_millis(50);

/// Drives a single accepted connection to completion (EOF or read error).
pub async fn run_connection(
    stream: TcpStream,
    peer: SocketAddr,
    sink: SinkHandle,
    max_line_bytes: usize,
) -> std::io::Result<()> {
    eprintln!("accepted connection from {peer}");

    let framed = Framed::new(stream, LinesCodec::new_with_max_length(max_line_bytes));
    let chunks = framed.chunks_timeout(PRODUCER_BATCH_MAX, BATCH_TIMEOUT);
    tokio::pin!(chunks);

    // `None` until the first detectable line locks the format for this connection.
    let mut format: Option<Format> = None;

    while let Some(chunk) = chunks.next().await {
        let mut batch = Vec::with_capacity(chunk.len());
        // An over-limit line desyncs framing, so we flush what we have and close
        // the connection rather than guess where the next record begins.
        let mut close = false;

        for item in chunk {
            let line = match item {
                Ok(line) => line,
                Err(LinesCodecError::MaxLineLengthExceeded) => {
                    eprintln!(
                        "connection {peer}: line exceeded {max_line_bytes} bytes, closing connection"
                    );
                    close = true;
                    break;
                }
                Err(LinesCodecError::Io(err)) => return Err(err),
            };

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

            match transform::transform(fmt, &line) {
                Ok(event) => match serde_json::to_string(&event) {
                    Ok(json) => batch.push(json),
                    Err(err) => {
                        eprintln!("connection {peer}: failed to serialize event ({err})")
                    }
                },
                Err(err) => {
                    eprintln!("connection {peer}: failed to transform event ({err}), dropping line")
                }
            }
        }

        if !batch.is_empty() {
            // Awaits when the sink queue is full → lossless backpressure.
            if let Err(err) = sink.send(batch).await {
                eprintln!("connection {peer}: {err}, closing");
                break;
            }
        }

        if close {
            break;
        }
    }

    eprintln!("closed connection from {peer}");
    Ok(())
}
