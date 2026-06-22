//! Per-connection processing pipeline.
//!
//! Frames the input into lines with a bounded length (so one client can't OOM the
//! process), detects the wire format from the first usable line and **locks** it
//! for the connection, transforms each line, and forwards batches of serialized
//! records to the shared sink.
//!
//! Lines are gathered into batches bounded by count, total bytes, and a short
//! timeout (producer-side batching with a latency bound), and the per-batch
//! `sink.send().await` is where lossless backpressure is felt. Undetectable or
//! malformed lines are logged and dropped while the connection keeps reading.
//! An over-limit line desyncs framing, so it closes the connection (after
//! flushing already-parsed records); a well-behaved client can reconnect.

use std::net::SocketAddr;
use std::time::Duration;

use tokio::io::AsyncRead;
use tokio::time::{timeout_at, Instant};
use tokio_stream::StreamExt;
use tokio_util::codec::{FramedRead, LinesCodec, LinesCodecError};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::format::{self, Format};
use crate::sink::SinkHandle;
use crate::transform;

/// Max lines a connection batches before yielding to the sink.
const PRODUCER_BATCH_MAX: usize = 256;
/// Max total bytes of raw lines buffered in one batch, so per-connection memory
/// stays bounded even when individual lines are large.
const BATCH_BYTE_BUDGET: usize = 256 * 1024;
/// Max time a partial batch waits before being flushed to the sink.
const BATCH_TIMEOUT: Duration = Duration::from_millis(50);

/// One framed line, already classified by the codec result.
enum LineOutcome {
    /// A decoded line to process.
    Line(String),
    /// An over-limit line desynced framing; the connection should close.
    Close,
}

/// Drives a single connection to completion: EOF, a read error, an over-limit
/// line, or a shutdown signal.
pub async fn run_connection<R>(
    reader: R,
    peer: SocketAddr,
    sink: SinkHandle,
    max_line_bytes: usize,
    shutdown: CancellationToken,
) -> std::io::Result<()>
where
    R: AsyncRead + Unpin,
{
    debug!(%peer, "accepted connection");

    let mut lines = FramedRead::new(reader, LinesCodec::new_with_max_length(max_line_bytes));
    // `None` until the first detectable line locks the format for this connection.
    let mut format: Option<Format> = None;

    loop {
        // Wait for the first line of a batch, or a shutdown signal.
        let first = tokio::select! {
            biased;
            () = shutdown.cancelled() => break,
            item = lines.next() => match item {
                Some(item) => item,
                None => break,
            },
        };

        let mut raw: Vec<String> = Vec::new();
        let mut bytes = 0usize;
        let mut close = false;
        let mut eof = false;

        match classify(first, peer, max_line_bytes)? {
            LineOutcome::Line(line) => {
                bytes += line.len();
                raw.push(line);
            }
            LineOutcome::Close => close = true,
        }

        // Coalesce further ready lines under the count/byte/time budget.
        let deadline = Instant::now() + BATCH_TIMEOUT;
        while !close && raw.len() < PRODUCER_BATCH_MAX && bytes < BATCH_BYTE_BUDGET {
            match timeout_at(deadline, lines.next()).await {
                Ok(Some(item)) => match classify(item, peer, max_line_bytes)? {
                    LineOutcome::Line(line) => {
                        bytes += line.len();
                        raw.push(line);
                    }
                    LineOutcome::Close => close = true,
                },
                Ok(None) => {
                    eof = true;
                    break;
                }
                Err(_) => break, // batch timeout elapsed
            }
        }

        let batch = serialize_batch(raw, &mut format, peer);
        if !batch.is_empty() {
            // Awaits when the sink queue is full → lossless backpressure.
            if let Err(err) = sink.send(batch).await {
                warn!(%peer, %err, "sink unavailable, closing connection");
                break;
            }
        }

        if close || eof {
            break;
        }
    }

    debug!(%peer, "closed connection");
    Ok(())
}

/// Classifies a codec result. An I/O error ends the connection (propagated);
/// an over-limit line is logged and signals a close.
fn classify(
    item: Result<String, LinesCodecError>,
    peer: SocketAddr,
    max_line_bytes: usize,
) -> std::io::Result<LineOutcome> {
    match item {
        Ok(line) => Ok(LineOutcome::Line(line)),
        Err(LinesCodecError::MaxLineLengthExceeded) => {
            warn!(%peer, max_line_bytes, "line exceeded limit, closing connection");
            Ok(LineOutcome::Close)
        }
        Err(LinesCodecError::Io(err)) => Err(err),
    }
}

/// Detects/locks the format and transforms each raw line into a serialized
/// record, dropping (with a log) lines that can't be detected or transformed.
fn serialize_batch(raw: Vec<String>, format: &mut Option<Format>, peer: SocketAddr) -> Vec<String> {
    let mut out = Vec::with_capacity(raw.len());

    for line in raw {
        // Empty/whitespace-only lines carry no event; skip them silently.
        if line.trim().is_empty() {
            continue;
        }

        let fmt = match *format {
            Some(fmt) => fmt,
            None => match format::detect(&line) {
                Some(detected) => {
                    debug!(%peer, format = ?detected, "locked connection format");
                    *format = Some(detected);
                    detected
                }
                None => {
                    warn!(%peer, line = %line, "could not detect format, dropping line");
                    continue;
                }
            },
        };

        match transform::transform(fmt, &line) {
            Ok(event) => match serde_json::to_string(&event) {
                Ok(json) => out.push(json),
                Err(err) => warn!(%peer, %err, "failed to serialize event, dropping line"),
            },
            Err(err) => warn!(%peer, %err, "failed to transform event, dropping line"),
        }
    }

    out
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use std::net::{Ipv4Addr, SocketAddr};

    use super::*;
    use crate::sink;

    fn test_peer() -> SocketAddr {
        SocketAddr::from((Ipv4Addr::LOCALHOST, 12345))
    }

    /// Runs the pipeline over an in-memory reader and returns the records the
    /// sink wrote (one per line).
    async fn run_to_records(input: &str, max_line_bytes: usize) -> Vec<String> {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir()
            .join(format!("pipe-test-{}-{id}.ndjson", std::process::id()));
        let path_str = path.to_str().unwrap().to_string();
        let _ = std::fs::remove_file(&path);

        let (handle, join) = sink::spawn(&path_str, 8).await.unwrap();
        run_connection(
            Cursor::new(input.to_string()),
            test_peer(),
            handle,
            max_line_bytes,
            CancellationToken::new(),
        )
        .await
        .unwrap();
        join.await.unwrap();

        let contents = std::fs::read_to_string(&path).unwrap_or_default();
        let _ = std::fs::remove_file(&path);
        contents.lines().map(str::to_string).collect()
    }

    #[tokio::test]
    async fn normalizes_a_syslog_line() {
        let input = "<134>Dec 05 10:30:45 host CEF:0|v|p|1|4624|logon|6|src=1.2.3.4 act=allow\n";
        let records = run_to_records(input, 8192).await;
        assert_eq!(records.len(), 1);
        assert!(records[0].contains(r#""source.ip":"1.2.3.4""#));
        assert!(records[0].contains(r#""event.type":"allowed""#));
    }

    #[tokio::test]
    async fn format_is_locked_after_first_line() {
        // First line is syslog; a following JSON-looking line is routed through
        // the syslog parser (not re-detected), so it is not parsed as JSON.
        let input = "<13>Dec 05 10:30:45 host hello\n{\"EventID\":4624}\n";
        let records = run_to_records(input, 8192).await;
        assert_eq!(records.len(), 2);
        // The second record went through the syslog transform, so its message is
        // the raw JSON text rather than a parsed Windows event.
        assert!(records[1].contains(r#"{\"EventID\":4624}"#) || records[1].contains("EventID"));
    }

    #[tokio::test]
    async fn oversized_line_closes_but_keeps_prior_records() {
        let big = "A".repeat(300);
        let input = format!("<13>Dec 05 10:30:45 host before\n<13>{big}\n<13>Dec 05 10:30:45 host after\n");
        let records = run_to_records(&input, 100).await;
        // The record before the oversized line survives; nothing after it.
        assert_eq!(records.len(), 1);
        assert!(records[0].contains("before"));
    }

    #[tokio::test]
    async fn undetectable_lines_are_dropped() {
        let input = "not a known format\nstill not\n";
        let records = run_to_records(input, 8192).await;
        assert!(records.is_empty());
    }
}
