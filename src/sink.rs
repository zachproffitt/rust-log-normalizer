//! Output sink for normalized records.
//!
//! A single consumer task owns the underlying writer (stdout or an appended
//! file); connection tasks send batches of serialized records to it over a
//! **bounded** channel. The bound provides lossless backpressure (a full channel
//! makes producers await), and the consumer coalesces ready batches into a single
//! buffered write + flush, so we don't pay a syscall per record.

use anyhow::Context;
use tokio::io::{AsyncWrite, AsyncWriteExt, BufWriter};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::error;

/// Upper bound on how many records the consumer coalesces into one flush, so a
/// sustained burst can't grow the write buffer without limit.
const MAX_COALESCED_RECORDS: usize = 4096;

/// A cheaply-cloneable handle that connection tasks use to enqueue record
/// batches for the sink consumer.
#[derive(Clone)]
pub struct SinkHandle {
    tx: mpsc::Sender<Vec<String>>,
}

impl SinkHandle {
    /// Enqueues a batch of serialized records. Awaits when the channel is full,
    /// propagating backpressure to the caller (and thus to the TCP sender).
    ///
    /// Errors only if the consumer task has stopped (channel closed).
    pub async fn send(&self, batch: Vec<String>) -> anyhow::Result<()> {
        self.tx
            .send(batch)
            .await
            .context("sink consumer has stopped")
    }
}

/// Builds a sink: opens the destination, spawns the consumer task, and returns a
/// handle plus the consumer's `JoinHandle`. `capacity` bounds the channel (in
/// batches). `output` is `-` for stdout or a file path (append, created if absent).
pub async fn spawn(output: &str, capacity: usize) -> anyhow::Result<(SinkHandle, JoinHandle<()>)> {
    let writer: Box<dyn AsyncWrite + Send + Unpin> = if output == "-" {
        Box::new(tokio::io::stdout())
    } else {
        let file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(output)
            .await
            .with_context(|| format!("failed to open output file: {output}"))?;
        Box::new(BufWriter::new(file))
    };

    let (tx, rx) = mpsc::channel::<Vec<String>>(capacity.max(1));
    let consumer = tokio::spawn(consume(writer, rx));

    Ok((SinkHandle { tx }, consumer))
}

/// Drains the channel until all senders are dropped, batching writes and flushing
/// once per drained burst.
///
/// A write or flush error stops the consumer: the receiver is dropped, which
/// closes the channel so producers see a clear error (and connections close)
/// rather than the sink silently discarding records forever.
async fn consume(mut writer: Box<dyn AsyncWrite + Send + Unpin>, mut rx: mpsc::Receiver<Vec<String>>) {
    'drain: while let Some(first) = rx.recv().await {
        let mut written = 0usize;
        if let Err(err) = write_batch(&mut writer, &first, &mut written).await {
            error!(%err, "sink write failed; stopping sink, further records will be rejected");
            break 'drain;
        }

        // Coalesce any batches that are already queued into the same flush.
        while written < MAX_COALESCED_RECORDS {
            match rx.try_recv() {
                Ok(batch) => {
                    if let Err(err) = write_batch(&mut writer, &batch, &mut written).await {
                        error!(%err, "sink write failed; stopping sink, further records will be rejected");
                        break 'drain;
                    }
                }
                Err(_) => break,
            }
        }

        if let Err(err) = writer.flush().await {
            error!(%err, "sink flush failed; stopping sink, further records will be rejected");
            break 'drain;
        }
    }

    // Best-effort final flush of anything already written (no-op if we stopped on error).
    let _ = writer.flush().await;
}

/// Writes every record in `batch` as one NDJSON line, advancing `written`.
async fn write_batch(
    writer: &mut (impl AsyncWrite + Unpin),
    batch: &[String],
    written: &mut usize,
) -> std::io::Result<()> {
    for record in batch {
        writer.write_all(record.as_bytes()).await?;
        writer.write_all(b"\n").await?;
        *written += 1;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use super::*;

    /// An `AsyncWrite` whose every write fails, for exercising sink error paths.
    struct FailingWriter;

    impl AsyncWrite for FailingWriter {
        fn poll_write(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            _: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            Poll::Ready(Err(std::io::Error::other("write boom")))
        }
        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
        fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn stdout_sink_spawns() {
        let (_handle, _join) = spawn("-", 8).await.expect("stdout sink");
    }

    #[tokio::test]
    async fn write_failure_stops_sink_and_rejects_further_records() {
        let (tx, rx) = mpsc::channel::<Vec<String>>(1);
        let consumer = tokio::spawn(consume(Box::new(FailingWriter), rx));
        let handle = SinkHandle { tx };

        // The first batch is accepted into the channel; the consumer then fails
        // writing it, stops, and drops the receiver.
        let _ = handle.send(vec!["{}".to_string()]).await;

        // Once the consumer has stopped, further sends must error rather than
        // being silently swallowed.
        let mut rejected = false;
        for _ in 0..100 {
            if handle.send(vec!["{}".to_string()]).await.is_err() {
                rejected = true;
                break;
            }
        }
        assert!(rejected, "sink should reject records after a write failure");
        consumer.await.unwrap();
    }

    #[tokio::test]
    async fn file_sink_appends_all_records() {
        let path = std::env::temp_dir().join(format!("sink-test-{}.ndjson", std::process::id()));
        let path_str = path.to_str().unwrap();
        let _ = std::fs::remove_file(&path);

        // First run writes one batch, then the handle is dropped so the consumer
        // sees the channel close and exits after flushing.
        {
            let (handle, join) = spawn(path_str, 8).await.unwrap();
            handle.send(vec![r#"{"a":1}"#.to_string()]).await.unwrap();
            drop(handle);
            join.await.unwrap();
        }
        // A second run must append, not truncate.
        {
            let (handle, join) = spawn(path_str, 8).await.unwrap();
            handle
                .send(vec![r#"{"b":2}"#.to_string(), r#"{"c":3}"#.to_string()])
                .await
                .unwrap();
            drop(handle);
            join.await.unwrap();
        }

        let contents = std::fs::read_to_string(&path).unwrap();
        assert_eq!(contents, "{\"a\":1}\n{\"b\":2}\n{\"c\":3}\n");

        let _ = std::fs::remove_file(&path);
    }
}
