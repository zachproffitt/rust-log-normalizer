//! Output sink for normalized records.
//!
//! A [`Sink`] is a cheaply-cloneable handle to a single shared writer (stdout or
//! a file). Connections run in independent tasks, so writes are serialized
//! through an async mutex; each record is written as one NDJSON line and flushed.

use std::sync::Arc;

use anyhow::Context;
use tokio::io::{AsyncWrite, AsyncWriteExt, BufWriter};
use tokio::sync::Mutex;

/// A shareable handle to the process-wide output destination.
#[derive(Clone)]
pub struct Sink {
    inner: Arc<Mutex<Box<dyn AsyncWrite + Send + Unpin>>>,
}

impl Sink {
    /// Builds a sink from the `--output` value: `-` writes to stdout, anything
    /// else is treated as a file path opened for appending (created if missing).
    pub async fn from_output(output: &str) -> anyhow::Result<Sink> {
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

        Ok(Sink {
            inner: Arc::new(Mutex::new(writer)),
        })
    }

    /// Writes one normalized record as an NDJSON line and flushes it.
    pub async fn write_record(&self, json: &str) -> std::io::Result<()> {
        let mut writer = self.inner.lock().await;
        writer.write_all(json.as_bytes()).await?;
        writer.write_all(b"\n").await?;
        writer.flush().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn stdout_sink_constructs() {
        Sink::from_output("-").await.expect("stdout sink");
    }

    #[tokio::test]
    async fn file_sink_appends_ndjson_lines() {
        let path = std::env::temp_dir().join(format!("sink-test-{}.ndjson", std::process::id()));
        let path_str = path.to_str().unwrap();
        let _ = std::fs::remove_file(&path);

        // First sink writes one record, then is dropped.
        {
            let sink = Sink::from_output(path_str).await.unwrap();
            sink.write_record(r#"{"a":1}"#).await.unwrap();
        }
        // A second sink to the same path should append, not truncate.
        {
            let sink = Sink::from_output(path_str).await.unwrap();
            sink.write_record(r#"{"b":2}"#).await.unwrap();
        }

        let contents = std::fs::read_to_string(&path).unwrap();
        assert_eq!(contents, "{\"a\":1}\n{\"b\":2}\n");

        let _ = std::fs::remove_file(&path);
    }
}
