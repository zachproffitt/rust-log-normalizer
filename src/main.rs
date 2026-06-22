use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

mod format;
mod pipeline;
mod sink;
mod transform;

use sink::SinkHandle;

/// Brief backoff after a transient accept error, to avoid a hot loop while a
/// recoverable condition (e.g. too many open files) persists.
const ACCEPT_BACKOFF: Duration = Duration::from_millis(50);

#[derive(Debug, Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    /// Address to bind the listener to.
    #[arg(long, default_value = "0.0.0.0")]
    bind: String,

    /// TCP port to listen on.
    #[arg(short, long, default_value_t = 5044)]
    port: u16,

    /// Output destination for normalized records: `-` for stdout, or a file path
    /// (opened for appending, created if missing).
    #[arg(short, long, default_value = "-")]
    output: String,

    /// Maximum number of connections handled concurrently. At the limit, new
    /// connections wait in the OS backlog until a slot frees.
    #[arg(long, default_value_t = 1024)]
    max_connections: usize,

    /// Maximum length of a single input line, in bytes. A longer line closes the
    /// connection to bound per-connection memory.
    #[arg(long, default_value_t = 1_048_576)]
    max_line_bytes: usize,

    /// Capacity (in batches) of the queue between connections and the sink.
    #[arg(long, default_value_t = 256)]
    queue_capacity: usize,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    // Logs go to stderr (via RUST_LOG, default `info`) so stdout stays a clean
    // NDJSON stream when the sink is stdout.
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    // Single sink consumer task; connection tasks send record batches to it.
    let (sink, writer) = sink::spawn(&cli.output, cli.queue_capacity).await?;
    info!(destination = %describe(&cli.output), "writing normalized records");

    let limit = Arc::new(Semaphore::new(cli.max_connections));

    let addr = SocketAddr::new(cli.bind.parse()?, cli.port);
    let listener = TcpListener::bind(addr).await?;
    info!(%addr, "listening");

    // Trigger graceful shutdown on Ctrl-C / SIGTERM.
    let shutdown = CancellationToken::new();
    {
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            wait_for_shutdown_signal().await;
            info!("shutdown signal received, draining");
            shutdown.cancel();
        });
    }

    serve(listener, sink, limit, cli.max_line_bytes, shutdown).await?;

    // `serve` has returned, dropping its sink handle; once in-flight connections
    // finish they drop theirs, the channel closes, and the consumer drains and
    // flushes. Wait for that to complete so no buffered records are lost.
    let _ = writer.await;
    info!("shutdown complete");
    Ok(())
}

/// Accept loop: gates concurrency with `limit`, spawns a task per connection, and
/// returns when `shutdown` is triggered. Transient accept errors are logged and
/// retried rather than terminating the server.
async fn serve(
    listener: TcpListener,
    sink: SinkHandle,
    limit: Arc<Semaphore>,
    max_line_bytes: usize,
    shutdown: CancellationToken,
) -> std::io::Result<()> {
    loop {
        // Acquire a slot before accepting, so at capacity we stop accepting and
        // let connections queue in the OS backlog (backpressure).
        let permit = tokio::select! {
            biased;
            () = shutdown.cancelled() => break,
            permit = limit.clone().acquire_owned() => {
                permit.expect("connection semaphore is never closed")
            }
        };

        let (stream, peer) = tokio::select! {
            biased;
            () = shutdown.cancelled() => break,
            accepted = listener.accept() => match accepted {
                Ok(pair) => pair,
                Err(err) => {
                    warn!(%err, "accept error, retrying");
                    drop(permit);
                    tokio::time::sleep(ACCEPT_BACKOFF).await;
                    continue;
                }
            }
        };

        let sink = sink.clone();
        let shutdown = shutdown.clone();
        // Handle each connection independently so one client cannot block others.
        tokio::spawn(async move {
            let _permit = permit; // released when this task ends, freeing a slot
            if let Err(err) =
                pipeline::run_connection(stream, peer, sink, max_line_bytes, shutdown).await
            {
                warn!(%peer, %err, "connection ended with error");
            }
        });
    }

    Ok(())
}

/// Resolves when the process receives Ctrl-C or (on Unix) SIGTERM.
async fn wait_for_shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut term = signal(SignalKind::terminate()).expect("install SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

/// Human-readable description of the output destination for startup logging.
fn describe(output: &str) -> String {
    if output == "-" {
        "stdout".to_string()
    } else {
        format!("file {output}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpStream;

    #[tokio::test]
    async fn serve_normalizes_over_tcp_and_shuts_down() {
        let path = std::env::temp_dir().join(format!("serve-test-{}.ndjson", std::process::id()));
        let path_str = path.to_str().unwrap().to_string();
        let _ = std::fs::remove_file(&path);

        let (sink, writer) = sink::spawn(&path_str, 8).await.unwrap();
        let limit = Arc::new(Semaphore::new(4));
        let shutdown = CancellationToken::new();

        // Bind an ephemeral port and run the accept loop in the background.
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let serve_handle = tokio::spawn(serve(listener, sink, limit, 8192, shutdown.clone()));

        // Connect, send one syslog event, then close the write side (EOF).
        let mut client = TcpStream::connect(addr).await.unwrap();
        client
            .write_all(b"<134>Dec 05 10:30:45 host CEF:0|v|p|1|4624|logon|6|src=1.2.3.4 act=allow\n")
            .await
            .unwrap();
        client.shutdown().await.unwrap();

        // Wait until the record has flowed all the way to the sink file before
        // triggering shutdown, so the assertion isn't racing the pipeline.
        let mut contents = String::new();
        for _ in 0..100 {
            contents = std::fs::read_to_string(&path).unwrap_or_default();
            if !contents.lines().next().unwrap_or_default().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        // Trigger shutdown, wait for the accept loop and the sink to drain.
        shutdown.cancel();
        serve_handle.await.unwrap().unwrap();
        let _ = writer.await;

        let _ = std::fs::remove_file(&path);
        assert_eq!(contents.lines().count(), 1);
        assert!(contents.contains(r#""source.ip":"1.2.3.4""#));
    }
}
