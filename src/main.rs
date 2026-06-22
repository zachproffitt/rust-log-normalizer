use std::net::SocketAddr;
use std::sync::Arc;

use clap::Parser;
use tokio::net::TcpListener;
use tokio::sync::Semaphore;

mod format;
mod pipeline;
mod sink;
mod transform;

#[derive(Debug, Parser)]
#[command(version, about, long_about = None)]
struct Cli {
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

    /// Maximum length of a single input line, in bytes. Longer lines are dropped
    /// to bound per-connection memory.
    #[arg(long, default_value_t = 1_048_576)]
    max_line_bytes: usize,

    /// Capacity (in batches) of the queue between connections and the sink.
    #[arg(long, default_value_t = 256)]
    queue_capacity: usize,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    // Single sink consumer task; connection tasks send record batches to it.
    let (sink, _writer) = sink::spawn(&cli.output, cli.queue_capacity).await?;
    eprintln!("writing normalized records to {}", describe(&cli.output));

    let limit = Arc::new(Semaphore::new(cli.max_connections));

    let addr = SocketAddr::from(([0, 0, 0, 0], cli.port));
    let listener = TcpListener::bind(addr).await?;
    eprintln!("listening on {addr}");

    loop {
        // Acquire a slot before accepting, so at capacity we stop accepting and
        // let connections queue in the OS backlog (backpressure).
        let permit = limit
            .clone()
            .acquire_owned()
            .await
            .expect("connection semaphore is never closed");

        let (stream, peer) = listener.accept().await?;
        let sink = sink.clone();
        let max_line_bytes = cli.max_line_bytes;
        // Handle each connection independently so one client cannot block others.
        tokio::spawn(async move {
            let _permit = permit; // released when this task ends, freeing a slot
            if let Err(err) = pipeline::run_connection(stream, peer, sink, max_line_bytes).await {
                eprintln!("connection {peer} ended with error: {err}");
            }
        });
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
