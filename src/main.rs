use std::net::SocketAddr;

use clap::Parser;
use tokio::net::TcpListener;

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
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let sink = sink::Sink::from_output(&cli.output).await?;
    eprintln!("writing normalized records to {}", describe(&cli.output));

    let addr = SocketAddr::from(([0, 0, 0, 0], cli.port));
    let listener = TcpListener::bind(addr).await?;
    eprintln!("listening on {addr}");

    loop {
        let (stream, peer) = listener.accept().await?;
        let sink = sink.clone();
        // Handle each connection independently so one client cannot block others.
        tokio::spawn(async move {
            if let Err(err) = pipeline::run_connection(stream, peer, sink).await {
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
