use std::net::SocketAddr;

use clap::Parser;
use tokio::net::TcpListener;

mod format;
mod pipeline;
mod transform;

#[derive(Debug, Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    /// TCP port to listen on.
    #[arg(short, long, default_value_t = 5044)]
    port: u16,
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let cli = Cli::parse();

    let addr = SocketAddr::from(([0, 0, 0, 0], cli.port));
    let listener = TcpListener::bind(addr).await?;
    println!("listening on {addr}");

    loop {
        let (stream, peer) = listener.accept().await?;
        // Handle each connection independently so one client cannot block others.
        tokio::spawn(async move {
            if let Err(err) = pipeline::run_connection(stream, peer).await {
                eprintln!("connection {peer} ended with error: {err}");
            }
        });
    }
}
