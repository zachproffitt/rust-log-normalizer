use std::net::SocketAddr;

use clap::Parser;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

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
            if let Err(err) = handle_connection(stream, peer).await {
                eprintln!("connection {peer} ended with error: {err}");
            }
        });
    }
}

async fn handle_connection(stream: TcpStream, peer: SocketAddr) -> std::io::Result<()> {
    println!("accepted connection from {peer}");

    let reader = BufReader::new(stream);
    let mut lines = reader.lines();

    while let Some(line) = lines.next_line().await? {
        println!("{peer}: {line}");
    }

    println!("closed connection from {peer}");
    Ok(())
}
