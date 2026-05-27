use anyhow::{Context, Result};
use clap::Parser;
use std::net::SocketAddr;
use tokio::io::{self, AsyncReadExt};
use tokio::net::UdpSocket;

#[derive(Debug, Parser)]
#[command(
    name = "udp-send",
    about = "Send stdin to an av-mesh raw UDP ingest socket"
)]
struct Args {
    target: SocketAddr,

    #[arg(long, default_value_t = 1316)]
    chunk_bytes: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let mut input = Vec::new();
    io::stdin()
        .read_to_end(&mut input)
        .await
        .context("failed to read stdin")?;

    let bind_addr: SocketAddr = if args.target.is_ipv6() {
        "[::]:0".parse().unwrap()
    } else {
        "0.0.0.0:0".parse().unwrap()
    };
    let socket = UdpSocket::bind(bind_addr)
        .await
        .with_context(|| format!("failed to create raw UDP sender socket for {}", args.target))?;
    let chunk_bytes = args.chunk_bytes.max(1);

    for chunk in input.chunks(chunk_bytes) {
        socket
            .send_to(chunk, args.target)
            .await
            .with_context(|| format!("failed to send raw UDP chunk to {}", args.target))?;
    }

    println!(
        "sent {} bytes to {} using raw UDP chunks of {} bytes",
        input.len(),
        args.target,
        chunk_bytes
    );
    Ok(())
}
