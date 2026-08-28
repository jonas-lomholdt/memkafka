use std::{io, net::SocketAddr, sync::Arc};

use clap::Parser;
use serde::Serialize;
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufWriter},
    net::{TcpListener, TcpSocket, TcpStream},
    sync::Mutex,
};

const MAX_FRAME_SIZE: usize = 100 * 1024 * 1024;

#[derive(Debug, Parser)]
#[command(name = "kafka-api-version-proxy")]
struct Arguments {
    #[arg(long)]
    listen: SocketAddr,
    #[arg(long)]
    upstream: SocketAddr,
    #[arg(long)]
    scenario: String,
    #[arg(long)]
    output: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct Observation<'a> {
    scenario: &'a str,
    api_key: i16,
    api_version: i16,
    client_id: Option<&'a str>,
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let arguments = Arguments::parse();
    let listener = bind_listener(arguments.listen)?;
    println!("READY listen={}", listener.local_addr()?);

    let output = Arc::new(Mutex::new(BufWriter::new(
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(arguments.output)
            .await?,
    )));
    let scenario = Arc::new(arguments.scenario);

    loop {
        let (client, _) = listener.accept().await?;
        let output = Arc::clone(&output);
        let scenario = Arc::clone(&scenario);
        tokio::spawn(async move {
            if let Err(error) =
                forward_connection(client, arguments.upstream, scenario, output).await
            {
                eprintln!("connection forwarding failed: {error}");
            }
        });
    }
}

fn bind_listener(address: SocketAddr) -> io::Result<TcpListener> {
    let socket = match address {
        SocketAddr::V4(_) => TcpSocket::new_v4()?,
        SocketAddr::V6(_) => TcpSocket::new_v6()?,
    };
    socket.set_reuseaddr(true)?;
    socket.bind(address)?;
    socket.listen(1024)
}

async fn forward_connection(
    client: TcpStream,
    upstream_address: SocketAddr,
    scenario: Arc<String>,
    output: Arc<Mutex<BufWriter<File>>>,
) -> io::Result<()> {
    let upstream = TcpStream::connect(upstream_address).await?;
    let (client_read, mut client_write) = client.into_split();
    let (mut upstream_read, upstream_write) = upstream.into_split();

    tokio::try_join!(
        forward_requests(client_read, upstream_write, scenario, output),
        tokio::io::copy(&mut upstream_read, &mut client_write),
    )?;
    Ok(())
}

async fn forward_requests<R, W>(
    mut client: R,
    mut upstream: W,
    scenario: Arc<String>,
    output: Arc<Mutex<BufWriter<File>>>,
) -> io::Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    loop {
        let mut prefix = [0_u8; 4];
        match client.read_exact(&mut prefix).await {
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(error) => return Err(error),
        }

        let frame_length = i32::from_be_bytes(prefix);
        if frame_length < 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Kafka frame length must not be negative",
            ));
        }
        let frame_length = frame_length as usize;
        if frame_length > MAX_FRAME_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Kafka frame exceeds the 100 MiB recorder limit",
            ));
        }

        let mut frame = vec![0; frame_length];
        client.read_exact(&mut frame).await?;
        if let Some((api_key, api_version, client_id)) = parse_request_header(&frame) {
            let observation = Observation {
                scenario: scenario.as_str(),
                api_key,
                api_version,
                client_id,
            };
            if let Err(error) = append_observation(&output, &observation).await {
                eprintln!("observation write failed: {error}");
            }
        }
        upstream.write_all(&prefix).await?;
        upstream.write_all(&frame).await?;
    }
}

async fn append_observation(
    output: &Mutex<BufWriter<File>>,
    observation: &Observation<'_>,
) -> io::Result<()> {
    let mut line = serde_json::to_vec(observation)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    line.push(b'\n');
    let mut output = output.lock().await;
    output.write_all(&line).await?;
    output.flush().await
}

fn parse_request_header(frame: &[u8]) -> Option<(i16, i16, Option<&str>)> {
    const FIXED_HEADER_BYTES: usize = 10;
    if frame.len() < FIXED_HEADER_BYTES {
        return None;
    }
    let api_key = i16::from_be_bytes(frame[0..2].try_into().ok()?);
    let api_version = i16::from_be_bytes(frame[2..4].try_into().ok()?);
    let client_id_length = i16::from_be_bytes(frame[8..10].try_into().ok()?);
    let client_id = match client_id_length {
        -1 => None,
        length if length >= 0 => {
            let end = FIXED_HEADER_BYTES.checked_add(length as usize)?;
            let client_id_bytes = frame.get(FIXED_HEADER_BYTES..end)?;
            Some(std::str::from_utf8(client_id_bytes).ok()?)
        }
        _ => return None,
    };
    Some((api_key, api_version, client_id))
}
