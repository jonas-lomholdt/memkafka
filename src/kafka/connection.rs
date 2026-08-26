use anyhow::{Context, Result};
use tokio::{net::TcpStream, sync::watch};

use super::{
    codec::{decode_request, encode_response},
    dispatcher::Dispatcher,
    frame::{read_frame, write_frame},
};

pub async fn serve(
    mut connection: TcpStream,
    dispatcher: Dispatcher,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return Ok(());
                }
            }
            handled = handle_one(&mut connection, &dispatcher) => {
                if !handled? {
                    return Ok(());
                }
            }
        }
    }
}

async fn handle_one(connection: &mut TcpStream, dispatcher: &Dispatcher) -> Result<bool> {
    let Some(frame) = read_frame(connection)
        .await
        .context("failed to read Kafka request frame")?
    else {
        return Ok(false);
    };
    let request = decode_request(frame)?;
    let response = dispatcher.dispatch(&request).await?;
    if !request.expects_response() {
        return Ok(true);
    }
    let encoded = encode_response(
        request.api_key,
        request.header.request_api_version,
        request.header.correlation_id,
        &response,
    )?;
    write_frame(connection, &encoded)
        .await
        .context("failed to write Kafka response frame")?;
    Ok(true)
}
