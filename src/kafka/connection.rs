use anyhow::{Context, Result};
use kafka_protocol::messages::ApiKey;
use tokio::{net::TcpStream, sync::watch};
use tracing::warn;

use super::{
    codec::{DecodedFrame, RequestDecodeError, decode_frame, encode_response},
    dispatcher::Dispatcher,
    frame::{read_frame, write_frame},
    request_router::{ResponseEnvelope, Route, RouteError, route},
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
    let decoded = match decode_frame(frame) {
        Ok(decoded) => decoded,
        Err(error) => {
            log_decode_failure(&error);
            return Err(error.into());
        }
    };
    let routed = match route(&decoded) {
        Ok(routed) => routed,
        Err(error) => {
            log_route_failure(&decoded, &error);
            return Err(error.into());
        }
    };

    match routed {
        Route::Dispatch(request) => {
            let expects_response = request.expects_response();
            let envelope = ResponseEnvelope {
                api_key: request.api_key(),
                encoding_version: request.header().request_api_version,
                correlation_id: request.header().correlation_id,
                body: dispatcher.dispatch(request).await?,
            };
            if !expects_response {
                return Ok(true);
            }
            write_response(connection, envelope).await?;
        }
        Route::Respond(envelope) => write_response(connection, envelope).await?,
        Route::NoResponse => return Ok(true),
    }
    Ok(true)
}

async fn write_response(connection: &mut TcpStream, envelope: ResponseEnvelope) -> Result<()> {
    let encoded = encode_response(
        envelope.api_key,
        envelope.encoding_version,
        envelope.correlation_id,
        &envelope.body,
    )
    .with_context(|| {
        format!(
            "failed to encode Kafka {:?} v{} response for correlation ID {}",
            envelope.api_key, envelope.encoding_version, envelope.correlation_id
        )
    })?;
    write_frame(connection, &encoded)
        .await
        .context("failed to write Kafka response frame")?;
    Ok(())
}

fn log_decode_failure(error: &RequestDecodeError) {
    match error {
        RequestDecodeError::TruncatedPrefix => warn!(
            decode_stage = "prefix",
            "closing Kafka connection after truncated request prefix"
        ),
        RequestDecodeError::UnknownApiKey { prefix } => warn!(
            raw_api_key = prefix.raw_api_key,
            version = prefix.api_version,
            correlation_id = prefix.correlation_id,
            decode_stage = "api_key",
            "closing Kafka connection after unknown API key"
        ),
        RequestDecodeError::VersionOutOfSchema { prefix, api_key } => warn!(
            raw_api_key = prefix.raw_api_key,
            api_key = ?api_key,
            version = prefix.api_version,
            correlation_id = prefix.correlation_id,
            decode_stage = "schema_range",
            "closing Kafka connection after out-of-schema request version"
        ),
        RequestDecodeError::Malformed {
            prefix,
            api_key,
            stage,
            ..
        } => warn!(
            raw_api_key = prefix.as_ref().map(|prefix| prefix.raw_api_key),
            api_key = ?api_key,
            version = prefix.as_ref().map(|prefix| prefix.api_version),
            correlation_id = prefix.as_ref().map(|prefix| prefix.correlation_id),
            decode_stage = %stage,
            "closing Kafka connection after malformed request"
        ),
    }
}

fn log_route_failure(frame: &DecodedFrame, error: &RouteError) {
    match error {
        RouteError::UnsupportedApi {
            api_key,
            version,
            correlation_id,
        } => warn!(
            raw_api_key = *api_key as i16,
            api_key = ?api_key,
            version,
            correlation_id,
            "unsupported_api"
        ),
        RouteError::ErrorResponse(source) => match frame {
            DecodedFrame::Request(request) => warn!(
                raw_api_key = request.api_key as i16,
                api_key = ?request.api_key,
                version = request.header.request_api_version,
                correlation_id = request.header.correlation_id,
                %source,
                "closing Kafka connection after request routing failure"
            ),
            DecodedFrame::UnsupportedApiVersions {
                header,
                requested_version,
            } => warn!(
                raw_api_key = ApiKey::ApiVersions as i16,
                api_key = ?ApiKey::ApiVersions,
                version = requested_version,
                correlation_id = header.correlation_id,
                %source,
                "closing Kafka connection after request routing failure"
            ),
        },
    }
}
