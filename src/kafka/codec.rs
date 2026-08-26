use anyhow::{Context, Result, ensure};
use bytes::{Buf, Bytes, BytesMut};
use kafka_protocol::{
    messages::{ApiKey, RequestHeader, RequestKind, ResponseHeader, ResponseKind},
    protocol::{Encodable, decode_request_header_from_buffer},
};

#[derive(Clone, Debug, PartialEq)]
pub struct DecodedRequest {
    pub header: RequestHeader,
    pub api_key: ApiKey,
    pub body: RequestKind,
}

impl DecodedRequest {
    pub fn expects_response(&self) -> bool {
        !matches!(&self.body, RequestKind::Produce(request) if request.acks == 0)
    }
}

pub fn decode_request(mut frame: Bytes) -> Result<DecodedRequest> {
    let header = decode_request_header_from_buffer(&mut frame)
        .context("failed to decode Kafka request header")?;
    let api_key = ApiKey::try_from(header.request_api_key)
        .map_err(|_| anyhow::anyhow!("unknown Kafka API key {}", header.request_api_key))?;
    let body = RequestKind::decode(api_key, &mut frame, header.request_api_version).with_context(
        || {
            format!(
                "failed to decode Kafka {api_key:?} v{} request body",
                header.request_api_version
            )
        },
    )?;
    ensure!(
        !frame.has_remaining(),
        "Kafka request has {} trailing bytes",
        frame.remaining()
    );

    Ok(DecodedRequest {
        header,
        api_key,
        body,
    })
}

pub fn encode_response(
    api_key: ApiKey,
    api_version: i16,
    correlation_id: i32,
    body: &ResponseKind,
) -> Result<Bytes> {
    let mut encoded = BytesMut::new();
    ResponseHeader::default()
        .with_correlation_id(correlation_id)
        .encode(&mut encoded, api_key.response_header_version(api_version))
        .context("failed to encode Kafka response header")?;
    body.encode(&mut encoded, api_version)
        .with_context(|| format!("failed to encode Kafka {api_key:?} v{api_version} response"))?;
    Ok(encoded.freeze())
}
