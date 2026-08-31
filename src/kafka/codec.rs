use std::{error::Error, fmt};

use anyhow::{Context, Result};
use bytes::{Buf, Bytes, BytesMut};
use kafka_protocol::{
    messages::{ApiKey, RequestHeader, RequestKind, ResponseHeader, ResponseKind},
    protocol::{Decodable, Encodable},
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RequestPrefix {
    pub raw_api_key: i16,
    pub api_version: i16,
    pub correlation_id: i32,
}

#[allow(clippy::large_enum_variant)] // Preserve the explicit, unboxed routing boundary.
#[derive(Debug, PartialEq)]
pub enum DecodedFrame {
    Request(DecodedRequest),
    UnsupportedApiVersions {
        header: RequestHeader,
        requested_version: i16,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DecodeStage {
    Header,
    Body,
    TrailingBytes,
}

impl fmt::Display for DecodeStage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Header => formatter.write_str("header"),
            Self::Body => formatter.write_str("body"),
            Self::TrailingBytes => formatter.write_str("trailing bytes"),
        }
    }
}

#[derive(Debug)]
pub enum RequestDecodeError {
    TruncatedPrefix,
    UnknownApiKey {
        prefix: RequestPrefix,
    },
    VersionOutOfSchema {
        prefix: RequestPrefix,
        api_key: ApiKey,
    },
    Malformed {
        prefix: Option<RequestPrefix>,
        api_key: Option<ApiKey>,
        stage: DecodeStage,
        source: anyhow::Error,
    },
}

impl fmt::Display for RequestDecodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TruncatedPrefix => formatter.write_str("truncated Kafka request prefix"),
            Self::UnknownApiKey { prefix } => write!(
                formatter,
                "unknown Kafka API key {} v{} (correlation ID {})",
                prefix.raw_api_key, prefix.api_version, prefix.correlation_id
            ),
            Self::VersionOutOfSchema { prefix, api_key } => write!(
                formatter,
                "Kafka {api_key:?} v{} is outside its generated schema range (correlation ID {})",
                prefix.api_version, prefix.correlation_id
            ),
            Self::Malformed {
                prefix,
                api_key,
                stage,
                ..
            } => write!(
                formatter,
                "malformed Kafka request {stage} (prefix: {prefix:?}, API key: {api_key:?})"
            ),
        }
    }
}

impl Error for RequestDecodeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Malformed { source, .. } => Some(source.as_ref()),
            Self::TruncatedPrefix
            | Self::UnknownApiKey { .. }
            | Self::VersionOutOfSchema { .. } => None,
        }
    }
}

pub fn decode_frame(mut frame: Bytes) -> std::result::Result<DecodedFrame, RequestDecodeError> {
    if frame.remaining() < 8 {
        return Err(RequestDecodeError::TruncatedPrefix);
    }

    let mut fixed_prefix = frame.slice(0..8);
    let prefix = RequestPrefix {
        raw_api_key: fixed_prefix.get_i16(),
        api_version: fixed_prefix.get_i16(),
        correlation_id: fixed_prefix.get_i32(),
    };
    let api_key =
        ApiKey::try_from(prefix.raw_api_key).map_err(|_| RequestDecodeError::UnknownApiKey {
            prefix: prefix.clone(),
        })?;
    let valid_versions = api_key.valid_versions();
    if api_key != ApiKey::ApiVersions
        && (prefix.api_version < valid_versions.min || prefix.api_version > valid_versions.max)
    {
        return Err(RequestDecodeError::VersionOutOfSchema { prefix, api_key });
    }

    let header = RequestHeader::decode(
        &mut frame,
        api_key.request_header_version(prefix.api_version),
    )
    .with_context(|| {
        format!(
            "failed to decode Kafka {api_key:?} v{} request header",
            prefix.api_version
        )
    })
    .map_err(|source| RequestDecodeError::Malformed {
        prefix: Some(prefix.clone()),
        api_key: Some(api_key),
        stage: DecodeStage::Header,
        source,
    })?;

    if api_key == ApiKey::ApiVersions
        && (prefix.api_version < valid_versions.min || prefix.api_version > valid_versions.max)
    {
        return Ok(DecodedFrame::UnsupportedApiVersions {
            header,
            requested_version: prefix.api_version,
        });
    }

    let body = RequestKind::decode(api_key, &mut frame, prefix.api_version)
        .with_context(|| {
            format!(
                "failed to decode Kafka {api_key:?} v{} request body",
                prefix.api_version
            )
        })
        .map_err(|source| RequestDecodeError::Malformed {
            prefix: Some(prefix.clone()),
            api_key: Some(api_key),
            stage: DecodeStage::Body,
            source,
        })?;
    if frame.has_remaining() {
        return Err(RequestDecodeError::Malformed {
            prefix: Some(prefix),
            api_key: Some(api_key),
            stage: DecodeStage::TrailingBytes,
            source: anyhow::anyhow!("Kafka request has {} trailing bytes", frame.remaining()),
        });
    }

    Ok(DecodedFrame::Request(DecodedRequest {
        header,
        api_key,
        body,
    }))
}

pub fn encode_response(
    api_key: ApiKey,
    encoding_version: i16,
    correlation_id: i32,
    body: &ResponseKind,
) -> Result<Bytes> {
    let mut encoded = BytesMut::new();
    ResponseHeader::default()
        .with_correlation_id(correlation_id)
        .encode(
            &mut encoded,
            api_key.response_header_version(encoding_version),
        )
        .context("failed to encode Kafka response header")?;
    body.encode(&mut encoded, encoding_version)
        .with_context(|| {
            format!("failed to encode Kafka {api_key:?} v{encoding_version} response")
        })?;
    Ok(encoded.freeze())
}
