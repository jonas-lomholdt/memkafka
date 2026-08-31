//! ControlRecordTypeSchema
//!
//! See the schema for this message [here](https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/ControlRecordTypeSchema.json).
// WARNING: the items of this module are generated and should not be edited directly
#![allow(unused)]

use std::borrow::Borrow;
use std::collections::BTreeMap;

use anyhow::{bail, Result};
use bytes::Bytes;
use uuid::Uuid;

use crate::protocol::{
    buf::{ByteBuf, ByteBufMut},
    compute_unknown_tagged_fields_size, types, write_unknown_tagged_fields, Decodable, Decoder,
    Encodable, Encoder, HeaderVersion, Message, StrBytes, VersionRange,
};

/// Valid versions: 0
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct ControlRecordTypeSchema {
    /// The type of the control record, such as commit or abort
    ///
    /// Supported API versions: 0
    pub _type: i16,
}

impl ControlRecordTypeSchema {
    /// Sets `_type` to the passed value.
    ///
    /// The type of the control record, such as commit or abort
    ///
    /// Supported API versions: 0
    pub fn with_type(mut self, value: i16) -> Self {
        self._type = value;
        self
    }
}

impl Encodable for ControlRecordTypeSchema {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        types::Int16.encode(buf, &self._type)?;

        Ok(())
    }
    fn compute_size(&self, version: i16) -> Result<usize> {
        let mut total_size = 0;
        total_size += types::Int16.compute_size(&self._type)?;

        Ok(total_size)
    }
}

impl Decodable for ControlRecordTypeSchema {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        let _type = types::Int16.decode(buf)?;
        Ok(Self { _type })
    }
}

impl Default for ControlRecordTypeSchema {
    fn default() -> Self {
        Self { _type: 0 }
    }
}

impl Message for ControlRecordTypeSchema {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 0 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}
