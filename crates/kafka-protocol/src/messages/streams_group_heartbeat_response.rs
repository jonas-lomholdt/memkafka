//! StreamsGroupHeartbeatResponse
//!
//! See the schema for this message [here](https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/StreamsGroupHeartbeatResponse.json).
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
pub struct Endpoint {
    /// host of the endpoint
    ///
    /// Supported API versions: 0
    pub host: StrBytes,

    /// port of the endpoint
    ///
    /// Supported API versions: 0
    pub port: u16,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl Endpoint {
    /// Sets `host` to the passed value.
    ///
    /// host of the endpoint
    ///
    /// Supported API versions: 0
    pub fn with_host(mut self, value: StrBytes) -> Self {
        self.host = value;
        self
    }
    /// Sets `port` to the passed value.
    ///
    /// port of the endpoint
    ///
    /// Supported API versions: 0
    pub fn with_port(mut self, value: u16) -> Self {
        self.port = value;
        self
    }
    /// Sets unknown_tagged_fields to the passed value.
    pub fn with_unknown_tagged_fields(mut self, value: BTreeMap<i32, Bytes>) -> Self {
        self.unknown_tagged_fields = value;
        self
    }
    /// Inserts an entry into unknown_tagged_fields.
    pub fn with_unknown_tagged_field(mut self, key: i32, value: Bytes) -> Self {
        self.unknown_tagged_fields.insert(key, value);
        self
    }
}

#[cfg(feature = "broker")]
impl Encodable for Endpoint {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        types::CompactString.encode(buf, &self.host)?;
        types::UInt16.encode(buf, &self.port)?;
        let num_tagged_fields = self.unknown_tagged_fields.len();
        if num_tagged_fields > std::u32::MAX as usize {
            bail!(
                "Too many tagged fields to encode ({} fields)",
                num_tagged_fields
            );
        }
        types::UnsignedVarInt.encode(buf, num_tagged_fields as u32)?;

        write_unknown_tagged_fields(buf, 0.., &self.unknown_tagged_fields)?;
        Ok(())
    }
    fn compute_size(&self, version: i16) -> Result<usize> {
        let mut total_size = 0;
        total_size += types::CompactString.compute_size(&self.host)?;
        total_size += types::UInt16.compute_size(&self.port)?;
        let num_tagged_fields = self.unknown_tagged_fields.len();
        if num_tagged_fields > std::u32::MAX as usize {
            bail!(
                "Too many tagged fields to encode ({} fields)",
                num_tagged_fields
            );
        }
        total_size += types::UnsignedVarInt.compute_size(num_tagged_fields as u32)?;

        total_size += compute_unknown_tagged_fields_size(&self.unknown_tagged_fields)?;
        Ok(total_size)
    }
}

#[cfg(feature = "client")]
impl Decodable for Endpoint {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        let host = types::CompactString.decode(buf)?;
        let port = types::UInt16.decode(buf)?;
        let mut unknown_tagged_fields = BTreeMap::new();
        let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
        for _ in 0..num_tagged_fields {
            let tag: u32 = types::UnsignedVarInt.decode(buf)?;
            let size: u32 = types::UnsignedVarInt.decode(buf)?;
            let unknown_value = buf.try_get_bytes(size as usize)?;
            unknown_tagged_fields.insert(tag as i32, unknown_value);
        }
        Ok(Self {
            host,
            port,
            unknown_tagged_fields,
        })
    }
}

impl Default for Endpoint {
    fn default() -> Self {
        Self {
            host: Default::default(),
            port: 0,
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for Endpoint {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 0 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 0
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct EndpointToPartitions {
    /// User-defined endpoint to connect to the node
    ///
    /// Supported API versions: 0
    pub user_endpoint: Endpoint,

    /// All topic partitions materialized by active tasks on the node
    ///
    /// Supported API versions: 0
    pub active_partitions: Vec<TopicPartition>,

    /// All topic partitions materialized by standby tasks on the node
    ///
    /// Supported API versions: 0
    pub standby_partitions: Vec<TopicPartition>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl EndpointToPartitions {
    /// Sets `user_endpoint` to the passed value.
    ///
    /// User-defined endpoint to connect to the node
    ///
    /// Supported API versions: 0
    pub fn with_user_endpoint(mut self, value: Endpoint) -> Self {
        self.user_endpoint = value;
        self
    }
    /// Sets `active_partitions` to the passed value.
    ///
    /// All topic partitions materialized by active tasks on the node
    ///
    /// Supported API versions: 0
    pub fn with_active_partitions(mut self, value: Vec<TopicPartition>) -> Self {
        self.active_partitions = value;
        self
    }
    /// Sets `standby_partitions` to the passed value.
    ///
    /// All topic partitions materialized by standby tasks on the node
    ///
    /// Supported API versions: 0
    pub fn with_standby_partitions(mut self, value: Vec<TopicPartition>) -> Self {
        self.standby_partitions = value;
        self
    }
    /// Sets unknown_tagged_fields to the passed value.
    pub fn with_unknown_tagged_fields(mut self, value: BTreeMap<i32, Bytes>) -> Self {
        self.unknown_tagged_fields = value;
        self
    }
    /// Inserts an entry into unknown_tagged_fields.
    pub fn with_unknown_tagged_field(mut self, key: i32, value: Bytes) -> Self {
        self.unknown_tagged_fields.insert(key, value);
        self
    }
}

#[cfg(feature = "broker")]
impl Encodable for EndpointToPartitions {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        types::Struct { version }.encode(buf, &self.user_endpoint)?;
        types::CompactArray(types::Struct { version }).encode(buf, &self.active_partitions)?;
        types::CompactArray(types::Struct { version }).encode(buf, &self.standby_partitions)?;
        let num_tagged_fields = self.unknown_tagged_fields.len();
        if num_tagged_fields > std::u32::MAX as usize {
            bail!(
                "Too many tagged fields to encode ({} fields)",
                num_tagged_fields
            );
        }
        types::UnsignedVarInt.encode(buf, num_tagged_fields as u32)?;

        write_unknown_tagged_fields(buf, 0.., &self.unknown_tagged_fields)?;
        Ok(())
    }
    fn compute_size(&self, version: i16) -> Result<usize> {
        let mut total_size = 0;
        total_size += types::Struct { version }.compute_size(&self.user_endpoint)?;
        total_size +=
            types::CompactArray(types::Struct { version }).compute_size(&self.active_partitions)?;
        total_size += types::CompactArray(types::Struct { version })
            .compute_size(&self.standby_partitions)?;
        let num_tagged_fields = self.unknown_tagged_fields.len();
        if num_tagged_fields > std::u32::MAX as usize {
            bail!(
                "Too many tagged fields to encode ({} fields)",
                num_tagged_fields
            );
        }
        total_size += types::UnsignedVarInt.compute_size(num_tagged_fields as u32)?;

        total_size += compute_unknown_tagged_fields_size(&self.unknown_tagged_fields)?;
        Ok(total_size)
    }
}

#[cfg(feature = "client")]
impl Decodable for EndpointToPartitions {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        let user_endpoint = types::Struct { version }.decode(buf)?;
        let active_partitions = types::CompactArray(types::Struct { version }).decode(buf)?;
        let standby_partitions = types::CompactArray(types::Struct { version }).decode(buf)?;
        let mut unknown_tagged_fields = BTreeMap::new();
        let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
        for _ in 0..num_tagged_fields {
            let tag: u32 = types::UnsignedVarInt.decode(buf)?;
            let size: u32 = types::UnsignedVarInt.decode(buf)?;
            let unknown_value = buf.try_get_bytes(size as usize)?;
            unknown_tagged_fields.insert(tag as i32, unknown_value);
        }
        Ok(Self {
            user_endpoint,
            active_partitions,
            standby_partitions,
            unknown_tagged_fields,
        })
    }
}

impl Default for EndpointToPartitions {
    fn default() -> Self {
        Self {
            user_endpoint: Default::default(),
            active_partitions: Default::default(),
            standby_partitions: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for EndpointToPartitions {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 0 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 0
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct Status {
    /// A code to indicate that a particular status is active for the group membership
    ///
    /// Supported API versions: 0
    pub status_code: i8,

    /// A string representation of the status.
    ///
    /// Supported API versions: 0
    pub status_detail: StrBytes,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl Status {
    /// Sets `status_code` to the passed value.
    ///
    /// A code to indicate that a particular status is active for the group membership
    ///
    /// Supported API versions: 0
    pub fn with_status_code(mut self, value: i8) -> Self {
        self.status_code = value;
        self
    }
    /// Sets `status_detail` to the passed value.
    ///
    /// A string representation of the status.
    ///
    /// Supported API versions: 0
    pub fn with_status_detail(mut self, value: StrBytes) -> Self {
        self.status_detail = value;
        self
    }
    /// Sets unknown_tagged_fields to the passed value.
    pub fn with_unknown_tagged_fields(mut self, value: BTreeMap<i32, Bytes>) -> Self {
        self.unknown_tagged_fields = value;
        self
    }
    /// Inserts an entry into unknown_tagged_fields.
    pub fn with_unknown_tagged_field(mut self, key: i32, value: Bytes) -> Self {
        self.unknown_tagged_fields.insert(key, value);
        self
    }
}

#[cfg(feature = "broker")]
impl Encodable for Status {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        types::Int8.encode(buf, &self.status_code)?;
        types::CompactString.encode(buf, &self.status_detail)?;
        let num_tagged_fields = self.unknown_tagged_fields.len();
        if num_tagged_fields > std::u32::MAX as usize {
            bail!(
                "Too many tagged fields to encode ({} fields)",
                num_tagged_fields
            );
        }
        types::UnsignedVarInt.encode(buf, num_tagged_fields as u32)?;

        write_unknown_tagged_fields(buf, 0.., &self.unknown_tagged_fields)?;
        Ok(())
    }
    fn compute_size(&self, version: i16) -> Result<usize> {
        let mut total_size = 0;
        total_size += types::Int8.compute_size(&self.status_code)?;
        total_size += types::CompactString.compute_size(&self.status_detail)?;
        let num_tagged_fields = self.unknown_tagged_fields.len();
        if num_tagged_fields > std::u32::MAX as usize {
            bail!(
                "Too many tagged fields to encode ({} fields)",
                num_tagged_fields
            );
        }
        total_size += types::UnsignedVarInt.compute_size(num_tagged_fields as u32)?;

        total_size += compute_unknown_tagged_fields_size(&self.unknown_tagged_fields)?;
        Ok(total_size)
    }
}

#[cfg(feature = "client")]
impl Decodable for Status {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        let status_code = types::Int8.decode(buf)?;
        let status_detail = types::CompactString.decode(buf)?;
        let mut unknown_tagged_fields = BTreeMap::new();
        let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
        for _ in 0..num_tagged_fields {
            let tag: u32 = types::UnsignedVarInt.decode(buf)?;
            let size: u32 = types::UnsignedVarInt.decode(buf)?;
            let unknown_value = buf.try_get_bytes(size as usize)?;
            unknown_tagged_fields.insert(tag as i32, unknown_value);
        }
        Ok(Self {
            status_code,
            status_detail,
            unknown_tagged_fields,
        })
    }
}

impl Default for Status {
    fn default() -> Self {
        Self {
            status_code: 0,
            status_detail: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for Status {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 0 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 0
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct StreamsGroupHeartbeatResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    ///
    /// Supported API versions: 0
    pub throttle_time_ms: i32,

    /// The top-level error code, or 0 if there was no error
    ///
    /// Supported API versions: 0
    pub error_code: i16,

    /// The top-level error message, or null if there was no error.
    ///
    /// Supported API versions: 0
    pub error_message: Option<StrBytes>,

    /// The member id is always generated by the streams consumer.
    ///
    /// Supported API versions: 0
    pub member_id: StrBytes,

    /// The member epoch.
    ///
    /// Supported API versions: 0
    pub member_epoch: i32,

    /// The heartbeat interval in milliseconds.
    ///
    /// Supported API versions: 0
    pub heartbeat_interval_ms: i32,

    /// The maximal lag a warm-up task can have to be considered caught-up.
    ///
    /// Supported API versions: 0
    pub acceptable_recovery_lag: i32,

    /// The interval in which the task changelog offsets on a client are updated on the broker. The offsets are sent with the next heartbeat after this time has passed.
    ///
    /// Supported API versions: 0
    pub task_offset_interval_ms: i32,

    /// Indicate zero or more status for the group.
    ///
    /// Supported API versions: 0
    pub status: Option<Vec<Status>>,

    /// Assigned active tasks for this client. Null if unchanged since last heartbeat.
    ///
    /// Supported API versions: 0
    pub active_tasks: Option<Vec<TaskIds>>,

    /// Assigned standby tasks for this client. Null if unchanged since last heartbeat.
    ///
    /// Supported API versions: 0
    pub standby_tasks: Option<Vec<TaskIds>>,

    /// Assigned warm-up tasks for this client. Null if unchanged since last heartbeat.
    ///
    /// Supported API versions: 0
    pub warmup_tasks: Option<Vec<TaskIds>>,

    /// The endpoint epoch set in the response
    ///
    /// Supported API versions: 0
    pub endpoint_information_epoch: i32,

    /// Global assignment information used for IQ. Null if unchanged since last heartbeat.
    ///
    /// Supported API versions: 0
    pub partitions_by_user_endpoint: Option<Vec<EndpointToPartitions>>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl StreamsGroupHeartbeatResponse {
    /// Sets `throttle_time_ms` to the passed value.
    ///
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    ///
    /// Supported API versions: 0
    pub fn with_throttle_time_ms(mut self, value: i32) -> Self {
        self.throttle_time_ms = value;
        self
    }
    /// Sets `error_code` to the passed value.
    ///
    /// The top-level error code, or 0 if there was no error
    ///
    /// Supported API versions: 0
    pub fn with_error_code(mut self, value: i16) -> Self {
        self.error_code = value;
        self
    }
    /// Sets `error_message` to the passed value.
    ///
    /// The top-level error message, or null if there was no error.
    ///
    /// Supported API versions: 0
    pub fn with_error_message(mut self, value: Option<StrBytes>) -> Self {
        self.error_message = value;
        self
    }
    /// Sets `member_id` to the passed value.
    ///
    /// The member id is always generated by the streams consumer.
    ///
    /// Supported API versions: 0
    pub fn with_member_id(mut self, value: StrBytes) -> Self {
        self.member_id = value;
        self
    }
    /// Sets `member_epoch` to the passed value.
    ///
    /// The member epoch.
    ///
    /// Supported API versions: 0
    pub fn with_member_epoch(mut self, value: i32) -> Self {
        self.member_epoch = value;
        self
    }
    /// Sets `heartbeat_interval_ms` to the passed value.
    ///
    /// The heartbeat interval in milliseconds.
    ///
    /// Supported API versions: 0
    pub fn with_heartbeat_interval_ms(mut self, value: i32) -> Self {
        self.heartbeat_interval_ms = value;
        self
    }
    /// Sets `acceptable_recovery_lag` to the passed value.
    ///
    /// The maximal lag a warm-up task can have to be considered caught-up.
    ///
    /// Supported API versions: 0
    pub fn with_acceptable_recovery_lag(mut self, value: i32) -> Self {
        self.acceptable_recovery_lag = value;
        self
    }
    /// Sets `task_offset_interval_ms` to the passed value.
    ///
    /// The interval in which the task changelog offsets on a client are updated on the broker. The offsets are sent with the next heartbeat after this time has passed.
    ///
    /// Supported API versions: 0
    pub fn with_task_offset_interval_ms(mut self, value: i32) -> Self {
        self.task_offset_interval_ms = value;
        self
    }
    /// Sets `status` to the passed value.
    ///
    /// Indicate zero or more status for the group.
    ///
    /// Supported API versions: 0
    pub fn with_status(mut self, value: Option<Vec<Status>>) -> Self {
        self.status = value;
        self
    }
    /// Sets `active_tasks` to the passed value.
    ///
    /// Assigned active tasks for this client. Null if unchanged since last heartbeat.
    ///
    /// Supported API versions: 0
    pub fn with_active_tasks(mut self, value: Option<Vec<TaskIds>>) -> Self {
        self.active_tasks = value;
        self
    }
    /// Sets `standby_tasks` to the passed value.
    ///
    /// Assigned standby tasks for this client. Null if unchanged since last heartbeat.
    ///
    /// Supported API versions: 0
    pub fn with_standby_tasks(mut self, value: Option<Vec<TaskIds>>) -> Self {
        self.standby_tasks = value;
        self
    }
    /// Sets `warmup_tasks` to the passed value.
    ///
    /// Assigned warm-up tasks for this client. Null if unchanged since last heartbeat.
    ///
    /// Supported API versions: 0
    pub fn with_warmup_tasks(mut self, value: Option<Vec<TaskIds>>) -> Self {
        self.warmup_tasks = value;
        self
    }
    /// Sets `endpoint_information_epoch` to the passed value.
    ///
    /// The endpoint epoch set in the response
    ///
    /// Supported API versions: 0
    pub fn with_endpoint_information_epoch(mut self, value: i32) -> Self {
        self.endpoint_information_epoch = value;
        self
    }
    /// Sets `partitions_by_user_endpoint` to the passed value.
    ///
    /// Global assignment information used for IQ. Null if unchanged since last heartbeat.
    ///
    /// Supported API versions: 0
    pub fn with_partitions_by_user_endpoint(
        mut self,
        value: Option<Vec<EndpointToPartitions>>,
    ) -> Self {
        self.partitions_by_user_endpoint = value;
        self
    }
    /// Sets unknown_tagged_fields to the passed value.
    pub fn with_unknown_tagged_fields(mut self, value: BTreeMap<i32, Bytes>) -> Self {
        self.unknown_tagged_fields = value;
        self
    }
    /// Inserts an entry into unknown_tagged_fields.
    pub fn with_unknown_tagged_field(mut self, key: i32, value: Bytes) -> Self {
        self.unknown_tagged_fields.insert(key, value);
        self
    }
}

#[cfg(feature = "broker")]
impl Encodable for StreamsGroupHeartbeatResponse {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        types::Int32.encode(buf, &self.throttle_time_ms)?;
        types::Int16.encode(buf, &self.error_code)?;
        types::CompactString.encode(buf, &self.error_message)?;
        types::CompactString.encode(buf, &self.member_id)?;
        types::Int32.encode(buf, &self.member_epoch)?;
        types::Int32.encode(buf, &self.heartbeat_interval_ms)?;
        types::Int32.encode(buf, &self.acceptable_recovery_lag)?;
        types::Int32.encode(buf, &self.task_offset_interval_ms)?;
        types::CompactArray(types::Struct { version }).encode(buf, &self.status)?;
        types::CompactArray(types::Struct { version }).encode(buf, &self.active_tasks)?;
        types::CompactArray(types::Struct { version }).encode(buf, &self.standby_tasks)?;
        types::CompactArray(types::Struct { version }).encode(buf, &self.warmup_tasks)?;
        types::Int32.encode(buf, &self.endpoint_information_epoch)?;
        types::CompactArray(types::Struct { version })
            .encode(buf, &self.partitions_by_user_endpoint)?;
        let num_tagged_fields = self.unknown_tagged_fields.len();
        if num_tagged_fields > std::u32::MAX as usize {
            bail!(
                "Too many tagged fields to encode ({} fields)",
                num_tagged_fields
            );
        }
        types::UnsignedVarInt.encode(buf, num_tagged_fields as u32)?;

        write_unknown_tagged_fields(buf, 0.., &self.unknown_tagged_fields)?;
        Ok(())
    }
    fn compute_size(&self, version: i16) -> Result<usize> {
        let mut total_size = 0;
        total_size += types::Int32.compute_size(&self.throttle_time_ms)?;
        total_size += types::Int16.compute_size(&self.error_code)?;
        total_size += types::CompactString.compute_size(&self.error_message)?;
        total_size += types::CompactString.compute_size(&self.member_id)?;
        total_size += types::Int32.compute_size(&self.member_epoch)?;
        total_size += types::Int32.compute_size(&self.heartbeat_interval_ms)?;
        total_size += types::Int32.compute_size(&self.acceptable_recovery_lag)?;
        total_size += types::Int32.compute_size(&self.task_offset_interval_ms)?;
        total_size += types::CompactArray(types::Struct { version }).compute_size(&self.status)?;
        total_size +=
            types::CompactArray(types::Struct { version }).compute_size(&self.active_tasks)?;
        total_size +=
            types::CompactArray(types::Struct { version }).compute_size(&self.standby_tasks)?;
        total_size +=
            types::CompactArray(types::Struct { version }).compute_size(&self.warmup_tasks)?;
        total_size += types::Int32.compute_size(&self.endpoint_information_epoch)?;
        total_size += types::CompactArray(types::Struct { version })
            .compute_size(&self.partitions_by_user_endpoint)?;
        let num_tagged_fields = self.unknown_tagged_fields.len();
        if num_tagged_fields > std::u32::MAX as usize {
            bail!(
                "Too many tagged fields to encode ({} fields)",
                num_tagged_fields
            );
        }
        total_size += types::UnsignedVarInt.compute_size(num_tagged_fields as u32)?;

        total_size += compute_unknown_tagged_fields_size(&self.unknown_tagged_fields)?;
        Ok(total_size)
    }
}

#[cfg(feature = "client")]
impl Decodable for StreamsGroupHeartbeatResponse {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        let throttle_time_ms = types::Int32.decode(buf)?;
        let error_code = types::Int16.decode(buf)?;
        let error_message = types::CompactString.decode(buf)?;
        let member_id = types::CompactString.decode(buf)?;
        let member_epoch = types::Int32.decode(buf)?;
        let heartbeat_interval_ms = types::Int32.decode(buf)?;
        let acceptable_recovery_lag = types::Int32.decode(buf)?;
        let task_offset_interval_ms = types::Int32.decode(buf)?;
        let status = types::CompactArray(types::Struct { version }).decode(buf)?;
        let active_tasks = types::CompactArray(types::Struct { version }).decode(buf)?;
        let standby_tasks = types::CompactArray(types::Struct { version }).decode(buf)?;
        let warmup_tasks = types::CompactArray(types::Struct { version }).decode(buf)?;
        let endpoint_information_epoch = types::Int32.decode(buf)?;
        let partitions_by_user_endpoint =
            types::CompactArray(types::Struct { version }).decode(buf)?;
        let mut unknown_tagged_fields = BTreeMap::new();
        let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
        for _ in 0..num_tagged_fields {
            let tag: u32 = types::UnsignedVarInt.decode(buf)?;
            let size: u32 = types::UnsignedVarInt.decode(buf)?;
            let unknown_value = buf.try_get_bytes(size as usize)?;
            unknown_tagged_fields.insert(tag as i32, unknown_value);
        }
        Ok(Self {
            throttle_time_ms,
            error_code,
            error_message,
            member_id,
            member_epoch,
            heartbeat_interval_ms,
            acceptable_recovery_lag,
            task_offset_interval_ms,
            status,
            active_tasks,
            standby_tasks,
            warmup_tasks,
            endpoint_information_epoch,
            partitions_by_user_endpoint,
            unknown_tagged_fields,
        })
    }
}

impl Default for StreamsGroupHeartbeatResponse {
    fn default() -> Self {
        Self {
            throttle_time_ms: 0,
            error_code: 0,
            error_message: None,
            member_id: Default::default(),
            member_epoch: 0,
            heartbeat_interval_ms: 0,
            acceptable_recovery_lag: 0,
            task_offset_interval_ms: 0,
            status: Some(Default::default()),
            active_tasks: None,
            standby_tasks: None,
            warmup_tasks: None,
            endpoint_information_epoch: 0,
            partitions_by_user_endpoint: None,
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for StreamsGroupHeartbeatResponse {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 0 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 0
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct TaskIds {
    /// The subtopology identifier.
    ///
    /// Supported API versions: 0
    pub subtopology_id: StrBytes,

    /// The partitions of the input topics processed by this member.
    ///
    /// Supported API versions: 0
    pub partitions: Vec<i32>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl TaskIds {
    /// Sets `subtopology_id` to the passed value.
    ///
    /// The subtopology identifier.
    ///
    /// Supported API versions: 0
    pub fn with_subtopology_id(mut self, value: StrBytes) -> Self {
        self.subtopology_id = value;
        self
    }
    /// Sets `partitions` to the passed value.
    ///
    /// The partitions of the input topics processed by this member.
    ///
    /// Supported API versions: 0
    pub fn with_partitions(mut self, value: Vec<i32>) -> Self {
        self.partitions = value;
        self
    }
    /// Sets unknown_tagged_fields to the passed value.
    pub fn with_unknown_tagged_fields(mut self, value: BTreeMap<i32, Bytes>) -> Self {
        self.unknown_tagged_fields = value;
        self
    }
    /// Inserts an entry into unknown_tagged_fields.
    pub fn with_unknown_tagged_field(mut self, key: i32, value: Bytes) -> Self {
        self.unknown_tagged_fields.insert(key, value);
        self
    }
}

#[cfg(feature = "broker")]
impl Encodable for TaskIds {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        types::CompactString.encode(buf, &self.subtopology_id)?;
        types::CompactArray(types::Int32).encode(buf, &self.partitions)?;
        let num_tagged_fields = self.unknown_tagged_fields.len();
        if num_tagged_fields > std::u32::MAX as usize {
            bail!(
                "Too many tagged fields to encode ({} fields)",
                num_tagged_fields
            );
        }
        types::UnsignedVarInt.encode(buf, num_tagged_fields as u32)?;

        write_unknown_tagged_fields(buf, 0.., &self.unknown_tagged_fields)?;
        Ok(())
    }
    fn compute_size(&self, version: i16) -> Result<usize> {
        let mut total_size = 0;
        total_size += types::CompactString.compute_size(&self.subtopology_id)?;
        total_size += types::CompactArray(types::Int32).compute_size(&self.partitions)?;
        let num_tagged_fields = self.unknown_tagged_fields.len();
        if num_tagged_fields > std::u32::MAX as usize {
            bail!(
                "Too many tagged fields to encode ({} fields)",
                num_tagged_fields
            );
        }
        total_size += types::UnsignedVarInt.compute_size(num_tagged_fields as u32)?;

        total_size += compute_unknown_tagged_fields_size(&self.unknown_tagged_fields)?;
        Ok(total_size)
    }
}

#[cfg(feature = "client")]
impl Decodable for TaskIds {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        let subtopology_id = types::CompactString.decode(buf)?;
        let partitions = types::CompactArray(types::Int32).decode(buf)?;
        let mut unknown_tagged_fields = BTreeMap::new();
        let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
        for _ in 0..num_tagged_fields {
            let tag: u32 = types::UnsignedVarInt.decode(buf)?;
            let size: u32 = types::UnsignedVarInt.decode(buf)?;
            let unknown_value = buf.try_get_bytes(size as usize)?;
            unknown_tagged_fields.insert(tag as i32, unknown_value);
        }
        Ok(Self {
            subtopology_id,
            partitions,
            unknown_tagged_fields,
        })
    }
}

impl Default for TaskIds {
    fn default() -> Self {
        Self {
            subtopology_id: Default::default(),
            partitions: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for TaskIds {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 0 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 0
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct TopicPartition {
    /// topic name
    ///
    /// Supported API versions: 0
    pub topic: super::TopicName,

    /// partitions
    ///
    /// Supported API versions: 0
    pub partitions: Vec<i32>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl TopicPartition {
    /// Sets `topic` to the passed value.
    ///
    /// topic name
    ///
    /// Supported API versions: 0
    pub fn with_topic(mut self, value: super::TopicName) -> Self {
        self.topic = value;
        self
    }
    /// Sets `partitions` to the passed value.
    ///
    /// partitions
    ///
    /// Supported API versions: 0
    pub fn with_partitions(mut self, value: Vec<i32>) -> Self {
        self.partitions = value;
        self
    }
    /// Sets unknown_tagged_fields to the passed value.
    pub fn with_unknown_tagged_fields(mut self, value: BTreeMap<i32, Bytes>) -> Self {
        self.unknown_tagged_fields = value;
        self
    }
    /// Inserts an entry into unknown_tagged_fields.
    pub fn with_unknown_tagged_field(mut self, key: i32, value: Bytes) -> Self {
        self.unknown_tagged_fields.insert(key, value);
        self
    }
}

#[cfg(feature = "broker")]
impl Encodable for TopicPartition {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        types::CompactString.encode(buf, &self.topic)?;
        types::CompactArray(types::Int32).encode(buf, &self.partitions)?;
        let num_tagged_fields = self.unknown_tagged_fields.len();
        if num_tagged_fields > std::u32::MAX as usize {
            bail!(
                "Too many tagged fields to encode ({} fields)",
                num_tagged_fields
            );
        }
        types::UnsignedVarInt.encode(buf, num_tagged_fields as u32)?;

        write_unknown_tagged_fields(buf, 0.., &self.unknown_tagged_fields)?;
        Ok(())
    }
    fn compute_size(&self, version: i16) -> Result<usize> {
        let mut total_size = 0;
        total_size += types::CompactString.compute_size(&self.topic)?;
        total_size += types::CompactArray(types::Int32).compute_size(&self.partitions)?;
        let num_tagged_fields = self.unknown_tagged_fields.len();
        if num_tagged_fields > std::u32::MAX as usize {
            bail!(
                "Too many tagged fields to encode ({} fields)",
                num_tagged_fields
            );
        }
        total_size += types::UnsignedVarInt.compute_size(num_tagged_fields as u32)?;

        total_size += compute_unknown_tagged_fields_size(&self.unknown_tagged_fields)?;
        Ok(total_size)
    }
}

#[cfg(feature = "client")]
impl Decodable for TopicPartition {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        let topic = types::CompactString.decode(buf)?;
        let partitions = types::CompactArray(types::Int32).decode(buf)?;
        let mut unknown_tagged_fields = BTreeMap::new();
        let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
        for _ in 0..num_tagged_fields {
            let tag: u32 = types::UnsignedVarInt.decode(buf)?;
            let size: u32 = types::UnsignedVarInt.decode(buf)?;
            let unknown_value = buf.try_get_bytes(size as usize)?;
            unknown_tagged_fields.insert(tag as i32, unknown_value);
        }
        Ok(Self {
            topic,
            partitions,
            unknown_tagged_fields,
        })
    }
}

impl Default for TopicPartition {
    fn default() -> Self {
        Self {
            topic: Default::default(),
            partitions: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for TopicPartition {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 0 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

impl HeaderVersion for StreamsGroupHeartbeatResponse {
    fn header_version(version: i16) -> i16 {
        1
    }
}
