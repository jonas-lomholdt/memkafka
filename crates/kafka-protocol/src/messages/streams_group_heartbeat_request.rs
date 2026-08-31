//! StreamsGroupHeartbeatRequest
//!
//! See the schema for this message [here](https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/StreamsGroupHeartbeatRequest.json).
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
pub struct CopartitionGroup {
    /// The topics the topology reads from. Index into the array on the subtopology level.
    ///
    /// Supported API versions: 0
    pub source_topics: Vec<i16>,

    /// Regular expressions identifying topics the subtopology reads from. Index into the array on the subtopology level.
    ///
    /// Supported API versions: 0
    pub source_topic_regex: Vec<i16>,

    /// The set of source topics that are internally created repartition topics. Index into the array on the subtopology level.
    ///
    /// Supported API versions: 0
    pub repartition_source_topics: Vec<i16>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl CopartitionGroup {
    /// Sets `source_topics` to the passed value.
    ///
    /// The topics the topology reads from. Index into the array on the subtopology level.
    ///
    /// Supported API versions: 0
    pub fn with_source_topics(mut self, value: Vec<i16>) -> Self {
        self.source_topics = value;
        self
    }
    /// Sets `source_topic_regex` to the passed value.
    ///
    /// Regular expressions identifying topics the subtopology reads from. Index into the array on the subtopology level.
    ///
    /// Supported API versions: 0
    pub fn with_source_topic_regex(mut self, value: Vec<i16>) -> Self {
        self.source_topic_regex = value;
        self
    }
    /// Sets `repartition_source_topics` to the passed value.
    ///
    /// The set of source topics that are internally created repartition topics. Index into the array on the subtopology level.
    ///
    /// Supported API versions: 0
    pub fn with_repartition_source_topics(mut self, value: Vec<i16>) -> Self {
        self.repartition_source_topics = value;
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

#[cfg(feature = "client")]
impl Encodable for CopartitionGroup {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        types::CompactArray(types::Int16).encode(buf, &self.source_topics)?;
        types::CompactArray(types::Int16).encode(buf, &self.source_topic_regex)?;
        types::CompactArray(types::Int16).encode(buf, &self.repartition_source_topics)?;
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
        total_size += types::CompactArray(types::Int16).compute_size(&self.source_topics)?;
        total_size += types::CompactArray(types::Int16).compute_size(&self.source_topic_regex)?;
        total_size +=
            types::CompactArray(types::Int16).compute_size(&self.repartition_source_topics)?;
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

#[cfg(feature = "broker")]
impl Decodable for CopartitionGroup {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        let source_topics = types::CompactArray(types::Int16).decode(buf)?;
        let source_topic_regex = types::CompactArray(types::Int16).decode(buf)?;
        let repartition_source_topics = types::CompactArray(types::Int16).decode(buf)?;
        let mut unknown_tagged_fields = BTreeMap::new();
        let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
        for _ in 0..num_tagged_fields {
            let tag: u32 = types::UnsignedVarInt.decode(buf)?;
            let size: u32 = types::UnsignedVarInt.decode(buf)?;
            let unknown_value = buf.try_get_bytes(size as usize)?;
            unknown_tagged_fields.insert(tag as i32, unknown_value);
        }
        Ok(Self {
            source_topics,
            source_topic_regex,
            repartition_source_topics,
            unknown_tagged_fields,
        })
    }
}

impl Default for CopartitionGroup {
    fn default() -> Self {
        Self {
            source_topics: Default::default(),
            source_topic_regex: Default::default(),
            repartition_source_topics: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for CopartitionGroup {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 0 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

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

#[cfg(feature = "client")]
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

#[cfg(feature = "broker")]
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
pub struct KeyValue {
    /// key of the config
    ///
    /// Supported API versions: 0
    pub key: StrBytes,

    /// value of the config
    ///
    /// Supported API versions: 0
    pub value: StrBytes,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl KeyValue {
    /// Sets `key` to the passed value.
    ///
    /// key of the config
    ///
    /// Supported API versions: 0
    pub fn with_key(mut self, value: StrBytes) -> Self {
        self.key = value;
        self
    }
    /// Sets `value` to the passed value.
    ///
    /// value of the config
    ///
    /// Supported API versions: 0
    pub fn with_value(mut self, value: StrBytes) -> Self {
        self.value = value;
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

#[cfg(feature = "client")]
impl Encodable for KeyValue {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        types::CompactString.encode(buf, &self.key)?;
        types::CompactString.encode(buf, &self.value)?;
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
        total_size += types::CompactString.compute_size(&self.key)?;
        total_size += types::CompactString.compute_size(&self.value)?;
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

#[cfg(feature = "broker")]
impl Decodable for KeyValue {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        let key = types::CompactString.decode(buf)?;
        let value = types::CompactString.decode(buf)?;
        let mut unknown_tagged_fields = BTreeMap::new();
        let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
        for _ in 0..num_tagged_fields {
            let tag: u32 = types::UnsignedVarInt.decode(buf)?;
            let size: u32 = types::UnsignedVarInt.decode(buf)?;
            let unknown_value = buf.try_get_bytes(size as usize)?;
            unknown_tagged_fields.insert(tag as i32, unknown_value);
        }
        Ok(Self {
            key,
            value,
            unknown_tagged_fields,
        })
    }
}

impl Default for KeyValue {
    fn default() -> Self {
        Self {
            key: Default::default(),
            value: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for KeyValue {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 0 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 0
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct StreamsGroupHeartbeatRequest {
    /// The group identifier.
    ///
    /// Supported API versions: 0
    pub group_id: super::GroupId,

    /// The member ID generated by the streams consumer. The member ID must be kept during the entire lifetime of the streams consumer process.
    ///
    /// Supported API versions: 0
    pub member_id: StrBytes,

    /// The current member epoch; 0 to join the group; -1 to leave the group; -2 to indicate that the static member will rejoin.
    ///
    /// Supported API versions: 0
    pub member_epoch: i32,

    /// The current endpoint epoch of this client, represents the latest endpoint epoch this client received
    ///
    /// Supported API versions: 0
    pub endpoint_information_epoch: i32,

    /// null if not provided or if it didn't change since the last heartbeat; the instance ID for static membership otherwise.
    ///
    /// Supported API versions: 0
    pub instance_id: Option<StrBytes>,

    /// null if not provided or if it didn't change since the last heartbeat; the rack ID of the member otherwise.
    ///
    /// Supported API versions: 0
    pub rack_id: Option<StrBytes>,

    /// -1 if it didn't change since the last heartbeat; the maximum time in milliseconds that the coordinator will wait on the member to revoke its tasks otherwise.
    ///
    /// Supported API versions: 0
    pub rebalance_timeout_ms: i32,

    /// The topology metadata of the streams application. Used to initialize the topology of the group and to check if the topology corresponds to the topology initialized for the group. Only sent when memberEpoch = 0, must be non-empty. Null otherwise.
    ///
    /// Supported API versions: 0
    pub topology: Option<Topology>,

    /// Currently owned active tasks for this client. Null if unchanged since last heartbeat.
    ///
    /// Supported API versions: 0
    pub active_tasks: Option<Vec<TaskIds>>,

    /// Currently owned standby tasks for this client. Null if unchanged since last heartbeat.
    ///
    /// Supported API versions: 0
    pub standby_tasks: Option<Vec<TaskIds>>,

    /// Currently owned warm-up tasks for this client. Null if unchanged since last heartbeat.
    ///
    /// Supported API versions: 0
    pub warmup_tasks: Option<Vec<TaskIds>>,

    /// Identity of the streams instance that may have multiple consumers. Null if unchanged since last heartbeat.
    ///
    /// Supported API versions: 0
    pub process_id: Option<StrBytes>,

    /// User-defined endpoint for Interactive Queries. Null if unchanged since last heartbeat, or if not defined on the client.
    ///
    /// Supported API versions: 0
    pub user_endpoint: Option<Endpoint>,

    /// Used for rack-aware assignment algorithm. Null if unchanged since last heartbeat.
    ///
    /// Supported API versions: 0
    pub client_tags: Option<Vec<KeyValue>>,

    /// Cumulative changelog offsets for tasks. Only updated when a warm-up task has caught up, and according to the task offset interval. Null if unchanged since last heartbeat.
    ///
    /// Supported API versions: 0
    pub task_offsets: Option<Vec<TaskOffset>>,

    /// Cumulative changelog end-offsets for tasks. Only updated when a warm-up task has caught up, and according to the task offset interval. Null if unchanged since last heartbeat.
    ///
    /// Supported API versions: 0
    pub task_end_offsets: Option<Vec<TaskOffset>>,

    /// Whether all Streams clients in the group should shut down.
    ///
    /// Supported API versions: 0
    pub shutdown_application: bool,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl StreamsGroupHeartbeatRequest {
    /// Sets `group_id` to the passed value.
    ///
    /// The group identifier.
    ///
    /// Supported API versions: 0
    pub fn with_group_id(mut self, value: super::GroupId) -> Self {
        self.group_id = value;
        self
    }
    /// Sets `member_id` to the passed value.
    ///
    /// The member ID generated by the streams consumer. The member ID must be kept during the entire lifetime of the streams consumer process.
    ///
    /// Supported API versions: 0
    pub fn with_member_id(mut self, value: StrBytes) -> Self {
        self.member_id = value;
        self
    }
    /// Sets `member_epoch` to the passed value.
    ///
    /// The current member epoch; 0 to join the group; -1 to leave the group; -2 to indicate that the static member will rejoin.
    ///
    /// Supported API versions: 0
    pub fn with_member_epoch(mut self, value: i32) -> Self {
        self.member_epoch = value;
        self
    }
    /// Sets `endpoint_information_epoch` to the passed value.
    ///
    /// The current endpoint epoch of this client, represents the latest endpoint epoch this client received
    ///
    /// Supported API versions: 0
    pub fn with_endpoint_information_epoch(mut self, value: i32) -> Self {
        self.endpoint_information_epoch = value;
        self
    }
    /// Sets `instance_id` to the passed value.
    ///
    /// null if not provided or if it didn't change since the last heartbeat; the instance ID for static membership otherwise.
    ///
    /// Supported API versions: 0
    pub fn with_instance_id(mut self, value: Option<StrBytes>) -> Self {
        self.instance_id = value;
        self
    }
    /// Sets `rack_id` to the passed value.
    ///
    /// null if not provided or if it didn't change since the last heartbeat; the rack ID of the member otherwise.
    ///
    /// Supported API versions: 0
    pub fn with_rack_id(mut self, value: Option<StrBytes>) -> Self {
        self.rack_id = value;
        self
    }
    /// Sets `rebalance_timeout_ms` to the passed value.
    ///
    /// -1 if it didn't change since the last heartbeat; the maximum time in milliseconds that the coordinator will wait on the member to revoke its tasks otherwise.
    ///
    /// Supported API versions: 0
    pub fn with_rebalance_timeout_ms(mut self, value: i32) -> Self {
        self.rebalance_timeout_ms = value;
        self
    }
    /// Sets `topology` to the passed value.
    ///
    /// The topology metadata of the streams application. Used to initialize the topology of the group and to check if the topology corresponds to the topology initialized for the group. Only sent when memberEpoch = 0, must be non-empty. Null otherwise.
    ///
    /// Supported API versions: 0
    pub fn with_topology(mut self, value: Option<Topology>) -> Self {
        self.topology = value;
        self
    }
    /// Sets `active_tasks` to the passed value.
    ///
    /// Currently owned active tasks for this client. Null if unchanged since last heartbeat.
    ///
    /// Supported API versions: 0
    pub fn with_active_tasks(mut self, value: Option<Vec<TaskIds>>) -> Self {
        self.active_tasks = value;
        self
    }
    /// Sets `standby_tasks` to the passed value.
    ///
    /// Currently owned standby tasks for this client. Null if unchanged since last heartbeat.
    ///
    /// Supported API versions: 0
    pub fn with_standby_tasks(mut self, value: Option<Vec<TaskIds>>) -> Self {
        self.standby_tasks = value;
        self
    }
    /// Sets `warmup_tasks` to the passed value.
    ///
    /// Currently owned warm-up tasks for this client. Null if unchanged since last heartbeat.
    ///
    /// Supported API versions: 0
    pub fn with_warmup_tasks(mut self, value: Option<Vec<TaskIds>>) -> Self {
        self.warmup_tasks = value;
        self
    }
    /// Sets `process_id` to the passed value.
    ///
    /// Identity of the streams instance that may have multiple consumers. Null if unchanged since last heartbeat.
    ///
    /// Supported API versions: 0
    pub fn with_process_id(mut self, value: Option<StrBytes>) -> Self {
        self.process_id = value;
        self
    }
    /// Sets `user_endpoint` to the passed value.
    ///
    /// User-defined endpoint for Interactive Queries. Null if unchanged since last heartbeat, or if not defined on the client.
    ///
    /// Supported API versions: 0
    pub fn with_user_endpoint(mut self, value: Option<Endpoint>) -> Self {
        self.user_endpoint = value;
        self
    }
    /// Sets `client_tags` to the passed value.
    ///
    /// Used for rack-aware assignment algorithm. Null if unchanged since last heartbeat.
    ///
    /// Supported API versions: 0
    pub fn with_client_tags(mut self, value: Option<Vec<KeyValue>>) -> Self {
        self.client_tags = value;
        self
    }
    /// Sets `task_offsets` to the passed value.
    ///
    /// Cumulative changelog offsets for tasks. Only updated when a warm-up task has caught up, and according to the task offset interval. Null if unchanged since last heartbeat.
    ///
    /// Supported API versions: 0
    pub fn with_task_offsets(mut self, value: Option<Vec<TaskOffset>>) -> Self {
        self.task_offsets = value;
        self
    }
    /// Sets `task_end_offsets` to the passed value.
    ///
    /// Cumulative changelog end-offsets for tasks. Only updated when a warm-up task has caught up, and according to the task offset interval. Null if unchanged since last heartbeat.
    ///
    /// Supported API versions: 0
    pub fn with_task_end_offsets(mut self, value: Option<Vec<TaskOffset>>) -> Self {
        self.task_end_offsets = value;
        self
    }
    /// Sets `shutdown_application` to the passed value.
    ///
    /// Whether all Streams clients in the group should shut down.
    ///
    /// Supported API versions: 0
    pub fn with_shutdown_application(mut self, value: bool) -> Self {
        self.shutdown_application = value;
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

#[cfg(feature = "client")]
impl Encodable for StreamsGroupHeartbeatRequest {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        types::CompactString.encode(buf, &self.group_id)?;
        types::CompactString.encode(buf, &self.member_id)?;
        types::Int32.encode(buf, &self.member_epoch)?;
        types::Int32.encode(buf, &self.endpoint_information_epoch)?;
        types::CompactString.encode(buf, &self.instance_id)?;
        types::CompactString.encode(buf, &self.rack_id)?;
        types::Int32.encode(buf, &self.rebalance_timeout_ms)?;
        types::OptionStruct { version }.encode(buf, &self.topology)?;
        types::CompactArray(types::Struct { version }).encode(buf, &self.active_tasks)?;
        types::CompactArray(types::Struct { version }).encode(buf, &self.standby_tasks)?;
        types::CompactArray(types::Struct { version }).encode(buf, &self.warmup_tasks)?;
        types::CompactString.encode(buf, &self.process_id)?;
        types::OptionStruct { version }.encode(buf, &self.user_endpoint)?;
        types::CompactArray(types::Struct { version }).encode(buf, &self.client_tags)?;
        types::CompactArray(types::Struct { version }).encode(buf, &self.task_offsets)?;
        types::CompactArray(types::Struct { version }).encode(buf, &self.task_end_offsets)?;
        types::Boolean.encode(buf, &self.shutdown_application)?;
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
        total_size += types::CompactString.compute_size(&self.group_id)?;
        total_size += types::CompactString.compute_size(&self.member_id)?;
        total_size += types::Int32.compute_size(&self.member_epoch)?;
        total_size += types::Int32.compute_size(&self.endpoint_information_epoch)?;
        total_size += types::CompactString.compute_size(&self.instance_id)?;
        total_size += types::CompactString.compute_size(&self.rack_id)?;
        total_size += types::Int32.compute_size(&self.rebalance_timeout_ms)?;
        total_size += types::OptionStruct { version }.compute_size(&self.topology)?;
        total_size +=
            types::CompactArray(types::Struct { version }).compute_size(&self.active_tasks)?;
        total_size +=
            types::CompactArray(types::Struct { version }).compute_size(&self.standby_tasks)?;
        total_size +=
            types::CompactArray(types::Struct { version }).compute_size(&self.warmup_tasks)?;
        total_size += types::CompactString.compute_size(&self.process_id)?;
        total_size += types::OptionStruct { version }.compute_size(&self.user_endpoint)?;
        total_size +=
            types::CompactArray(types::Struct { version }).compute_size(&self.client_tags)?;
        total_size +=
            types::CompactArray(types::Struct { version }).compute_size(&self.task_offsets)?;
        total_size +=
            types::CompactArray(types::Struct { version }).compute_size(&self.task_end_offsets)?;
        total_size += types::Boolean.compute_size(&self.shutdown_application)?;
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

#[cfg(feature = "broker")]
impl Decodable for StreamsGroupHeartbeatRequest {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        let group_id = types::CompactString.decode(buf)?;
        let member_id = types::CompactString.decode(buf)?;
        let member_epoch = types::Int32.decode(buf)?;
        let endpoint_information_epoch = types::Int32.decode(buf)?;
        let instance_id = types::CompactString.decode(buf)?;
        let rack_id = types::CompactString.decode(buf)?;
        let rebalance_timeout_ms = types::Int32.decode(buf)?;
        let topology = types::OptionStruct { version }.decode(buf)?;
        let active_tasks = types::CompactArray(types::Struct { version }).decode(buf)?;
        let standby_tasks = types::CompactArray(types::Struct { version }).decode(buf)?;
        let warmup_tasks = types::CompactArray(types::Struct { version }).decode(buf)?;
        let process_id = types::CompactString.decode(buf)?;
        let user_endpoint = types::OptionStruct { version }.decode(buf)?;
        let client_tags = types::CompactArray(types::Struct { version }).decode(buf)?;
        let task_offsets = types::CompactArray(types::Struct { version }).decode(buf)?;
        let task_end_offsets = types::CompactArray(types::Struct { version }).decode(buf)?;
        let shutdown_application = types::Boolean.decode(buf)?;
        let mut unknown_tagged_fields = BTreeMap::new();
        let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
        for _ in 0..num_tagged_fields {
            let tag: u32 = types::UnsignedVarInt.decode(buf)?;
            let size: u32 = types::UnsignedVarInt.decode(buf)?;
            let unknown_value = buf.try_get_bytes(size as usize)?;
            unknown_tagged_fields.insert(tag as i32, unknown_value);
        }
        Ok(Self {
            group_id,
            member_id,
            member_epoch,
            endpoint_information_epoch,
            instance_id,
            rack_id,
            rebalance_timeout_ms,
            topology,
            active_tasks,
            standby_tasks,
            warmup_tasks,
            process_id,
            user_endpoint,
            client_tags,
            task_offsets,
            task_end_offsets,
            shutdown_application,
            unknown_tagged_fields,
        })
    }
}

impl Default for StreamsGroupHeartbeatRequest {
    fn default() -> Self {
        Self {
            group_id: Default::default(),
            member_id: Default::default(),
            member_epoch: 0,
            endpoint_information_epoch: 0,
            instance_id: None,
            rack_id: None,
            rebalance_timeout_ms: -1,
            topology: None,
            active_tasks: None,
            standby_tasks: None,
            warmup_tasks: None,
            process_id: None,
            user_endpoint: None,
            client_tags: None,
            task_offsets: None,
            task_end_offsets: None,
            shutdown_application: false,
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for StreamsGroupHeartbeatRequest {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 0 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 0
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct Subtopology {
    /// String to uniquely identify the subtopology. Deterministically generated from the topology
    ///
    /// Supported API versions: 0
    pub subtopology_id: StrBytes,

    /// The topics the topology reads from.
    ///
    /// Supported API versions: 0
    pub source_topics: Vec<super::TopicName>,

    /// The regular expressions identifying topics the subtopology reads from.
    ///
    /// Supported API versions: 0
    pub source_topic_regex: Vec<StrBytes>,

    /// The set of state changelog topics associated with this subtopology. Created automatically.
    ///
    /// Supported API versions: 0
    pub state_changelog_topics: Vec<TopicInfo>,

    /// The repartition topics the subtopology writes to.
    ///
    /// Supported API versions: 0
    pub repartition_sink_topics: Vec<super::TopicName>,

    /// The set of source topics that are internally created repartition topics. Created automatically.
    ///
    /// Supported API versions: 0
    pub repartition_source_topics: Vec<TopicInfo>,

    /// A subset of source topics that must be copartitioned.
    ///
    /// Supported API versions: 0
    pub copartition_groups: Vec<CopartitionGroup>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl Subtopology {
    /// Sets `subtopology_id` to the passed value.
    ///
    /// String to uniquely identify the subtopology. Deterministically generated from the topology
    ///
    /// Supported API versions: 0
    pub fn with_subtopology_id(mut self, value: StrBytes) -> Self {
        self.subtopology_id = value;
        self
    }
    /// Sets `source_topics` to the passed value.
    ///
    /// The topics the topology reads from.
    ///
    /// Supported API versions: 0
    pub fn with_source_topics(mut self, value: Vec<super::TopicName>) -> Self {
        self.source_topics = value;
        self
    }
    /// Sets `source_topic_regex` to the passed value.
    ///
    /// The regular expressions identifying topics the subtopology reads from.
    ///
    /// Supported API versions: 0
    pub fn with_source_topic_regex(mut self, value: Vec<StrBytes>) -> Self {
        self.source_topic_regex = value;
        self
    }
    /// Sets `state_changelog_topics` to the passed value.
    ///
    /// The set of state changelog topics associated with this subtopology. Created automatically.
    ///
    /// Supported API versions: 0
    pub fn with_state_changelog_topics(mut self, value: Vec<TopicInfo>) -> Self {
        self.state_changelog_topics = value;
        self
    }
    /// Sets `repartition_sink_topics` to the passed value.
    ///
    /// The repartition topics the subtopology writes to.
    ///
    /// Supported API versions: 0
    pub fn with_repartition_sink_topics(mut self, value: Vec<super::TopicName>) -> Self {
        self.repartition_sink_topics = value;
        self
    }
    /// Sets `repartition_source_topics` to the passed value.
    ///
    /// The set of source topics that are internally created repartition topics. Created automatically.
    ///
    /// Supported API versions: 0
    pub fn with_repartition_source_topics(mut self, value: Vec<TopicInfo>) -> Self {
        self.repartition_source_topics = value;
        self
    }
    /// Sets `copartition_groups` to the passed value.
    ///
    /// A subset of source topics that must be copartitioned.
    ///
    /// Supported API versions: 0
    pub fn with_copartition_groups(mut self, value: Vec<CopartitionGroup>) -> Self {
        self.copartition_groups = value;
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

#[cfg(feature = "client")]
impl Encodable for Subtopology {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        types::CompactString.encode(buf, &self.subtopology_id)?;
        types::CompactArray(types::CompactString).encode(buf, &self.source_topics)?;
        types::CompactArray(types::CompactString).encode(buf, &self.source_topic_regex)?;
        types::CompactArray(types::Struct { version }).encode(buf, &self.state_changelog_topics)?;
        types::CompactArray(types::CompactString).encode(buf, &self.repartition_sink_topics)?;
        types::CompactArray(types::Struct { version })
            .encode(buf, &self.repartition_source_topics)?;
        types::CompactArray(types::Struct { version }).encode(buf, &self.copartition_groups)?;
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
        total_size +=
            types::CompactArray(types::CompactString).compute_size(&self.source_topics)?;
        total_size +=
            types::CompactArray(types::CompactString).compute_size(&self.source_topic_regex)?;
        total_size += types::CompactArray(types::Struct { version })
            .compute_size(&self.state_changelog_topics)?;
        total_size += types::CompactArray(types::CompactString)
            .compute_size(&self.repartition_sink_topics)?;
        total_size += types::CompactArray(types::Struct { version })
            .compute_size(&self.repartition_source_topics)?;
        total_size += types::CompactArray(types::Struct { version })
            .compute_size(&self.copartition_groups)?;
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

#[cfg(feature = "broker")]
impl Decodable for Subtopology {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        let subtopology_id = types::CompactString.decode(buf)?;
        let source_topics = types::CompactArray(types::CompactString).decode(buf)?;
        let source_topic_regex = types::CompactArray(types::CompactString).decode(buf)?;
        let state_changelog_topics = types::CompactArray(types::Struct { version }).decode(buf)?;
        let repartition_sink_topics = types::CompactArray(types::CompactString).decode(buf)?;
        let repartition_source_topics =
            types::CompactArray(types::Struct { version }).decode(buf)?;
        let copartition_groups = types::CompactArray(types::Struct { version }).decode(buf)?;
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
            source_topics,
            source_topic_regex,
            state_changelog_topics,
            repartition_sink_topics,
            repartition_source_topics,
            copartition_groups,
            unknown_tagged_fields,
        })
    }
}

impl Default for Subtopology {
    fn default() -> Self {
        Self {
            subtopology_id: Default::default(),
            source_topics: Default::default(),
            source_topic_regex: Default::default(),
            state_changelog_topics: Default::default(),
            repartition_sink_topics: Default::default(),
            repartition_source_topics: Default::default(),
            copartition_groups: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for Subtopology {
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

#[cfg(feature = "client")]
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

#[cfg(feature = "broker")]
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
pub struct TaskOffset {
    /// The subtopology identifier.
    ///
    /// Supported API versions: 0
    pub subtopology_id: StrBytes,

    /// The partition.
    ///
    /// Supported API versions: 0
    pub partition: i32,

    /// The offset.
    ///
    /// Supported API versions: 0
    pub offset: i64,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl TaskOffset {
    /// Sets `subtopology_id` to the passed value.
    ///
    /// The subtopology identifier.
    ///
    /// Supported API versions: 0
    pub fn with_subtopology_id(mut self, value: StrBytes) -> Self {
        self.subtopology_id = value;
        self
    }
    /// Sets `partition` to the passed value.
    ///
    /// The partition.
    ///
    /// Supported API versions: 0
    pub fn with_partition(mut self, value: i32) -> Self {
        self.partition = value;
        self
    }
    /// Sets `offset` to the passed value.
    ///
    /// The offset.
    ///
    /// Supported API versions: 0
    pub fn with_offset(mut self, value: i64) -> Self {
        self.offset = value;
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

#[cfg(feature = "client")]
impl Encodable for TaskOffset {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        types::CompactString.encode(buf, &self.subtopology_id)?;
        types::Int32.encode(buf, &self.partition)?;
        types::Int64.encode(buf, &self.offset)?;
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
        total_size += types::Int32.compute_size(&self.partition)?;
        total_size += types::Int64.compute_size(&self.offset)?;
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

#[cfg(feature = "broker")]
impl Decodable for TaskOffset {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        let subtopology_id = types::CompactString.decode(buf)?;
        let partition = types::Int32.decode(buf)?;
        let offset = types::Int64.decode(buf)?;
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
            partition,
            offset,
            unknown_tagged_fields,
        })
    }
}

impl Default for TaskOffset {
    fn default() -> Self {
        Self {
            subtopology_id: Default::default(),
            partition: 0,
            offset: 0,
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for TaskOffset {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 0 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 0
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct TopicInfo {
    /// The name of the topic.
    ///
    /// Supported API versions: 0
    pub name: super::TopicName,

    /// The number of partitions in the topic. Can be 0 if no specific number of partitions is enforced. Always 0 for changelog topics.
    ///
    /// Supported API versions: 0
    pub partitions: i32,

    /// The replication factor of the topic. Can be 0 if the default replication factor should be used.
    ///
    /// Supported API versions: 0
    pub replication_factor: i16,

    /// Topic-level configurations as key-value pairs.
    ///
    /// Supported API versions: 0
    pub topic_configs: Vec<KeyValue>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl TopicInfo {
    /// Sets `name` to the passed value.
    ///
    /// The name of the topic.
    ///
    /// Supported API versions: 0
    pub fn with_name(mut self, value: super::TopicName) -> Self {
        self.name = value;
        self
    }
    /// Sets `partitions` to the passed value.
    ///
    /// The number of partitions in the topic. Can be 0 if no specific number of partitions is enforced. Always 0 for changelog topics.
    ///
    /// Supported API versions: 0
    pub fn with_partitions(mut self, value: i32) -> Self {
        self.partitions = value;
        self
    }
    /// Sets `replication_factor` to the passed value.
    ///
    /// The replication factor of the topic. Can be 0 if the default replication factor should be used.
    ///
    /// Supported API versions: 0
    pub fn with_replication_factor(mut self, value: i16) -> Self {
        self.replication_factor = value;
        self
    }
    /// Sets `topic_configs` to the passed value.
    ///
    /// Topic-level configurations as key-value pairs.
    ///
    /// Supported API versions: 0
    pub fn with_topic_configs(mut self, value: Vec<KeyValue>) -> Self {
        self.topic_configs = value;
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

#[cfg(feature = "client")]
impl Encodable for TopicInfo {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        types::CompactString.encode(buf, &self.name)?;
        types::Int32.encode(buf, &self.partitions)?;
        types::Int16.encode(buf, &self.replication_factor)?;
        types::CompactArray(types::Struct { version }).encode(buf, &self.topic_configs)?;
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
        total_size += types::CompactString.compute_size(&self.name)?;
        total_size += types::Int32.compute_size(&self.partitions)?;
        total_size += types::Int16.compute_size(&self.replication_factor)?;
        total_size +=
            types::CompactArray(types::Struct { version }).compute_size(&self.topic_configs)?;
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

#[cfg(feature = "broker")]
impl Decodable for TopicInfo {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        let name = types::CompactString.decode(buf)?;
        let partitions = types::Int32.decode(buf)?;
        let replication_factor = types::Int16.decode(buf)?;
        let topic_configs = types::CompactArray(types::Struct { version }).decode(buf)?;
        let mut unknown_tagged_fields = BTreeMap::new();
        let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
        for _ in 0..num_tagged_fields {
            let tag: u32 = types::UnsignedVarInt.decode(buf)?;
            let size: u32 = types::UnsignedVarInt.decode(buf)?;
            let unknown_value = buf.try_get_bytes(size as usize)?;
            unknown_tagged_fields.insert(tag as i32, unknown_value);
        }
        Ok(Self {
            name,
            partitions,
            replication_factor,
            topic_configs,
            unknown_tagged_fields,
        })
    }
}

impl Default for TopicInfo {
    fn default() -> Self {
        Self {
            name: Default::default(),
            partitions: 0,
            replication_factor: 0,
            topic_configs: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for TopicInfo {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 0 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 0
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct Topology {
    /// The epoch of the topology. Used to check if the topology corresponds to the topology initialized on the brokers.
    ///
    /// Supported API versions: 0
    pub epoch: i32,

    /// The sub-topologies of the streams application.
    ///
    /// Supported API versions: 0
    pub subtopologies: Vec<Subtopology>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl Topology {
    /// Sets `epoch` to the passed value.
    ///
    /// The epoch of the topology. Used to check if the topology corresponds to the topology initialized on the brokers.
    ///
    /// Supported API versions: 0
    pub fn with_epoch(mut self, value: i32) -> Self {
        self.epoch = value;
        self
    }
    /// Sets `subtopologies` to the passed value.
    ///
    /// The sub-topologies of the streams application.
    ///
    /// Supported API versions: 0
    pub fn with_subtopologies(mut self, value: Vec<Subtopology>) -> Self {
        self.subtopologies = value;
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

#[cfg(feature = "client")]
impl Encodable for Topology {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        types::Int32.encode(buf, &self.epoch)?;
        types::CompactArray(types::Struct { version }).encode(buf, &self.subtopologies)?;
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
        total_size += types::Int32.compute_size(&self.epoch)?;
        total_size +=
            types::CompactArray(types::Struct { version }).compute_size(&self.subtopologies)?;
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

#[cfg(feature = "broker")]
impl Decodable for Topology {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        let epoch = types::Int32.decode(buf)?;
        let subtopologies = types::CompactArray(types::Struct { version }).decode(buf)?;
        let mut unknown_tagged_fields = BTreeMap::new();
        let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
        for _ in 0..num_tagged_fields {
            let tag: u32 = types::UnsignedVarInt.decode(buf)?;
            let size: u32 = types::UnsignedVarInt.decode(buf)?;
            let unknown_value = buf.try_get_bytes(size as usize)?;
            unknown_tagged_fields.insert(tag as i32, unknown_value);
        }
        Ok(Self {
            epoch,
            subtopologies,
            unknown_tagged_fields,
        })
    }
}

impl Default for Topology {
    fn default() -> Self {
        Self {
            epoch: 0,
            subtopologies: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for Topology {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 0 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

impl HeaderVersion for StreamsGroupHeartbeatRequest {
    fn header_version(version: i16) -> i16 {
        2
    }
}
