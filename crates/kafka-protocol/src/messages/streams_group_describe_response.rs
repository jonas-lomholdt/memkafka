//! StreamsGroupDescribeResponse
//!
//! See the schema for this message [here](https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/StreamsGroupDescribeResponse.json).
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
pub struct Assignment {
    /// Active tasks for this client.
    ///
    /// Supported API versions: 0
    pub active_tasks: Vec<TaskIds>,

    /// Standby tasks for this client.
    ///
    /// Supported API versions: 0
    pub standby_tasks: Vec<TaskIds>,

    /// Warm-up tasks for this client.
    ///
    /// Supported API versions: 0
    pub warmup_tasks: Vec<TaskIds>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl Assignment {
    /// Sets `active_tasks` to the passed value.
    ///
    /// Active tasks for this client.
    ///
    /// Supported API versions: 0
    pub fn with_active_tasks(mut self, value: Vec<TaskIds>) -> Self {
        self.active_tasks = value;
        self
    }
    /// Sets `standby_tasks` to the passed value.
    ///
    /// Standby tasks for this client.
    ///
    /// Supported API versions: 0
    pub fn with_standby_tasks(mut self, value: Vec<TaskIds>) -> Self {
        self.standby_tasks = value;
        self
    }
    /// Sets `warmup_tasks` to the passed value.
    ///
    /// Warm-up tasks for this client.
    ///
    /// Supported API versions: 0
    pub fn with_warmup_tasks(mut self, value: Vec<TaskIds>) -> Self {
        self.warmup_tasks = value;
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
impl Encodable for Assignment {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        types::CompactArray(types::Struct { version }).encode(buf, &self.active_tasks)?;
        types::CompactArray(types::Struct { version }).encode(buf, &self.standby_tasks)?;
        types::CompactArray(types::Struct { version }).encode(buf, &self.warmup_tasks)?;
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
        total_size +=
            types::CompactArray(types::Struct { version }).compute_size(&self.active_tasks)?;
        total_size +=
            types::CompactArray(types::Struct { version }).compute_size(&self.standby_tasks)?;
        total_size +=
            types::CompactArray(types::Struct { version }).compute_size(&self.warmup_tasks)?;
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
impl Decodable for Assignment {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        let active_tasks = types::CompactArray(types::Struct { version }).decode(buf)?;
        let standby_tasks = types::CompactArray(types::Struct { version }).decode(buf)?;
        let warmup_tasks = types::CompactArray(types::Struct { version }).decode(buf)?;
        let mut unknown_tagged_fields = BTreeMap::new();
        let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
        for _ in 0..num_tagged_fields {
            let tag: u32 = types::UnsignedVarInt.decode(buf)?;
            let size: u32 = types::UnsignedVarInt.decode(buf)?;
            let unknown_value = buf.try_get_bytes(size as usize)?;
            unknown_tagged_fields.insert(tag as i32, unknown_value);
        }
        Ok(Self {
            active_tasks,
            standby_tasks,
            warmup_tasks,
            unknown_tagged_fields,
        })
    }
}

impl Default for Assignment {
    fn default() -> Self {
        Self {
            active_tasks: Default::default(),
            standby_tasks: Default::default(),
            warmup_tasks: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for Assignment {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 0 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 0
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct DescribedGroup {
    /// The describe error, or 0 if there was no error.
    ///
    /// Supported API versions: 0
    pub error_code: i16,

    /// The top-level error message, or null if there was no error.
    ///
    /// Supported API versions: 0
    pub error_message: Option<StrBytes>,

    /// The group ID string.
    ///
    /// Supported API versions: 0
    pub group_id: super::GroupId,

    /// The group state string, or the empty string.
    ///
    /// Supported API versions: 0
    pub group_state: StrBytes,

    /// The group epoch.
    ///
    /// Supported API versions: 0
    pub group_epoch: i32,

    /// The assignment epoch.
    ///
    /// Supported API versions: 0
    pub assignment_epoch: i32,

    /// The topology metadata currently initialized for the streams application. Can be null in case of a describe error.
    ///
    /// Supported API versions: 0
    pub topology: Option<Topology>,

    /// The members.
    ///
    /// Supported API versions: 0
    pub members: Vec<Member>,

    /// 32-bit bitfield to represent authorized operations for this group.
    ///
    /// Supported API versions: 0
    pub authorized_operations: i32,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl DescribedGroup {
    /// Sets `error_code` to the passed value.
    ///
    /// The describe error, or 0 if there was no error.
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
    /// Sets `group_id` to the passed value.
    ///
    /// The group ID string.
    ///
    /// Supported API versions: 0
    pub fn with_group_id(mut self, value: super::GroupId) -> Self {
        self.group_id = value;
        self
    }
    /// Sets `group_state` to the passed value.
    ///
    /// The group state string, or the empty string.
    ///
    /// Supported API versions: 0
    pub fn with_group_state(mut self, value: StrBytes) -> Self {
        self.group_state = value;
        self
    }
    /// Sets `group_epoch` to the passed value.
    ///
    /// The group epoch.
    ///
    /// Supported API versions: 0
    pub fn with_group_epoch(mut self, value: i32) -> Self {
        self.group_epoch = value;
        self
    }
    /// Sets `assignment_epoch` to the passed value.
    ///
    /// The assignment epoch.
    ///
    /// Supported API versions: 0
    pub fn with_assignment_epoch(mut self, value: i32) -> Self {
        self.assignment_epoch = value;
        self
    }
    /// Sets `topology` to the passed value.
    ///
    /// The topology metadata currently initialized for the streams application. Can be null in case of a describe error.
    ///
    /// Supported API versions: 0
    pub fn with_topology(mut self, value: Option<Topology>) -> Self {
        self.topology = value;
        self
    }
    /// Sets `members` to the passed value.
    ///
    /// The members.
    ///
    /// Supported API versions: 0
    pub fn with_members(mut self, value: Vec<Member>) -> Self {
        self.members = value;
        self
    }
    /// Sets `authorized_operations` to the passed value.
    ///
    /// 32-bit bitfield to represent authorized operations for this group.
    ///
    /// Supported API versions: 0
    pub fn with_authorized_operations(mut self, value: i32) -> Self {
        self.authorized_operations = value;
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
impl Encodable for DescribedGroup {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        types::Int16.encode(buf, &self.error_code)?;
        types::CompactString.encode(buf, &self.error_message)?;
        types::CompactString.encode(buf, &self.group_id)?;
        types::CompactString.encode(buf, &self.group_state)?;
        types::Int32.encode(buf, &self.group_epoch)?;
        types::Int32.encode(buf, &self.assignment_epoch)?;
        types::OptionStruct { version }.encode(buf, &self.topology)?;
        types::CompactArray(types::Struct { version }).encode(buf, &self.members)?;
        types::Int32.encode(buf, &self.authorized_operations)?;
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
        total_size += types::Int16.compute_size(&self.error_code)?;
        total_size += types::CompactString.compute_size(&self.error_message)?;
        total_size += types::CompactString.compute_size(&self.group_id)?;
        total_size += types::CompactString.compute_size(&self.group_state)?;
        total_size += types::Int32.compute_size(&self.group_epoch)?;
        total_size += types::Int32.compute_size(&self.assignment_epoch)?;
        total_size += types::OptionStruct { version }.compute_size(&self.topology)?;
        total_size += types::CompactArray(types::Struct { version }).compute_size(&self.members)?;
        total_size += types::Int32.compute_size(&self.authorized_operations)?;
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
impl Decodable for DescribedGroup {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        let error_code = types::Int16.decode(buf)?;
        let error_message = types::CompactString.decode(buf)?;
        let group_id = types::CompactString.decode(buf)?;
        let group_state = types::CompactString.decode(buf)?;
        let group_epoch = types::Int32.decode(buf)?;
        let assignment_epoch = types::Int32.decode(buf)?;
        let topology = types::OptionStruct { version }.decode(buf)?;
        let members = types::CompactArray(types::Struct { version }).decode(buf)?;
        let authorized_operations = types::Int32.decode(buf)?;
        let mut unknown_tagged_fields = BTreeMap::new();
        let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
        for _ in 0..num_tagged_fields {
            let tag: u32 = types::UnsignedVarInt.decode(buf)?;
            let size: u32 = types::UnsignedVarInt.decode(buf)?;
            let unknown_value = buf.try_get_bytes(size as usize)?;
            unknown_tagged_fields.insert(tag as i32, unknown_value);
        }
        Ok(Self {
            error_code,
            error_message,
            group_id,
            group_state,
            group_epoch,
            assignment_epoch,
            topology,
            members,
            authorized_operations,
            unknown_tagged_fields,
        })
    }
}

impl Default for DescribedGroup {
    fn default() -> Self {
        Self {
            error_code: 0,
            error_message: None,
            group_id: Default::default(),
            group_state: Default::default(),
            group_epoch: 0,
            assignment_epoch: 0,
            topology: None,
            members: Default::default(),
            authorized_operations: -2147483648,
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for DescribedGroup {
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

#[cfg(feature = "broker")]
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

#[cfg(feature = "client")]
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
pub struct Member {
    /// The member ID.
    ///
    /// Supported API versions: 0
    pub member_id: StrBytes,

    /// The member epoch.
    ///
    /// Supported API versions: 0
    pub member_epoch: i32,

    /// The member instance ID for static membership.
    ///
    /// Supported API versions: 0
    pub instance_id: Option<StrBytes>,

    /// The rack ID.
    ///
    /// Supported API versions: 0
    pub rack_id: Option<StrBytes>,

    /// The client ID.
    ///
    /// Supported API versions: 0
    pub client_id: StrBytes,

    /// The client host.
    ///
    /// Supported API versions: 0
    pub client_host: StrBytes,

    /// The epoch of the topology on the client.
    ///
    /// Supported API versions: 0
    pub topology_epoch: i32,

    /// Identity of the streams instance that may have multiple clients.
    ///
    /// Supported API versions: 0
    pub process_id: StrBytes,

    /// User-defined endpoint for Interactive Queries. Null if not defined for this client.
    ///
    /// Supported API versions: 0
    pub user_endpoint: Option<Endpoint>,

    /// Used for rack-aware assignment algorithm.
    ///
    /// Supported API versions: 0
    pub client_tags: Vec<KeyValue>,

    /// Cumulative changelog offsets for tasks.
    ///
    /// Supported API versions: 0
    pub task_offsets: Vec<TaskOffset>,

    /// Cumulative changelog end offsets for tasks.
    ///
    /// Supported API versions: 0
    pub task_end_offsets: Vec<TaskOffset>,

    /// The current assignment.
    ///
    /// Supported API versions: 0
    pub assignment: Assignment,

    /// The target assignment.
    ///
    /// Supported API versions: 0
    pub target_assignment: Assignment,

    /// True for classic members that have not been upgraded yet.
    ///
    /// Supported API versions: 0
    pub is_classic: bool,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl Member {
    /// Sets `member_id` to the passed value.
    ///
    /// The member ID.
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
    /// Sets `instance_id` to the passed value.
    ///
    /// The member instance ID for static membership.
    ///
    /// Supported API versions: 0
    pub fn with_instance_id(mut self, value: Option<StrBytes>) -> Self {
        self.instance_id = value;
        self
    }
    /// Sets `rack_id` to the passed value.
    ///
    /// The rack ID.
    ///
    /// Supported API versions: 0
    pub fn with_rack_id(mut self, value: Option<StrBytes>) -> Self {
        self.rack_id = value;
        self
    }
    /// Sets `client_id` to the passed value.
    ///
    /// The client ID.
    ///
    /// Supported API versions: 0
    pub fn with_client_id(mut self, value: StrBytes) -> Self {
        self.client_id = value;
        self
    }
    /// Sets `client_host` to the passed value.
    ///
    /// The client host.
    ///
    /// Supported API versions: 0
    pub fn with_client_host(mut self, value: StrBytes) -> Self {
        self.client_host = value;
        self
    }
    /// Sets `topology_epoch` to the passed value.
    ///
    /// The epoch of the topology on the client.
    ///
    /// Supported API versions: 0
    pub fn with_topology_epoch(mut self, value: i32) -> Self {
        self.topology_epoch = value;
        self
    }
    /// Sets `process_id` to the passed value.
    ///
    /// Identity of the streams instance that may have multiple clients.
    ///
    /// Supported API versions: 0
    pub fn with_process_id(mut self, value: StrBytes) -> Self {
        self.process_id = value;
        self
    }
    /// Sets `user_endpoint` to the passed value.
    ///
    /// User-defined endpoint for Interactive Queries. Null if not defined for this client.
    ///
    /// Supported API versions: 0
    pub fn with_user_endpoint(mut self, value: Option<Endpoint>) -> Self {
        self.user_endpoint = value;
        self
    }
    /// Sets `client_tags` to the passed value.
    ///
    /// Used for rack-aware assignment algorithm.
    ///
    /// Supported API versions: 0
    pub fn with_client_tags(mut self, value: Vec<KeyValue>) -> Self {
        self.client_tags = value;
        self
    }
    /// Sets `task_offsets` to the passed value.
    ///
    /// Cumulative changelog offsets for tasks.
    ///
    /// Supported API versions: 0
    pub fn with_task_offsets(mut self, value: Vec<TaskOffset>) -> Self {
        self.task_offsets = value;
        self
    }
    /// Sets `task_end_offsets` to the passed value.
    ///
    /// Cumulative changelog end offsets for tasks.
    ///
    /// Supported API versions: 0
    pub fn with_task_end_offsets(mut self, value: Vec<TaskOffset>) -> Self {
        self.task_end_offsets = value;
        self
    }
    /// Sets `assignment` to the passed value.
    ///
    /// The current assignment.
    ///
    /// Supported API versions: 0
    pub fn with_assignment(mut self, value: Assignment) -> Self {
        self.assignment = value;
        self
    }
    /// Sets `target_assignment` to the passed value.
    ///
    /// The target assignment.
    ///
    /// Supported API versions: 0
    pub fn with_target_assignment(mut self, value: Assignment) -> Self {
        self.target_assignment = value;
        self
    }
    /// Sets `is_classic` to the passed value.
    ///
    /// True for classic members that have not been upgraded yet.
    ///
    /// Supported API versions: 0
    pub fn with_is_classic(mut self, value: bool) -> Self {
        self.is_classic = value;
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
impl Encodable for Member {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        types::CompactString.encode(buf, &self.member_id)?;
        types::Int32.encode(buf, &self.member_epoch)?;
        types::CompactString.encode(buf, &self.instance_id)?;
        types::CompactString.encode(buf, &self.rack_id)?;
        types::CompactString.encode(buf, &self.client_id)?;
        types::CompactString.encode(buf, &self.client_host)?;
        types::Int32.encode(buf, &self.topology_epoch)?;
        types::CompactString.encode(buf, &self.process_id)?;
        types::OptionStruct { version }.encode(buf, &self.user_endpoint)?;
        types::CompactArray(types::Struct { version }).encode(buf, &self.client_tags)?;
        types::CompactArray(types::Struct { version }).encode(buf, &self.task_offsets)?;
        types::CompactArray(types::Struct { version }).encode(buf, &self.task_end_offsets)?;
        types::Struct { version }.encode(buf, &self.assignment)?;
        types::Struct { version }.encode(buf, &self.target_assignment)?;
        types::Boolean.encode(buf, &self.is_classic)?;
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
        total_size += types::CompactString.compute_size(&self.member_id)?;
        total_size += types::Int32.compute_size(&self.member_epoch)?;
        total_size += types::CompactString.compute_size(&self.instance_id)?;
        total_size += types::CompactString.compute_size(&self.rack_id)?;
        total_size += types::CompactString.compute_size(&self.client_id)?;
        total_size += types::CompactString.compute_size(&self.client_host)?;
        total_size += types::Int32.compute_size(&self.topology_epoch)?;
        total_size += types::CompactString.compute_size(&self.process_id)?;
        total_size += types::OptionStruct { version }.compute_size(&self.user_endpoint)?;
        total_size +=
            types::CompactArray(types::Struct { version }).compute_size(&self.client_tags)?;
        total_size +=
            types::CompactArray(types::Struct { version }).compute_size(&self.task_offsets)?;
        total_size +=
            types::CompactArray(types::Struct { version }).compute_size(&self.task_end_offsets)?;
        total_size += types::Struct { version }.compute_size(&self.assignment)?;
        total_size += types::Struct { version }.compute_size(&self.target_assignment)?;
        total_size += types::Boolean.compute_size(&self.is_classic)?;
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
impl Decodable for Member {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        let member_id = types::CompactString.decode(buf)?;
        let member_epoch = types::Int32.decode(buf)?;
        let instance_id = types::CompactString.decode(buf)?;
        let rack_id = types::CompactString.decode(buf)?;
        let client_id = types::CompactString.decode(buf)?;
        let client_host = types::CompactString.decode(buf)?;
        let topology_epoch = types::Int32.decode(buf)?;
        let process_id = types::CompactString.decode(buf)?;
        let user_endpoint = types::OptionStruct { version }.decode(buf)?;
        let client_tags = types::CompactArray(types::Struct { version }).decode(buf)?;
        let task_offsets = types::CompactArray(types::Struct { version }).decode(buf)?;
        let task_end_offsets = types::CompactArray(types::Struct { version }).decode(buf)?;
        let assignment = types::Struct { version }.decode(buf)?;
        let target_assignment = types::Struct { version }.decode(buf)?;
        let is_classic = types::Boolean.decode(buf)?;
        let mut unknown_tagged_fields = BTreeMap::new();
        let num_tagged_fields = types::UnsignedVarInt.decode(buf)?;
        for _ in 0..num_tagged_fields {
            let tag: u32 = types::UnsignedVarInt.decode(buf)?;
            let size: u32 = types::UnsignedVarInt.decode(buf)?;
            let unknown_value = buf.try_get_bytes(size as usize)?;
            unknown_tagged_fields.insert(tag as i32, unknown_value);
        }
        Ok(Self {
            member_id,
            member_epoch,
            instance_id,
            rack_id,
            client_id,
            client_host,
            topology_epoch,
            process_id,
            user_endpoint,
            client_tags,
            task_offsets,
            task_end_offsets,
            assignment,
            target_assignment,
            is_classic,
            unknown_tagged_fields,
        })
    }
}

impl Default for Member {
    fn default() -> Self {
        Self {
            member_id: Default::default(),
            member_epoch: 0,
            instance_id: None,
            rack_id: None,
            client_id: Default::default(),
            client_host: Default::default(),
            topology_epoch: 0,
            process_id: Default::default(),
            user_endpoint: None,
            client_tags: Default::default(),
            task_offsets: Default::default(),
            task_end_offsets: Default::default(),
            assignment: Default::default(),
            target_assignment: Default::default(),
            is_classic: false,
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for Member {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 0 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 0
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct StreamsGroupDescribeResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    ///
    /// Supported API versions: 0
    pub throttle_time_ms: i32,

    /// Each described group.
    ///
    /// Supported API versions: 0
    pub groups: Vec<DescribedGroup>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl StreamsGroupDescribeResponse {
    /// Sets `throttle_time_ms` to the passed value.
    ///
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    ///
    /// Supported API versions: 0
    pub fn with_throttle_time_ms(mut self, value: i32) -> Self {
        self.throttle_time_ms = value;
        self
    }
    /// Sets `groups` to the passed value.
    ///
    /// Each described group.
    ///
    /// Supported API versions: 0
    pub fn with_groups(mut self, value: Vec<DescribedGroup>) -> Self {
        self.groups = value;
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
impl Encodable for StreamsGroupDescribeResponse {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        types::Int32.encode(buf, &self.throttle_time_ms)?;
        types::CompactArray(types::Struct { version }).encode(buf, &self.groups)?;
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
        total_size += types::CompactArray(types::Struct { version }).compute_size(&self.groups)?;
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
impl Decodable for StreamsGroupDescribeResponse {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        let throttle_time_ms = types::Int32.decode(buf)?;
        let groups = types::CompactArray(types::Struct { version }).decode(buf)?;
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
            groups,
            unknown_tagged_fields,
        })
    }
}

impl Default for StreamsGroupDescribeResponse {
    fn default() -> Self {
        Self {
            throttle_time_ms: 0,
            groups: Default::default(),
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for StreamsGroupDescribeResponse {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 0 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

/// Valid versions: 0
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub struct Subtopology {
    /// String to uniquely identify the subtopology.
    ///
    /// Supported API versions: 0
    pub subtopology_id: StrBytes,

    /// The topics the subtopology reads from.
    ///
    /// Supported API versions: 0
    pub source_topics: Vec<super::TopicName>,

    /// The repartition topics the subtopology writes to.
    ///
    /// Supported API versions: 0
    pub repartition_sink_topics: Vec<super::TopicName>,

    /// The set of state changelog topics associated with this subtopology. Created automatically.
    ///
    /// Supported API versions: 0
    pub state_changelog_topics: Vec<TopicInfo>,

    /// The set of source topics that are internally created repartition topics. Created automatically.
    ///
    /// Supported API versions: 0
    pub repartition_source_topics: Vec<TopicInfo>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl Subtopology {
    /// Sets `subtopology_id` to the passed value.
    ///
    /// String to uniquely identify the subtopology.
    ///
    /// Supported API versions: 0
    pub fn with_subtopology_id(mut self, value: StrBytes) -> Self {
        self.subtopology_id = value;
        self
    }
    /// Sets `source_topics` to the passed value.
    ///
    /// The topics the subtopology reads from.
    ///
    /// Supported API versions: 0
    pub fn with_source_topics(mut self, value: Vec<super::TopicName>) -> Self {
        self.source_topics = value;
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
    /// Sets `state_changelog_topics` to the passed value.
    ///
    /// The set of state changelog topics associated with this subtopology. Created automatically.
    ///
    /// Supported API versions: 0
    pub fn with_state_changelog_topics(mut self, value: Vec<TopicInfo>) -> Self {
        self.state_changelog_topics = value;
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
impl Encodable for Subtopology {
    fn encode<B: ByteBufMut>(&self, buf: &mut B, version: i16) -> Result<()> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        types::CompactString.encode(buf, &self.subtopology_id)?;
        types::CompactArray(types::CompactString).encode(buf, &self.source_topics)?;
        types::CompactArray(types::CompactString).encode(buf, &self.repartition_sink_topics)?;
        types::CompactArray(types::Struct { version }).encode(buf, &self.state_changelog_topics)?;
        types::CompactArray(types::Struct { version })
            .encode(buf, &self.repartition_source_topics)?;
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
        total_size += types::CompactArray(types::CompactString)
            .compute_size(&self.repartition_sink_topics)?;
        total_size += types::CompactArray(types::Struct { version })
            .compute_size(&self.state_changelog_topics)?;
        total_size += types::CompactArray(types::Struct { version })
            .compute_size(&self.repartition_source_topics)?;
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
impl Decodable for Subtopology {
    fn decode<B: ByteBuf>(buf: &mut B, version: i16) -> Result<Self> {
        if version != 0 {
            bail!("specified version not supported by this message type");
        }
        let subtopology_id = types::CompactString.decode(buf)?;
        let source_topics = types::CompactArray(types::CompactString).decode(buf)?;
        let repartition_sink_topics = types::CompactArray(types::CompactString).decode(buf)?;
        let state_changelog_topics = types::CompactArray(types::Struct { version }).decode(buf)?;
        let repartition_source_topics =
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
            subtopology_id,
            source_topics,
            repartition_sink_topics,
            state_changelog_topics,
            repartition_source_topics,
            unknown_tagged_fields,
        })
    }
}

impl Default for Subtopology {
    fn default() -> Self {
        Self {
            subtopology_id: Default::default(),
            source_topics: Default::default(),
            repartition_sink_topics: Default::default(),
            state_changelog_topics: Default::default(),
            repartition_source_topics: Default::default(),
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

#[cfg(feature = "broker")]
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

#[cfg(feature = "client")]
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

#[cfg(feature = "broker")]
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

#[cfg(feature = "client")]
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
    /// The epoch of the currently initialized topology for this group.
    ///
    /// Supported API versions: 0
    pub epoch: i32,

    /// The subtopologies of the streams application. This contains the configured subtopologies, where the number of partitions are set and any regular expressions are resolved to actual topics. Null if the group is uninitialized, source topics are missing or incorrectly partitioned.
    ///
    /// Supported API versions: 0
    pub subtopologies: Option<Vec<Subtopology>>,

    /// Other tagged fields
    pub unknown_tagged_fields: BTreeMap<i32, Bytes>,
}

impl Topology {
    /// Sets `epoch` to the passed value.
    ///
    /// The epoch of the currently initialized topology for this group.
    ///
    /// Supported API versions: 0
    pub fn with_epoch(mut self, value: i32) -> Self {
        self.epoch = value;
        self
    }
    /// Sets `subtopologies` to the passed value.
    ///
    /// The subtopologies of the streams application. This contains the configured subtopologies, where the number of partitions are set and any regular expressions are resolved to actual topics. Null if the group is uninitialized, source topics are missing or incorrectly partitioned.
    ///
    /// Supported API versions: 0
    pub fn with_subtopologies(mut self, value: Option<Vec<Subtopology>>) -> Self {
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

#[cfg(feature = "broker")]
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

#[cfg(feature = "client")]
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
            subtopologies: None,
            unknown_tagged_fields: BTreeMap::new(),
        }
    }
}

impl Message for Topology {
    const VERSIONS: VersionRange = VersionRange { min: 0, max: 0 };
    const DEPRECATED_VERSIONS: Option<VersionRange> = None;
}

impl HeaderVersion for StreamsGroupDescribeResponse {
    fn header_version(version: i16) -> i16 {
        1
    }
}
