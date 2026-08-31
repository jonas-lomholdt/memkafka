mod api_versions;
#[doc(hidden)]
pub mod capabilities;
pub mod codec;
pub mod connection;
mod create_topics;
mod describe_configs;
mod describe_groups;
pub mod dispatcher;
#[allow(dead_code)] // Task 6 connects the pure rejection builders to request routing.
mod error_response;
mod fetch;
mod find_coordinator;
pub mod frame;
mod group_error;
mod heartbeat;
mod init_producer_id;
mod join_group;
mod leave_group;
mod list_groups;
mod list_offsets;
mod metadata;
mod offset_commit;
mod offset_fetch;
mod produce;
mod sync_group;
