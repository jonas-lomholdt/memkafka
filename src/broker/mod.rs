use std::{num::NonZeroU32, sync::Arc};

use tokio::sync::Notify;

use crate::config::AdvertisedAddress;

pub(crate) mod groups;
pub(crate) mod partition;
pub mod topics;

use groups::GroupCoordinator;
use topics::TopicCatalog;

#[derive(Clone, Debug)]
pub struct BrokerState {
    broker_id: i32,
    advertised_kafka: AdvertisedAddress,
    auto_create_topics: bool,
    force_auto_create_topics: bool,
    topics: TopicCatalog,
    groups: GroupCoordinator,
    append_notification: Arc<Notify>,
}

impl BrokerState {
    pub fn new(
        broker_id: i32,
        advertised_kafka: AdvertisedAddress,
        auto_create_topics: bool,
        force_auto_create_topics: bool,
        default_partitions: NonZeroU32,
    ) -> Self {
        Self {
            broker_id,
            advertised_kafka,
            auto_create_topics,
            force_auto_create_topics,
            topics: TopicCatalog::new(default_partitions),
            groups: GroupCoordinator::new(),
            append_notification: Arc::new(Notify::new()),
        }
    }

    pub fn broker_id(&self) -> i32 {
        self.broker_id
    }

    pub fn advertised_kafka(&self) -> &AdvertisedAddress {
        &self.advertised_kafka
    }

    pub fn auto_create_topics(&self) -> bool {
        self.auto_create_topics
    }

    pub fn force_auto_create_topics(&self) -> bool {
        self.force_auto_create_topics
    }

    pub fn topics(&self) -> &TopicCatalog {
        &self.topics
    }

    pub(crate) fn groups(&self) -> &GroupCoordinator {
        &self.groups
    }

    pub(crate) fn append_notification(&self) -> Arc<Notify> {
        Arc::clone(&self.append_notification)
    }

    pub(crate) fn notify_append(&self) {
        self.append_notification.notify_waiters();
    }
}
