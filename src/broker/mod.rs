use std::{num::NonZeroU32, sync::Arc};

use tokio::sync::Notify;

pub(crate) mod groups;
pub(crate) mod partition;
pub(crate) mod producers;
pub mod topics;

use groups::GroupCoordinator;
use producers::ProducerCoordinator;
use topics::TopicCatalog;

#[derive(Clone, Debug)]
pub struct BrokerState {
    broker_id: i32,
    auto_create_topics: bool,
    force_auto_create_topics: bool,
    topics: TopicCatalog,
    groups: GroupCoordinator,
    producers: ProducerCoordinator,
    append_notification: Arc<Notify>,
}

impl BrokerState {
    pub fn new(
        broker_id: i32,
        auto_create_topics: bool,
        force_auto_create_topics: bool,
        default_partitions: NonZeroU32,
    ) -> Self {
        Self {
            broker_id,
            auto_create_topics,
            force_auto_create_topics,
            topics: TopicCatalog::new(default_partitions),
            groups: GroupCoordinator::new(),
            producers: ProducerCoordinator::new(),
            append_notification: Arc::new(Notify::new()),
        }
    }

    pub fn broker_id(&self) -> i32 {
        self.broker_id
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

    pub(crate) fn producers(&self) -> &ProducerCoordinator {
        &self.producers
    }

    pub(crate) fn append_notification(&self) -> Arc<Notify> {
        Arc::clone(&self.append_notification)
    }

    pub(crate) fn notify_append(&self) {
        self.append_notification.notify_waiters();
    }
}
