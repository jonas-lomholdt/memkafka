use std::num::NonZeroU32;

use crate::config::AdvertisedAddress;

pub mod topics;

use topics::TopicCatalog;

#[derive(Clone, Debug)]
pub struct BrokerState {
    broker_id: i32,
    advertised_kafka: AdvertisedAddress,
    auto_create_topics: bool,
    topics: TopicCatalog,
}

impl BrokerState {
    pub fn new(
        broker_id: i32,
        advertised_kafka: AdvertisedAddress,
        auto_create_topics: bool,
        default_partitions: NonZeroU32,
    ) -> Self {
        Self {
            broker_id,
            advertised_kafka,
            auto_create_topics,
            topics: TopicCatalog::new(default_partitions),
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

    pub fn topics(&self) -> &TopicCatalog {
        &self.topics
    }
}
