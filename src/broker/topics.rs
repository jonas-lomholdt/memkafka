use std::{
    collections::{BTreeMap, btree_map::Entry},
    error::Error,
    fmt,
    num::NonZeroU32,
    sync::Arc,
};

use tokio::sync::RwLock;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TopicMetadata {
    pub name: String,
    pub partition_count: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TopicError {
    AlreadyExists,
    InvalidName,
    InvalidPartitions,
    InvalidReplicationFactor,
}

impl fmt::Display for TopicError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::AlreadyExists => "topic already exists",
            Self::InvalidName => "invalid topic name",
            Self::InvalidPartitions => "partition count must be positive",
            Self::InvalidReplicationFactor => "replication factor must be 1",
        })
    }
}

impl Error for TopicError {}

#[derive(Clone, Debug)]
pub struct TopicCatalog {
    default_partitions: NonZeroU32,
    topics: Arc<RwLock<BTreeMap<String, TopicMetadata>>>,
}

impl TopicCatalog {
    pub fn new(default_partitions: NonZeroU32) -> Self {
        Self {
            default_partitions,
            topics: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    pub async fn create_explicit(
        &self,
        name: &str,
        partitions: i32,
        replication_factor: i16,
    ) -> Result<TopicMetadata, TopicError> {
        validate_name(name)?;
        let partition_count =
            u32::try_from(partitions).map_err(|_| TopicError::InvalidPartitions)?;
        if partition_count == 0 {
            return Err(TopicError::InvalidPartitions);
        }
        if replication_factor != 1 {
            return Err(TopicError::InvalidReplicationFactor);
        }

        let metadata = TopicMetadata {
            name: name.to_owned(),
            partition_count,
        };
        let mut topics = self.topics.write().await;
        match topics.entry(name.to_owned()) {
            Entry::Vacant(entry) => {
                entry.insert(metadata.clone());
                Ok(metadata)
            }
            Entry::Occupied(_) => Err(TopicError::AlreadyExists),
        }
    }

    pub async fn get_or_auto_create(
        &self,
        name: &str,
        allow_auto_create: bool,
    ) -> Result<Option<TopicMetadata>, TopicError> {
        validate_name(name)?;

        if let Some(metadata) = self.topics.read().await.get(name).cloned() {
            return Ok(Some(metadata));
        }
        if !allow_auto_create {
            return Ok(None);
        }

        let mut topics = self.topics.write().await;
        if let Some(metadata) = topics.get(name) {
            return Ok(Some(metadata.clone()));
        }
        let metadata = TopicMetadata {
            name: name.to_owned(),
            partition_count: self.default_partitions.get(),
        };
        topics.insert(name.to_owned(), metadata.clone());
        Ok(Some(metadata))
    }

    pub async fn list(&self) -> Vec<TopicMetadata> {
        self.topics.read().await.values().cloned().collect()
    }
}

fn validate_name(name: &str) -> Result<(), TopicError> {
    let valid = !name.is_empty()
        && name.len() <= 249
        && name != "."
        && name != ".."
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'));

    if valid {
        Ok(())
    } else {
        Err(TopicError::InvalidName)
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use tokio::task::JoinSet;

    use super::{TopicCatalog, TopicError, TopicMetadata};

    #[tokio::test]
    async fn duplicate_explicit_creation_preserves_original_metadata() {
        let catalog = catalog_with_two_defaults();

        let created = catalog
            .create_explicit("events", 6, 1)
            .await
            .expect("create topic");
        let duplicate = catalog.create_explicit("events", 3, 1).await;

        assert_eq!(
            created,
            TopicMetadata {
                name: "events".to_owned(),
                partition_count: 6,
            }
        );
        assert_eq!(duplicate, Err(TopicError::AlreadyExists));
        assert_eq!(catalog.list().await, vec![created]);
    }

    #[tokio::test]
    async fn explicit_creation_rejects_invalid_partition_and_replication_counts() {
        let catalog = catalog_with_two_defaults();

        assert_eq!(
            catalog.create_explicit("no-partitions", 0, 1).await,
            Err(TopicError::InvalidPartitions)
        );
        assert_eq!(
            catalog.create_explicit("replicated", 3, 2).await,
            Err(TopicError::InvalidReplicationFactor)
        );
        assert!(catalog.list().await.is_empty());
    }

    #[tokio::test]
    async fn invalid_topic_names_never_enter_the_catalog() {
        let catalog = catalog_with_two_defaults();
        let too_long = "a".repeat(250);

        for invalid_name in ["", ".", "..", "has/slash", "has space", &too_long] {
            assert_eq!(
                catalog.create_explicit(invalid_name, 1, 1).await,
                Err(TopicError::InvalidName),
                "name {invalid_name:?} should be rejected"
            );
        }
        assert!(catalog.list().await.is_empty());
    }

    #[tokio::test]
    async fn concurrent_auto_creation_inserts_one_default_topic() {
        let catalog = catalog_with_two_defaults();
        let mut tasks = JoinSet::new();

        for _ in 0..32 {
            let catalog = catalog.clone();
            tasks.spawn(async move {
                catalog
                    .get_or_auto_create("events", true)
                    .await
                    .expect("auto-create topic")
            });
        }

        while let Some(result) = tasks.join_next().await {
            assert_eq!(
                result.expect("auto-create task panicked"),
                Some(TopicMetadata {
                    name: "events".to_owned(),
                    partition_count: 2,
                })
            );
        }
        assert_eq!(
            catalog.list().await,
            vec![TopicMetadata {
                name: "events".to_owned(),
                partition_count: 2,
            }]
        );
    }

    #[tokio::test]
    async fn disabled_auto_creation_returns_none_without_mutating() {
        let catalog = catalog_with_two_defaults();

        assert_eq!(
            catalog
                .get_or_auto_create("events", false)
                .await
                .expect("look up topic"),
            None
        );
        assert!(catalog.list().await.is_empty());
    }

    fn catalog_with_two_defaults() -> TopicCatalog {
        TopicCatalog::new(NonZeroU32::new(2).expect("nonzero literal"))
    }
}
