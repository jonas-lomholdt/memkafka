use std::{
    collections::{BTreeMap, HashMap},
    error::Error,
    fmt,
    num::NonZeroU32,
    sync::Arc,
};

use tokio::sync::RwLock;
use uuid::Uuid;

use super::partition::PartitionLog;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TopicMetadata {
    pub id: Uuid,
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
    state: Arc<RwLock<CatalogState>>,
}

#[derive(Debug, Default)]
struct CatalogState {
    topics_by_name: BTreeMap<String, TopicEntry>,
    names_by_id: HashMap<Uuid, String>,
}

#[derive(Debug)]
struct TopicEntry {
    metadata: TopicMetadata,
    partitions: Vec<Arc<PartitionLog>>,
}

impl TopicEntry {
    fn new(metadata: TopicMetadata) -> Self {
        let partitions = (0..metadata.partition_count)
            .map(|_| Arc::new(PartitionLog::default()))
            .collect();
        Self {
            metadata,
            partitions,
        }
    }
}

impl TopicCatalog {
    pub fn new(default_partitions: NonZeroU32) -> Self {
        Self {
            default_partitions,
            state: Arc::new(RwLock::new(CatalogState::default())),
        }
    }

    pub async fn create_explicit(
        &self,
        name: &str,
        partitions: i32,
        replication_factor: i16,
    ) -> Result<TopicMetadata, TopicError> {
        let mut state = self.state.write().await;
        let partition_count = validate_explicit_definition(name, partitions, replication_factor)?;
        if state.topics_by_name.contains_key(name) {
            return Err(TopicError::AlreadyExists);
        }
        Ok(insert_topic(&mut state, name, partition_count))
    }

    pub async fn validate_explicit(
        &self,
        name: &str,
        partitions: i32,
        replication_factor: i16,
    ) -> Result<(), TopicError> {
        validate_explicit_definition(name, partitions, replication_factor)?;
        if self.state.read().await.topics_by_name.contains_key(name) {
            Err(TopicError::AlreadyExists)
        } else {
            Ok(())
        }
    }

    pub async fn get(&self, name: &str) -> Result<Option<TopicMetadata>, TopicError> {
        validate_name(name)?;
        Ok(self
            .state
            .read()
            .await
            .topics_by_name
            .get(name)
            .map(|entry| entry.metadata.clone()))
    }

    pub async fn get_by_id(&self, id: Uuid) -> Option<TopicMetadata> {
        let state = self.state.read().await;
        let name = state.names_by_id.get(&id)?;
        state
            .topics_by_name
            .get(name)
            .map(|entry| entry.metadata.clone())
    }

    pub async fn get_or_auto_create(
        &self,
        name: &str,
        allow_auto_create: bool,
    ) -> Result<Option<TopicMetadata>, TopicError> {
        validate_name(name)?;

        if let Some(entry) = self.state.read().await.topics_by_name.get(name) {
            return Ok(Some(entry.metadata.clone()));
        }
        if !allow_auto_create {
            return Ok(None);
        }

        let mut state = self.state.write().await;
        if let Some(entry) = state.topics_by_name.get(name) {
            return Ok(Some(entry.metadata.clone()));
        }
        let metadata = insert_topic(&mut state, name, self.default_partitions.get());
        Ok(Some(metadata))
    }

    pub async fn list(&self) -> Vec<TopicMetadata> {
        self.state
            .read()
            .await
            .topics_by_name
            .values()
            .map(|entry| entry.metadata.clone())
            .collect()
    }

    pub(crate) async fn partition(&self, topic: &str, partition: i32) -> Option<Arc<PartitionLog>> {
        let partition = usize::try_from(partition).ok()?;
        self.state
            .read()
            .await
            .topics_by_name
            .get(topic)?
            .partitions
            .get(partition)
            .cloned()
    }
}

fn validate_explicit_definition(
    name: &str,
    partitions: i32,
    replication_factor: i16,
) -> Result<u32, TopicError> {
    validate_name(name)?;
    let partition_count = u32::try_from(partitions).map_err(|_| TopicError::InvalidPartitions)?;
    if partition_count == 0 {
        return Err(TopicError::InvalidPartitions);
    }
    if replication_factor != 1 {
        return Err(TopicError::InvalidReplicationFactor);
    }

    Ok(partition_count)
}

fn insert_topic(state: &mut CatalogState, name: &str, partition_count: u32) -> TopicMetadata {
    let metadata = TopicMetadata {
        id: next_topic_id(state),
        name: name.to_owned(),
        partition_count,
    };
    state
        .topics_by_name
        .insert(name.to_owned(), TopicEntry::new(metadata.clone()));
    state.names_by_id.insert(metadata.id, name.to_owned());
    metadata
}

fn next_topic_id(state: &CatalogState) -> Uuid {
    loop {
        let candidate = Uuid::new_v4();
        if !candidate.is_nil() && !state.names_by_id.contains_key(&candidate) {
            return candidate;
        }
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
    use std::{num::NonZeroU32, sync::Arc};

    use tokio::task::JoinSet;

    use super::{TopicCatalog, TopicError};

    #[tokio::test]
    async fn duplicate_explicit_creation_preserves_original_metadata() {
        let catalog = catalog_with_two_defaults();

        let created = catalog
            .create_explicit("events", 6, 1)
            .await
            .expect("create topic");
        let duplicate = catalog.create_explicit("events", 3, 1).await;

        assert_eq!(created.name, "events");
        assert_eq!(created.partition_count, 6);
        assert_eq!(duplicate, Err(TopicError::AlreadyExists));
        assert_eq!(catalog.get("events").await, Ok(Some(created.clone())));
        assert_eq!(catalog.get_by_id(created.id).await, Some(created.clone()));
        assert_eq!(catalog.list().await, vec![created]);
    }

    #[tokio::test]
    async fn created_topic_has_one_stable_non_nil_id_in_both_indexes() {
        let catalog = catalog_with_two_defaults();
        let created = catalog
            .create_explicit("events", 3, 1)
            .await
            .expect("create topic");

        assert!(!created.id.is_nil());
        assert_eq!(catalog.get("events").await, Ok(Some(created.clone())));
        assert_eq!(catalog.get_by_id(created.id).await, Some(created.clone()));
        assert_eq!(catalog.list().await, vec![created]);
    }

    #[tokio::test]
    async fn separate_topics_receive_distinct_ids() {
        let catalog = catalog_with_two_defaults();
        let first = catalog.create_explicit("a", 1, 1).await.expect("first");
        let second = catalog.create_explicit("b", 1, 1).await.expect("second");

        assert_ne!(first.id, second.id);
    }

    #[tokio::test]
    async fn validation_and_failed_creation_do_not_mutate_identity_indexes() {
        let catalog = catalog_with_two_defaults();

        assert_eq!(catalog.validate_explicit("valid", 2, 1).await, Ok(()));
        assert_eq!(
            catalog.create_explicit("invalid", 0, 1).await,
            Err(TopicError::InvalidPartitions)
        );
        assert!(catalog.list().await.is_empty());
        assert_eq!(catalog.get("valid").await, Ok(None));
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
        let mut created = Vec::new();

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
            created.push(
                result
                    .expect("auto-create task panicked")
                    .expect("auto-created topic"),
            );
        }
        let id = created.first().expect("at least one result").id;
        assert!(!id.is_nil());
        assert_eq!(created.len(), 32);
        assert!(created.iter().all(|metadata| metadata.id == id));
        assert!(
            created
                .iter()
                .all(|metadata| metadata.name == "events" && metadata.partition_count == 2)
        );
        assert_eq!(catalog.list().await, vec![created[0].clone()]);
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

    #[tokio::test]
    async fn created_topics_own_one_distinct_log_per_partition() {
        let catalog = catalog_with_two_defaults();
        catalog
            .create_explicit("events", 3, 1)
            .await
            .expect("create topic");

        let zero = catalog.partition("events", 0).await.expect("partition 0");
        let one = catalog.partition("events", 1).await.expect("partition 1");
        let two = catalog.partition("events", 2).await.expect("partition 2");

        assert!(!Arc::ptr_eq(&zero, &one));
        assert!(!Arc::ptr_eq(&one, &two));
        assert_eq!(zero.next_offset().await, 0);
        assert_eq!(one.next_offset().await, 0);
        assert_eq!(two.next_offset().await, 0);
        assert!(catalog.partition("events", -1).await.is_none());
        assert!(catalog.partition("events", 3).await.is_none());
        assert!(catalog.partition("missing", 0).await.is_none());
    }

    fn catalog_with_two_defaults() -> TopicCatalog {
        TopicCatalog::new(NonZeroU32::new(2).expect("nonzero literal"))
    }
}
