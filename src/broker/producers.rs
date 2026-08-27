use std::{collections::HashMap, error::Error, fmt, sync::Arc};

use tokio::sync::Mutex;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ProducerIdentity {
    pub(crate) producer_id: i64,
    pub(crate) producer_epoch: i16,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ProducerError {
    IdExhausted,
    UnknownProducerId,
    InvalidProducerEpoch,
}

impl fmt::Display for ProducerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::IdExhausted => "producer ID space exhausted",
            Self::UnknownProducerId => "unknown producer ID",
            Self::InvalidProducerEpoch => "invalid producer epoch",
        })
    }
}

impl Error for ProducerError {}

#[derive(Clone, Debug)]
pub(crate) struct ProducerCoordinator {
    inner: Arc<Mutex<ProducerCoordinatorInner>>,
}

#[derive(Debug)]
struct ProducerCoordinatorInner {
    next_id: i64,
    epochs: HashMap<i64, i16>,
}

impl ProducerCoordinator {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(ProducerCoordinatorInner {
                next_id: 0,
                epochs: HashMap::new(),
            })),
        }
    }

    pub(crate) async fn allocate(&self) -> Result<ProducerIdentity, ProducerError> {
        let mut inner = self.inner.lock().await;
        let producer_id = inner
            .next_id
            .checked_add(1)
            .ok_or(ProducerError::IdExhausted)?;
        inner.next_id = producer_id;
        inner.epochs.insert(producer_id, 0);
        Ok(ProducerIdentity {
            producer_id,
            producer_epoch: 0,
        })
    }

    pub(crate) async fn validate(
        &self,
        producer_id: i64,
        producer_epoch: i16,
    ) -> Result<(), ProducerError> {
        let inner = self.inner.lock().await;
        match inner.epochs.get(&producer_id) {
            None => Err(ProducerError::UnknownProducerId),
            Some(expected_epoch) if *expected_epoch == producer_epoch => Ok(()),
            Some(_) => Err(ProducerError::InvalidProducerEpoch),
        }
    }
}

impl Default for ProducerCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::{ProducerCoordinator, ProducerError, ProducerIdentity};

    #[tokio::test]
    async fn allocates_positive_ids_and_validates_epoch_zero() {
        let coordinator = ProducerCoordinator::new();
        let first = coordinator.allocate().await.expect("first producer");
        let second = coordinator.allocate().await.expect("second producer");

        assert_eq!(
            first,
            ProducerIdentity {
                producer_id: 1,
                producer_epoch: 0
            }
        );
        assert_eq!(
            second,
            ProducerIdentity {
                producer_id: 2,
                producer_epoch: 0
            }
        );
        assert_eq!(coordinator.validate(1, 0).await, Ok(()));
        assert_eq!(
            coordinator.validate(99, 0).await,
            Err(ProducerError::UnknownProducerId)
        );
        assert_eq!(
            coordinator.validate(1, 1).await,
            Err(ProducerError::InvalidProducerEpoch)
        );
    }
}
