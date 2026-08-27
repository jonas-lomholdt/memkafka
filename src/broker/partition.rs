use std::{
    collections::{HashMap, VecDeque},
    error::Error,
    fmt,
};

use bytes::{Bytes, BytesMut};
use kafka_protocol::records::{NO_PRODUCER_EPOCH, NO_PRODUCER_ID, NO_SEQUENCE, RecordBatchDecoder};
use tokio::sync::Mutex;

const FIXED_BATCH_SIZE: usize = 61;
const BATCH_LENGTH_OFFSET: usize = 8;
const MAGIC_OFFSET: usize = 16;
const CRC_OFFSET: usize = 17;
const LAST_OFFSET_DELTA_OFFSET: usize = 23;
const RECORD_COUNT_OFFSET: usize = 57;
const RECENT_APPEND_LIMIT: usize = 5;
const SEQUENCE_MODULUS: i64 = i32::MAX as i64 + 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AppendError {
    Malformed,
    UnsupportedBatch,
    OffsetOverflow,
    OutOfOrderSequence,
    DuplicateSequence,
}

impl fmt::Display for AppendError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Malformed => "malformed record batch",
            Self::UnsupportedBatch => "unsupported record batch",
            Self::OffsetOverflow => "partition offset overflow",
            Self::OutOfOrderSequence => "out-of-order producer sequence",
            Self::DuplicateSequence => "duplicate producer sequence",
        })
    }
}

impl Error for AppendError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FetchError {
    OutOfRange,
}

impl fmt::Display for FetchError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("fetch offset is outside the partition log")
    }
}

impl Error for FetchError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct AppendResult {
    pub(crate) base_offset: i64,
    pub(crate) last_offset: i64,
    pub(crate) record_count: i32,
    pub(crate) appended: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RecordSetProducer {
    pub(crate) producer_id: i64,
    pub(crate) producer_epoch: i16,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct FetchSnapshot {
    pub(crate) records: Bytes,
    pub(crate) high_watermark: i64,
}

#[derive(Debug, Default)]
pub(crate) struct PartitionLog {
    inner: Mutex<PartitionLogInner>,
}

#[derive(Debug, Default)]
struct PartitionLogInner {
    next_offset: i64,
    batches: Vec<StoredBatch>,
    producer_states: HashMap<i64, ProducerPartitionState>,
}

#[derive(Debug)]
struct ProducerPartitionState {
    producer_epoch: i16,
    next_sequence: i32,
    recent: VecDeque<RecentAppend>,
}

#[derive(Debug)]
struct RecentAppend {
    base_sequence: i32,
    fingerprints: Vec<(i32, u32)>,
    result: AppendResult,
}

#[derive(Debug)]
struct StoredBatch {
    base_offset: i64,
    last_offset: i64,
    record_count: i32,
    bytes: Bytes,
}

#[derive(Debug)]
struct ValidatedBatch {
    record_count: i32,
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,
    crc: u32,
    bytes: Bytes,
}

impl PartitionLog {
    pub(crate) async fn append(&self, records: Bytes) -> Result<AppendResult, AppendError> {
        let validated = validate_record_set(records)?;
        let total_record_count = validated.iter().try_fold(0_i32, |total, batch| {
            total
                .checked_add(batch.record_count)
                .ok_or(AppendError::OffsetOverflow)
        })?;
        let first = &validated[0];
        let producer = (first.producer_id != NO_PRODUCER_ID).then_some(RecordSetProducer {
            producer_id: first.producer_id,
            producer_epoch: first.producer_epoch,
        });
        let base_sequence = first.base_sequence;
        let fingerprints = validated
            .iter()
            .map(|batch| (batch.record_count, batch.crc))
            .collect::<Vec<_>>();

        let mut inner = self.inner.lock().await;
        let next_sequence = if let Some(producer) = producer {
            let state = inner.producer_states.get(&producer.producer_id);
            let expected_sequence = state.map_or(0, |state| state.next_sequence);

            if let Some(state) = state {
                if state.producer_epoch != producer.producer_epoch {
                    return Err(AppendError::OutOfOrderSequence);
                }
                if let Some(recent) = state.recent.iter().find(|recent| {
                    recent.base_sequence == base_sequence && recent.fingerprints == fingerprints
                }) {
                    return Ok(AppendResult {
                        appended: false,
                        ..recent.result
                    });
                }
            }

            validate_sequences(&validated, expected_sequence)?;
            Some(next_sequence(base_sequence, total_record_count))
        } else {
            None
        };

        let base_offset = inner.next_offset;
        let next_offset = base_offset
            .checked_add(i64::from(total_record_count))
            .ok_or(AppendError::OffsetOverflow)?;
        let last_offset = next_offset - 1;
        let mut assigned_offset = base_offset;
        let mut stored = Vec::with_capacity(validated.len());

        for batch in validated {
            let batch_last_offset = assigned_offset + i64::from(batch.record_count) - 1;
            let mut bytes = BytesMut::from(batch.bytes.as_ref());
            bytes[..8].copy_from_slice(&assigned_offset.to_be_bytes());
            stored.push(StoredBatch {
                base_offset: assigned_offset,
                last_offset: batch_last_offset,
                record_count: batch.record_count,
                bytes: bytes.freeze(),
            });
            assigned_offset = batch_last_offset + 1;
        }

        let result = AppendResult {
            base_offset,
            last_offset,
            record_count: total_record_count,
            appended: true,
        };

        inner.batches.extend(stored);
        inner.next_offset = next_offset;
        if let (Some(producer), Some(next_sequence)) = (producer, next_sequence) {
            let state = inner
                .producer_states
                .entry(producer.producer_id)
                .or_insert_with(|| ProducerPartitionState {
                    producer_epoch: producer.producer_epoch,
                    next_sequence,
                    recent: VecDeque::with_capacity(RECENT_APPEND_LIMIT),
                });
            state.next_sequence = next_sequence;
            while state.recent.len() >= RECENT_APPEND_LIMIT {
                state.recent.pop_front();
            }
            state.recent.push_back(RecentAppend {
                base_sequence,
                fingerprints,
                result,
            });
        }

        Ok(result)
    }

    pub(crate) async fn fetch(
        &self,
        offset: i64,
        partition_max_bytes: usize,
    ) -> Result<FetchSnapshot, FetchError> {
        let inner = self.inner.lock().await;
        if offset < 0 || offset > inner.next_offset {
            return Err(FetchError::OutOfRange);
        }

        let mut records = BytesMut::new();
        for batch in inner
            .batches
            .iter()
            .skip_while(|batch| batch.last_offset < offset)
        {
            debug_assert_eq!(
                batch.last_offset - batch.base_offset + 1,
                i64::from(batch.record_count)
            );
            let exceeds_limit = records
                .len()
                .checked_add(batch.bytes.len())
                .is_none_or(|size| size > partition_max_bytes);
            if !records.is_empty() && exceeds_limit {
                break;
            }
            records.extend_from_slice(&batch.bytes);
        }

        Ok(FetchSnapshot {
            records: records.freeze(),
            high_watermark: inner.next_offset,
        })
    }

    pub(crate) async fn next_offset(&self) -> i64 {
        self.inner.lock().await.next_offset
    }
}

#[allow(
    dead_code,
    reason = "consumed by Produce identity validation in Task 4"
)]
pub(crate) fn record_set_producer(
    records: &Bytes,
) -> Result<Option<RecordSetProducer>, AppendError> {
    let validated = validate_record_set(records.clone())?;
    let first = &validated[0];
    Ok(
        (first.producer_id != NO_PRODUCER_ID).then_some(RecordSetProducer {
            producer_id: first.producer_id,
            producer_epoch: first.producer_epoch,
        }),
    )
}

fn validate_sequences(
    validated: &[ValidatedBatch],
    expected_sequence: i32,
) -> Result<(), AppendError> {
    let mut expected = expected_sequence;
    for batch in validated {
        if batch.base_sequence != expected {
            return Err(classify_sequence_mismatch(batch.base_sequence, expected));
        }
        expected = next_sequence(expected, batch.record_count);
    }
    Ok(())
}

fn classify_sequence_mismatch(received: i32, expected: i32) -> AppendError {
    let forward_distance = (i64::from(received) - i64::from(expected)).rem_euclid(SEQUENCE_MODULUS);
    if forward_distance < SEQUENCE_MODULUS / 2 {
        AppendError::OutOfOrderSequence
    } else {
        AppendError::DuplicateSequence
    }
}

fn next_sequence(base_sequence: i32, record_count: i32) -> i32 {
    ((i64::from(base_sequence) + i64::from(record_count)) % SEQUENCE_MODULUS) as i32
}

fn validate_record_set(records: Bytes) -> Result<Vec<ValidatedBatch>, AppendError> {
    if records.is_empty() {
        return Err(AppendError::Malformed);
    }

    let mut validated = Vec::new();
    let mut position = 0;
    while position < records.len() {
        let remaining = &records[position..];
        if remaining.len() < FIXED_BATCH_SIZE {
            return Err(AppendError::Malformed);
        }

        let batch_length = i32::from_be_bytes(
            remaining[BATCH_LENGTH_OFFSET..BATCH_LENGTH_OFFSET + 4]
                .try_into()
                .expect("four-byte slice"),
        );
        let batch_length = usize::try_from(batch_length).map_err(|_| AppendError::Malformed)?;
        let total_length = 12_usize
            .checked_add(batch_length)
            .ok_or(AppendError::Malformed)?;
        if total_length < FIXED_BATCH_SIZE || total_length > remaining.len() {
            return Err(AppendError::Malformed);
        }

        let batch = records.slice(position..position + total_length);
        if batch[MAGIC_OFFSET] != 2 {
            return Err(AppendError::UnsupportedBatch);
        }

        let mut decode_input = batch.clone();
        let info = RecordBatchDecoder::decode_batch_info(&mut decode_input)
            .map_err(|_| AppendError::Malformed)?;
        if !decode_input.is_empty() || info.len() != 1 {
            return Err(AppendError::Malformed);
        }
        let info = &info[0];
        if info.transactional || info.control {
            return Err(AppendError::UnsupportedBatch);
        }
        // Preserve legacy RecordBatch inputs that encoded an unused base sequence as zero.
        let non_idempotent = info.producer_id == NO_PRODUCER_ID
            && info.producer_epoch == NO_PRODUCER_EPOCH
            && matches!(info.base_sequence, NO_SEQUENCE | 0);
        let idempotent = info.producer_id != NO_PRODUCER_ID
            && info.producer_epoch >= 0
            && info.base_sequence >= 0;
        if !non_idempotent && !idempotent {
            return Err(AppendError::Malformed);
        }

        let last_offset_delta = i32::from_be_bytes(
            batch[LAST_OFFSET_DELTA_OFFSET..LAST_OFFSET_DELTA_OFFSET + 4]
                .try_into()
                .expect("four-byte slice"),
        );
        let record_count = i32::from_be_bytes(
            batch[RECORD_COUNT_OFFSET..RECORD_COUNT_OFFSET + 4]
                .try_into()
                .expect("four-byte slice"),
        );
        if record_count <= 0
            || info.record_count != record_count
            || last_offset_delta != record_count - 1
        {
            return Err(AppendError::Malformed);
        }

        validated.push(ValidatedBatch {
            record_count,
            producer_id: info.producer_id,
            producer_epoch: info.producer_epoch,
            base_sequence: info.base_sequence,
            crc: u32::from_be_bytes(
                batch[CRC_OFFSET..CRC_OFFSET + 4]
                    .try_into()
                    .expect("four-byte slice"),
            ),
            bytes: batch,
        });
        position += total_length;
    }

    let first = &validated[0];
    if validated.iter().any(|batch| {
        batch.producer_id != first.producer_id || batch.producer_epoch != first.producer_epoch
    }) {
        return Err(AppendError::Malformed);
    }

    Ok(validated)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::{Buf, Bytes, BytesMut};
    use kafka_protocol::{
        protocol::StrBytes,
        records::{
            Compression, NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH, NO_PRODUCER_ID, NO_SEQUENCE,
            Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
        },
    };
    use tokio::task::JoinSet;

    use super::{
        AppendError, FetchError, PartitionLog, ProducerPartitionState, RecordSetProducer,
        record_set_producer,
    };

    #[tokio::test]
    async fn appends_batches_with_contiguous_offsets_and_fetches_in_order() {
        let log = PartitionLog::default();
        let first = batch(&["first", "second"]);
        let second = batch(&["third"]);

        let first_result = log.append(first).await.expect("append first batch");
        let second_result = log.append(second).await.expect("append second batch");

        assert_eq!(first_result.base_offset, 0);
        assert_eq!(first_result.last_offset, 1);
        assert_eq!(first_result.record_count, 2);
        assert_eq!(second_result.base_offset, 2);
        assert_eq!(second_result.last_offset, 2);
        assert_eq!(log.next_offset().await, 3);

        let fetched = log.fetch(0, usize::MAX).await.expect("fetch log");
        assert_eq!(fetched.high_watermark, 3);
        assert_eq!(batch_base_offsets(fetched.records), vec![0, 2]);
    }

    #[tokio::test]
    async fn fetch_inside_a_batch_returns_the_complete_batch() {
        let log = PartitionLog::default();
        log.append(batch(&["zero", "one", "two"]))
            .await
            .expect("append batch");

        let fetched = log.fetch(1, usize::MAX).await.expect("fetch batch");

        assert_eq!(batch_base_offsets(fetched.records), vec![0]);
        assert_eq!(fetched.high_watermark, 3);
    }

    #[tokio::test]
    async fn concurrent_appends_assign_every_offset_once() {
        let log = Arc::new(PartitionLog::default());
        let mut tasks = JoinSet::new();

        for index in 0..64 {
            let log = Arc::clone(&log);
            tasks.spawn(async move {
                log.append(batch(&[&format!("message-{index}")]))
                    .await
                    .expect("append record")
                    .base_offset
            });
        }

        let mut offsets = Vec::new();
        while let Some(result) = tasks.join_next().await {
            offsets.push(result.expect("append task"));
        }
        offsets.sort_unstable();

        assert_eq!(offsets, (0..64).collect::<Vec<_>>());
        assert_eq!(log.next_offset().await, 64);
    }

    #[tokio::test]
    async fn rejects_invalid_batches_without_advancing_the_log() {
        let log = PartitionLog::default();
        let valid = batch(&["valid"]);

        let truncated = valid.slice(..valid.len() - 1);
        assert_eq!(log.append(truncated).await, Err(AppendError::Malformed));

        let mut bad_crc = BytesMut::from(valid.as_ref());
        let last = bad_crc.len() - 1;
        bad_crc[last] ^= 1;
        assert_eq!(
            log.append(bad_crc.freeze()).await,
            Err(AppendError::Malformed)
        );

        let mut legacy = BytesMut::from(valid.as_ref());
        legacy[16] = 1;
        assert_eq!(
            log.append(legacy.freeze()).await,
            Err(AppendError::UnsupportedBatch)
        );

        assert_eq!(
            log.append(special_batch(true, false, NO_PRODUCER_ID)).await,
            Err(AppendError::UnsupportedBatch)
        );
        assert_eq!(
            log.append(special_batch(false, true, NO_PRODUCER_ID)).await,
            Err(AppendError::UnsupportedBatch)
        );

        assert_eq!(log.next_offset().await, 0);
    }

    #[tokio::test]
    async fn idempotent_new_append_assigns_offsets_and_marks_bytes_appended() {
        let log = PartitionLog::default();

        let result = log
            .append(idempotent_batch(7, 0, 0, &["zero", "one"]))
            .await
            .expect("append idempotent batch");

        assert_eq!(result.base_offset, 0);
        assert_eq!(result.last_offset, 1);
        assert_eq!(result.record_count, 2);
        assert!(result.appended);
        assert_eq!(log.next_offset().await, 2);
    }

    #[tokio::test]
    async fn idempotent_exact_retry_replays_original_result_without_appending() {
        let log = PartitionLog::default();
        let records = record_set(&[
            idempotent_batch(7, 0, 0, &["zero", "one"]),
            idempotent_batch(7, 0, 2, &["two"]),
        ]);

        let first = log
            .append(records.clone())
            .await
            .expect("append record set");
        let retry = log.append(records).await.expect("retry record set");

        assert_eq!(retry.base_offset, first.base_offset);
        assert_eq!(retry.last_offset, first.last_offset);
        assert_eq!(retry.record_count, first.record_count);
        assert!(!retry.appended);
        assert_eq!(log.next_offset().await, first.last_offset + 1);
    }

    #[tokio::test]
    async fn idempotent_next_sequence_appends_at_the_next_offset() {
        let log = PartitionLog::default();
        let first = log
            .append(idempotent_batch(7, 0, 0, &["zero", "one"]))
            .await
            .expect("append first batch");

        let second = log
            .append(idempotent_batch(7, 0, 2, &["two"]))
            .await
            .expect("append next sequence");

        assert_eq!(second.base_offset, first.last_offset + 1);
        assert_eq!(second.last_offset, 2);
        assert!(second.appended);
    }

    #[tokio::test]
    async fn idempotent_sequence_gap_is_rejected_without_mutation() {
        let log = PartitionLog::default();
        log.append(idempotent_batch(7, 0, 0, &["zero"]))
            .await
            .expect("append first sequence");
        let next_offset = log.next_offset().await;

        assert_eq!(
            log.append(idempotent_batch(7, 0, 7, &["gap"])).await,
            Err(AppendError::OutOfOrderSequence)
        );
        assert_eq!(log.next_offset().await, next_offset);
    }

    #[tokio::test]
    async fn idempotent_changed_retry_is_rejected_as_duplicate_without_mutation() {
        let log = PartitionLog::default();
        log.append(idempotent_batch(7, 0, 0, &["original"]))
            .await
            .expect("append original");
        let next_offset = log.next_offset().await;

        assert_eq!(
            log.append(idempotent_batch(7, 0, 0, &["changed"])).await,
            Err(AppendError::DuplicateSequence)
        );
        assert_eq!(log.next_offset().await, next_offset);
    }

    #[tokio::test]
    async fn idempotent_discontinuous_record_set_is_rejected_without_partial_append() {
        let log = PartitionLog::default();
        let records = record_set(&[
            idempotent_batch(7, 0, 0, &["zero", "one"]),
            idempotent_batch(7, 0, 3, &["gap"]),
        ]);

        assert_eq!(
            log.append(records).await,
            Err(AppendError::OutOfOrderSequence)
        );
        assert_eq!(log.next_offset().await, 0);
    }

    #[tokio::test]
    async fn idempotent_producer_ids_track_sequences_independently() {
        let log = PartitionLog::default();

        let producer_7_first = log
            .append(idempotent_batch(7, 0, 0, &["seven-zero"]))
            .await
            .expect("append producer 7 sequence 0");
        let producer_8_first = log
            .append(idempotent_batch(8, 0, 0, &["eight-zero"]))
            .await
            .expect("append producer 8 sequence 0");
        let producer_7_second = log
            .append(idempotent_batch(7, 0, 1, &["seven-one"]))
            .await
            .expect("append producer 7 sequence 1");
        let producer_8_second = log
            .append(idempotent_batch(8, 0, 1, &["eight-one"]))
            .await
            .expect("append producer 8 sequence 1");

        assert_eq!(producer_7_first.base_offset, 0);
        assert_eq!(producer_8_first.base_offset, 1);
        assert_eq!(producer_7_second.base_offset, 2);
        assert_eq!(producer_8_second.base_offset, 3);
    }

    #[tokio::test]
    async fn idempotent_partitions_track_sequences_independently() {
        let first_partition = PartitionLog::default();
        let second_partition = PartitionLog::default();

        let first = first_partition
            .append(idempotent_batch(7, 0, 0, &["partition-zero"]))
            .await
            .expect("append first partition");
        let second = second_partition
            .append(idempotent_batch(7, 0, 0, &["partition-one"]))
            .await
            .expect("append second partition");

        assert_eq!(first.base_offset, 0);
        assert_eq!(second.base_offset, 0);
        assert_eq!(first_partition.next_offset().await, 1);
        assert_eq!(second_partition.next_offset().await, 1);
    }

    #[tokio::test]
    async fn idempotent_sixth_request_evicts_the_first_retry() {
        let log = PartitionLog::default();
        let first = idempotent_batch(7, 0, 0, &["sequence-0"]);

        log.append(first.clone()).await.expect("append sequence 0");
        for sequence in 1..=5 {
            log.append(idempotent_batch(
                7,
                0,
                sequence,
                &[&format!("sequence-{sequence}")],
            ))
            .await
            .expect("append subsequent sequence");
        }
        let next_offset = log.next_offset().await;

        assert_eq!(log.append(first).await, Err(AppendError::DuplicateSequence));
        assert_eq!(log.next_offset().await, next_offset);

        let retained_retry = log
            .append(idempotent_batch(7, 0, 1, &["sequence-1"]))
            .await
            .expect("retry oldest retained request");
        assert_eq!(retained_retry.base_offset, 1);
        assert!(!retained_retry.appended);
    }

    #[tokio::test]
    async fn idempotent_sequence_wraps_from_i32_max_to_zero() {
        let log = PartitionLog::default();
        log.inner.lock().await.producer_states.insert(
            7,
            ProducerPartitionState {
                producer_epoch: 0,
                next_sequence: i32::MAX,
                recent: Default::default(),
            },
        );

        let at_max = log
            .append(idempotent_batch(7, 0, i32::MAX, &["maximum"]))
            .await
            .expect("append maximum sequence");
        let at_zero = log
            .append(idempotent_batch(7, 0, 0, &["wrapped"]))
            .await
            .expect("append wrapped sequence");

        assert_eq!(at_max.base_offset, 0);
        assert_eq!(at_zero.base_offset, 1);
        assert_eq!(log.next_offset().await, 2);
    }

    #[tokio::test]
    async fn idempotent_zero_is_a_gap_when_maximum_sequence_is_expected() {
        let log = PartitionLog::default();
        log.inner.lock().await.producer_states.insert(
            7,
            ProducerPartitionState {
                producer_epoch: 0,
                next_sequence: i32::MAX,
                recent: Default::default(),
            },
        );

        assert_eq!(
            log.append(idempotent_batch(7, 0, 0, &["skipped-maximum"]))
                .await,
            Err(AppendError::OutOfOrderSequence)
        );
        assert_eq!(log.next_offset().await, 0);
    }

    #[tokio::test]
    async fn idempotent_changed_maximum_is_stale_after_wrap_to_zero() {
        let log = PartitionLog::default();
        log.inner.lock().await.producer_states.insert(
            7,
            ProducerPartitionState {
                producer_epoch: 0,
                next_sequence: i32::MAX,
                recent: Default::default(),
            },
        );
        log.append(idempotent_batch(7, 0, i32::MAX, &["original"]))
            .await
            .expect("append maximum sequence");
        let next_offset = log.next_offset().await;

        assert_eq!(
            log.append(idempotent_batch(7, 0, i32::MAX, &["changed"]))
                .await,
            Err(AppendError::DuplicateSequence)
        );
        assert_eq!(log.next_offset().await, next_offset);
    }

    #[test]
    fn idempotent_record_set_inspection_reports_one_identity_or_legacy() {
        let idempotent = idempotent_batch(7, 3, 0, &["idempotent"]);
        assert_eq!(
            record_set_producer(&idempotent),
            Ok(Some(RecordSetProducer {
                producer_id: 7,
                producer_epoch: 3,
            }))
        );

        assert_eq!(record_set_producer(&batch(&["legacy"])), Ok(None));
    }

    #[tokio::test]
    async fn idempotent_inspection_preserves_legacy_non_producer_base_sequence() {
        let log = PartitionLog::default();
        let records = legacy_non_idempotent_batch(&["legacy-zero", "legacy-one"]);

        assert_eq!(record_set_producer(&records), Ok(None));
        let result = log.append(records).await.expect("append legacy batch");
        assert_eq!(result.base_offset, 0);
        assert_eq!(result.last_offset, 1);
        assert!(result.appended);
    }

    #[tokio::test]
    async fn idempotent_mixed_record_set_identities_are_malformed_without_mutation() {
        let log = PartitionLog::default();
        let mixed_producers = record_set(&[
            idempotent_batch(7, 0, 0, &["seven"]),
            idempotent_batch(8, 0, 1, &["eight"]),
        ]);
        let mixed_epochs = record_set(&[
            idempotent_batch(7, 0, 0, &["epoch-zero"]),
            idempotent_batch(7, 1, 1, &["epoch-one"]),
        ]);
        let mixed_legacy = record_set(&[
            idempotent_batch(7, 0, 0, &["idempotent"]),
            batch(&["legacy"]),
        ]);

        for records in [mixed_producers, mixed_epochs, mixed_legacy] {
            assert_eq!(record_set_producer(&records), Err(AppendError::Malformed));
            assert_eq!(log.append(records).await, Err(AppendError::Malformed));
            assert_eq!(log.next_offset().await, 0);
        }
    }

    #[tokio::test]
    async fn fetch_enforces_bounds_and_first_batch_progress() {
        let log = PartitionLog::default();
        let encoded = batch(&["a-value-that-is-larger-than-the-limit"]);
        let encoded_len = encoded.len();
        log.append(encoded).await.expect("append batch");
        log.append(batch(&["later"]))
            .await
            .expect("append later batch");

        assert_eq!(log.fetch(-1, usize::MAX).await, Err(FetchError::OutOfRange));
        assert_eq!(log.fetch(3, usize::MAX).await, Err(FetchError::OutOfRange));

        let at_end = log.fetch(2, usize::MAX).await.expect("fetch at end");
        assert!(at_end.records.is_empty());
        assert_eq!(at_end.high_watermark, 2);

        let oversized = log.fetch(0, 1).await.expect("fetch oversized first batch");
        assert_eq!(oversized.records.len(), encoded_len);
        assert_eq!(batch_base_offsets(oversized.records), vec![0]);
    }

    fn batch(values: &[&str]) -> Bytes {
        let records = values
            .iter()
            .enumerate()
            .map(|(offset, value)| record(offset as i64, value, false, false, NO_PRODUCER_ID))
            .collect::<Vec<_>>();
        encode(&records)
    }

    fn idempotent_batch(
        producer_id: i64,
        producer_epoch: i16,
        base_sequence: i32,
        values: &[&str],
    ) -> Bytes {
        let records = values
            .iter()
            .enumerate()
            .map(|(offset, value)| Record {
                producer_epoch,
                sequence: ((i64::from(base_sequence) + offset as i64) % (i64::from(i32::MAX) + 1))
                    as i32,
                ..record(offset as i64, value, false, false, producer_id)
            })
            .collect::<Vec<_>>();
        encode(&records)
    }

    fn legacy_non_idempotent_batch(values: &[&str]) -> Bytes {
        let records = values
            .iter()
            .enumerate()
            .map(|(offset, value)| Record {
                sequence: offset as i32,
                ..record(offset as i64, value, false, false, NO_PRODUCER_ID)
            })
            .collect::<Vec<_>>();
        encode(&records)
    }

    fn record_set(batches: &[Bytes]) -> Bytes {
        let mut records = BytesMut::new();
        for batch in batches {
            records.extend_from_slice(batch);
        }
        records.freeze()
    }

    fn special_batch(transactional: bool, control: bool, producer_id: i64) -> Bytes {
        encode(&[record(0, "special", transactional, control, producer_id)])
    }

    fn record(
        offset: i64,
        value: &str,
        transactional: bool,
        control: bool,
        producer_id: i64,
    ) -> Record {
        Record {
            transactional,
            control,
            delete_horizon: false,
            partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
            producer_id,
            producer_epoch: if producer_id == NO_PRODUCER_ID {
                NO_PRODUCER_EPOCH
            } else {
                0
            },
            timestamp_type: TimestampType::Creation,
            offset,
            sequence: if producer_id == NO_PRODUCER_ID {
                (offset as i32).wrapping_add(NO_SEQUENCE)
            } else {
                offset as i32
            },
            timestamp: offset,
            key: Some(Bytes::from(format!("key-{offset}"))),
            value: Some(Bytes::copy_from_slice(value.as_bytes())),
            headers: [(
                StrBytes::from_static_str("source"),
                Some(Bytes::from_static(b"test")),
            )]
            .into_iter()
            .collect(),
        }
    }

    fn encode(records: &[Record]) -> Bytes {
        let mut encoded = BytesMut::new();
        RecordBatchEncoder::encode(
            &mut encoded,
            records,
            &RecordEncodeOptions {
                version: 2,
                compression: Compression::None,
            },
        )
        .expect("encode records");
        encoded.freeze()
    }

    fn batch_base_offsets(mut records: Bytes) -> Vec<i64> {
        let mut offsets = Vec::new();
        while records.has_remaining() {
            let base_offset = records.get_i64();
            let batch_length = records.get_i32();
            offsets.push(base_offset);
            records.advance(batch_length as usize);
        }
        offsets
    }
}
