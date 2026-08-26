use std::{error::Error, fmt};

use bytes::{Bytes, BytesMut};
use kafka_protocol::records::{NO_PRODUCER_ID, RecordBatchDecoder};
use tokio::sync::Mutex;

const FIXED_BATCH_SIZE: usize = 61;
const BATCH_LENGTH_OFFSET: usize = 8;
const MAGIC_OFFSET: usize = 16;
const LAST_OFFSET_DELTA_OFFSET: usize = 23;
const RECORD_COUNT_OFFSET: usize = 57;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AppendError {
    Malformed,
    UnsupportedBatch,
    OffsetOverflow,
}

impl fmt::Display for AppendError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Malformed => "malformed record batch",
            Self::UnsupportedBatch => "unsupported record batch",
            Self::OffsetOverflow => "partition offset overflow",
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

        let mut inner = self.inner.lock().await;
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

        inner.batches.extend(stored);
        inner.next_offset = next_offset;

        Ok(AppendResult {
            base_offset,
            last_offset,
            record_count: total_record_count,
        })
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
        if info.transactional || info.control || info.producer_id != NO_PRODUCER_ID {
            return Err(AppendError::UnsupportedBatch);
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
            bytes: batch,
        });
        position += total_length;
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
            Compression, NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH, NO_PRODUCER_ID, Record,
            RecordBatchEncoder, RecordEncodeOptions, TimestampType,
        },
    };
    use tokio::task::JoinSet;

    use super::{AppendError, FetchError, PartitionLog};

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
            log.append(special_batch(false, false, 7)).await,
            Err(AppendError::UnsupportedBatch)
        );

        assert_eq!(log.next_offset().await, 0);
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
            sequence: offset as i32,
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
