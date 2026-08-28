use std::collections::BTreeMap;

use anyhow::{Context, Result, bail};
use chrono::{DateTime, TimeZone, Utc};
use rskafka::record::{Record, RecordAndOffset};
use serde::Serialize;

use crate::config::MIN_PAYLOAD_BYTES;

const FIXED_TIMESTAMP: &str = "2026-08-28T00:00:00Z";

#[derive(Serialize)]
struct EventEnvelope<'a> {
    partition: &'a str,
    sequence: &'a str,
    timestamp: &'a str,
    padding: String,
}

pub fn record(partition: i32, sequence: u64, payload_bytes: usize) -> Result<Record> {
    if payload_bytes < MIN_PAYLOAD_BYTES {
        bail!("payload_bytes must be at least {MIN_PAYLOAD_BYTES}, got {payload_bytes}");
    }

    let partition_identity = format!("{partition:02}");
    let sequence_identity = format!("{sequence:020}");
    let value = serde_json::to_vec(&EventEnvelope {
        partition: &partition_identity,
        sequence: &sequence_identity,
        timestamp: FIXED_TIMESTAMP,
        padding: " ".repeat(payload_bytes - MIN_PAYLOAD_BYTES),
    })
    .context("serialize benchmark event")?;

    debug_assert_eq!(value.len(), payload_bytes);

    Ok(Record {
        key: Some(format!("p{partition_identity}-s{sequence_identity}").into_bytes()),
        value: Some(value),
        headers: BTreeMap::from([
            ("content-type".to_owned(), b"application/json".to_vec()),
            ("event-type".to_owned(), b"EquipmentMoved".to_vec()),
        ]),
        timestamp: fixed_timestamp(),
    })
}

pub fn validate(
    record: &RecordAndOffset,
    partition: i32,
    sequence: u64,
    payload_bytes: usize,
) -> Result<()> {
    let expected_offset = i64::try_from(sequence).context("sequence does not fit in an offset")?;
    if record.offset != expected_offset {
        bail!(
            "offset mismatch: expected {expected_offset}, got {}",
            record.offset
        );
    }

    let partition_identity = format!("{partition:02}");
    let sequence_identity = format!("{sequence:020}");
    let expected_key = format!("p{partition_identity}-s{sequence_identity}");
    if record.record.key.as_deref() != Some(expected_key.as_bytes()) {
        bail!("key mismatch");
    }

    let value = record.record.value.as_deref().context("value is missing")?;
    if value.len() != payload_bytes {
        bail!(
            "value length mismatch: expected {payload_bytes}, got {}",
            value.len()
        );
    }
    if value.first() != Some(&b'{') || value.last() != Some(&b'}') {
        bail!("value is not a JSON object");
    }

    let identity =
        format!(r#"{{"partition":"{partition_identity}","sequence":"{sequence_identity}","#);
    if !value.starts_with(identity.as_bytes()) {
        bail!("value identity mismatch");
    }

    Ok(())
}

fn fixed_timestamp() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 8, 28, 0, 0, 0)
        .single()
        .expect("fixed timestamp is valid")
}

#[cfg(test)]
mod tests {
    use rskafka::record::RecordAndOffset;

    use super::{record, validate};

    #[test]
    fn creates_a_fixed_size_json_record_with_a_deterministic_key() {
        let record = record(3, 42, 4096).unwrap();

        assert_eq!(record.value.as_ref().unwrap().len(), 4096);
        assert_eq!(
            record.key.as_deref(),
            Some(b"p03-s00000000000000000042".as_slice())
        );
        serde_json::from_slice::<serde_json::Value>(record.value.as_ref().unwrap()).unwrap();
        assert_eq!(
            record.headers.get("content-type").map(Vec::as_slice),
            Some(b"application/json".as_slice())
        );
        assert_eq!(
            record.headers.get("event-type").map(Vec::as_slice),
            Some(b"EquipmentMoved".as_slice())
        );
        assert_eq!(record.timestamp.to_rfc3339(), "2026-08-28T00:00:00+00:00");
    }

    #[test]
    fn validates_the_generated_record_at_its_sequence_offset() {
        let record = record(3, 42, 4096).unwrap();
        let record_and_offset = RecordAndOffset { record, offset: 42 };

        validate(&record_and_offset, 3, 42, 4096).unwrap();
    }

    #[test]
    fn rejects_a_mismatched_key() {
        let mut record = record(3, 42, 4096).unwrap();
        record.key = Some(b"wrong-key".to_vec());
        let record_and_offset = RecordAndOffset { record, offset: 42 };

        let error = validate(&record_and_offset, 3, 42, 4096).unwrap_err();

        assert!(error.to_string().contains("key"));
    }

    #[test]
    fn rejects_a_truncated_value() {
        let mut record = record(3, 42, 4096).unwrap();
        record.value.as_mut().unwrap().pop();
        let record_and_offset = RecordAndOffset { record, offset: 42 };

        let error = validate(&record_and_offset, 3, 42, 4096).unwrap_err();

        assert!(error.to_string().contains("value length"));
    }

    #[test]
    fn rejects_a_mismatched_offset() {
        let record = record(3, 42, 4096).unwrap();
        let record_and_offset = RecordAndOffset { record, offset: 43 };

        let error = validate(&record_and_offset, 3, 42, 4096).unwrap_err();

        assert!(error.to_string().contains("offset"));
    }
}
