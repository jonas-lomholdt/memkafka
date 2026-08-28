pub const MIN_PAYLOAD_BYTES: usize = 100;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkloadConfig {
    pub messages: u64,
    pub payload_bytes: usize,
    pub partitions: i32,
    pub batch_records: usize,
}

impl Default for WorkloadConfig {
    fn default() -> Self {
        Self {
            messages: 1_000_000,
            payload_bytes: 4096,
            partitions: 8,
            batch_records: 256,
        }
    }
}

impl WorkloadConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.messages == 0 {
            anyhow::bail!("messages must be greater than zero");
        }
        if self.payload_bytes < MIN_PAYLOAD_BYTES {
            anyhow::bail!(
                "payload_bytes must be at least {MIN_PAYLOAD_BYTES}, got {}",
                self.payload_bytes
            );
        }
        if self.partitions <= 0 {
            anyhow::bail!("partitions must be greater than zero");
        }
        if self.batch_records == 0 {
            anyhow::bail!("batch_records must be greater than zero");
        }
        if self.messages < self.partitions as u64 {
            anyhow::bail!(
                "messages ({}) must be at least partitions ({})",
                self.messages,
                self.partitions
            );
        }

        Ok(())
    }

    pub fn records_in_partition(&self, partition: i32) -> u64 {
        let base = self.messages / self.partitions as u64;
        let remainder = self.messages % self.partitions as u64;
        base + u64::from((partition as u64) < remainder)
    }
}

#[cfg(test)]
mod tests {
    use super::{MIN_PAYLOAD_BYTES, WorkloadConfig};

    #[test]
    fn defaults_match_the_standard_workload() {
        assert_eq!(
            WorkloadConfig::default(),
            WorkloadConfig {
                messages: 1_000_000,
                payload_bytes: 4096,
                partitions: 8,
                batch_records: 256,
            }
        );
    }

    #[test]
    fn rejects_zero_values() {
        for config in [
            WorkloadConfig {
                messages: 0,
                ..WorkloadConfig::default()
            },
            WorkloadConfig {
                payload_bytes: 0,
                ..WorkloadConfig::default()
            },
            WorkloadConfig {
                partitions: 0,
                ..WorkloadConfig::default()
            },
            WorkloadConfig {
                batch_records: 0,
                ..WorkloadConfig::default()
            },
        ] {
            assert!(config.validate().is_err());
        }
    }

    #[test]
    fn rejects_negative_or_excessive_partition_counts() {
        for config in [
            WorkloadConfig {
                partitions: -1,
                ..WorkloadConfig::default()
            },
            WorkloadConfig {
                messages: 7,
                partitions: 8,
                ..WorkloadConfig::default()
            },
        ] {
            assert!(config.validate().is_err());
        }
    }

    #[test]
    fn distributes_remainder_to_lowest_partition_numbers() {
        let config = WorkloadConfig {
            messages: 10,
            partitions: 3,
            ..WorkloadConfig::default()
        };

        assert_eq!(config.records_in_partition(0), 4);
        assert_eq!(config.records_in_partition(1), 3);
        assert_eq!(config.records_in_partition(2), 3);
    }

    #[test]
    fn rejects_payloads_smaller_than_the_fixed_json_envelope() {
        let config = WorkloadConfig {
            payload_bytes: MIN_PAYLOAD_BYTES - 1,
            ..WorkloadConfig::default()
        };

        assert!(config.validate().is_err());
    }
}
