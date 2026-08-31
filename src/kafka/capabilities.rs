use kafka_protocol::messages::ApiKey;

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct VersionWindow {
    pub(crate) min: i16,
    pub(crate) max: i16,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ApiCapability {
    pub(crate) api_key: ApiKey,
    pub(crate) name: &'static str,
    pub(crate) supported: VersionWindow,
    pub(crate) kafka_4_3: VersionWindow,
    pub(crate) proof_scenarios: &'static [&'static str],
}

impl ApiCapability {
    pub(crate) fn supports(&self, version: i16) -> bool {
        self.supported.min <= version && version <= self.supported.max
    }
}

pub(crate) static CAPABILITIES: &[ApiCapability] = &[
    ApiCapability {
        api_key: ApiKey::Produce,
        name: "Produce",
        supported: VersionWindow { min: 7, max: 7 },
        kafka_4_3: VersionWindow { min: 3, max: 13 },
        proof_scenarios: &[
            "apache-kafka-java-4.3.1",
            "confluent-kafka-2.15.0",
            "confluent-kafka-flow-2.13.2",
            "franz-go-1.21.6",
            "rskafka-0.6.0",
        ],
    },
    ApiCapability {
        api_key: ApiKey::Fetch,
        name: "Fetch",
        supported: VersionWindow { min: 4, max: 4 },
        kafka_4_3: VersionWindow { min: 4, max: 18 },
        proof_scenarios: &[
            "apache-kafka-java-4.3.1",
            "confluent-kafka-2.15.0",
            "confluent-kafka-flow-2.13.2",
            "franz-go-1.21.6",
            "kafbat-1.5.0",
            "rskafka-0.6.0",
        ],
    },
    ApiCapability {
        api_key: ApiKey::ListOffsets,
        name: "ListOffsets",
        supported: VersionWindow { min: 3, max: 3 },
        kafka_4_3: VersionWindow { min: 1, max: 11 },
        proof_scenarios: &[
            "apache-kafka-java-4.3.1",
            "confluent-kafka-2.15.0",
            "confluent-kafka-flow-2.13.2",
            "franz-go-1.21.6",
            "kafbat-1.5.0",
            "rskafka-0.6.0",
        ],
    },
    ApiCapability {
        api_key: ApiKey::Metadata,
        name: "Metadata",
        supported: VersionWindow { min: 4, max: 9 },
        kafka_4_3: VersionWindow { min: 0, max: 13 },
        proof_scenarios: &[
            "apache-kafka-java-4.3.1",
            "confluent-kafka-2.15.0",
            "confluent-kafka-flow-2.13.2",
            "franz-go-1.21.6",
            "kafbat-1.5.0",
            "rskafka-0.6.0",
        ],
    },
    ApiCapability {
        api_key: ApiKey::OffsetCommit,
        name: "OffsetCommit",
        supported: VersionWindow { min: 7, max: 7 },
        kafka_4_3: VersionWindow { min: 2, max: 10 },
        proof_scenarios: &["confluent-kafka-2.15.0"],
    },
    ApiCapability {
        api_key: ApiKey::OffsetFetch,
        name: "OffsetFetch",
        supported: VersionWindow { min: 5, max: 5 },
        kafka_4_3: VersionWindow { min: 1, max: 10 },
        proof_scenarios: &[
            "confluent-kafka-2.15.0",
            "confluent-kafka-flow-2.13.2",
            "kafbat-1.5.0",
        ],
    },
    ApiCapability {
        api_key: ApiKey::FindCoordinator,
        name: "FindCoordinator",
        supported: VersionWindow { min: 2, max: 2 },
        kafka_4_3: VersionWindow { min: 0, max: 6 },
        proof_scenarios: &[
            "confluent-kafka-2.15.0",
            "confluent-kafka-flow-2.13.2",
            "kafbat-1.5.0",
        ],
    },
    ApiCapability {
        api_key: ApiKey::JoinGroup,
        name: "JoinGroup",
        supported: VersionWindow { min: 5, max: 5 },
        kafka_4_3: VersionWindow { min: 0, max: 9 },
        proof_scenarios: &["confluent-kafka-2.15.0", "confluent-kafka-flow-2.13.2"],
    },
    ApiCapability {
        api_key: ApiKey::Heartbeat,
        name: "Heartbeat",
        supported: VersionWindow { min: 3, max: 3 },
        kafka_4_3: VersionWindow { min: 0, max: 4 },
        proof_scenarios: &["confluent-kafka-2.15.0", "confluent-kafka-flow-2.13.2"],
    },
    ApiCapability {
        api_key: ApiKey::LeaveGroup,
        name: "LeaveGroup",
        supported: VersionWindow { min: 1, max: 3 },
        kafka_4_3: VersionWindow { min: 0, max: 5 },
        proof_scenarios: &["confluent-kafka-2.15.0", "confluent-kafka-flow-2.13.2"],
    },
    ApiCapability {
        api_key: ApiKey::SyncGroup,
        name: "SyncGroup",
        supported: VersionWindow { min: 3, max: 3 },
        kafka_4_3: VersionWindow { min: 0, max: 5 },
        proof_scenarios: &["confluent-kafka-2.15.0", "confluent-kafka-flow-2.13.2"],
    },
    ApiCapability {
        api_key: ApiKey::DescribeGroups,
        name: "DescribeGroups",
        supported: VersionWindow { min: 0, max: 0 },
        kafka_4_3: VersionWindow { min: 0, max: 6 },
        proof_scenarios: &["kafbat-1.5.0"],
    },
    ApiCapability {
        api_key: ApiKey::ListGroups,
        name: "ListGroups",
        supported: VersionWindow { min: 0, max: 0 },
        kafka_4_3: VersionWindow { min: 0, max: 5 },
        proof_scenarios: &["kafbat-1.5.0"],
    },
    ApiCapability {
        api_key: ApiKey::ApiVersions,
        name: "ApiVersions",
        supported: VersionWindow { min: 3, max: 4 },
        kafka_4_3: VersionWindow { min: 0, max: 4 },
        proof_scenarios: &[
            "apache-kafka-java-4.3.1",
            "confluent-kafka-2.15.0",
            "confluent-kafka-flow-2.13.2",
            "franz-go-1.21.6",
            "kafbat-1.5.0",
            "rskafka-0.6.0",
        ],
    },
    ApiCapability {
        api_key: ApiKey::CreateTopics,
        name: "CreateTopics",
        supported: VersionWindow { min: 4, max: 6 },
        kafka_4_3: VersionWindow { min: 2, max: 7 },
        proof_scenarios: &[
            "apache-kafka-java-4.3.1",
            "confluent-kafka-2.15.0",
            "confluent-kafka-flow-2.13.2",
            "franz-go-1.21.6",
            "rskafka-0.6.0",
        ],
    },
    ApiCapability {
        api_key: ApiKey::InitProducerId,
        name: "InitProducerId",
        supported: VersionWindow { min: 0, max: 0 },
        kafka_4_3: VersionWindow { min: 0, max: 6 },
        proof_scenarios: &["apache-kafka-java-4.3.1", "confluent-kafka-flow-2.13.2"],
    },
    ApiCapability {
        api_key: ApiKey::DescribeConfigs,
        name: "DescribeConfigs",
        supported: VersionWindow { min: 1, max: 1 },
        kafka_4_3: VersionWindow { min: 1, max: 4 },
        proof_scenarios: &["kafbat-1.5.0"],
    },
];

pub(crate) fn capability(api_key: ApiKey) -> Option<&'static ApiCapability> {
    CAPABILITIES
        .binary_search_by_key(&(api_key as i16), |capability| capability.api_key as i16)
        .ok()
        .map(|index| &CAPABILITIES[index])
}

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct CapabilityManifest<'a> {
    schema_version: u8,
    kafka_baseline: &'static str,
    apis: Vec<ManifestApi<'a>>,
}

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct ManifestApi<'a> {
    api_key: i16,
    name: &'a str,
    supported: VersionWindow,
    #[serde(rename = "kafka43")]
    kafka_4_3: VersionWindow,
    proof_scenarios: &'a [&'a str],
}

pub fn manifest_json() -> Result<String, serde_json::Error> {
    let manifest = CapabilityManifest {
        schema_version: 1,
        kafka_baseline: "4.3",
        apis: CAPABILITIES
            .iter()
            .map(|capability| ManifestApi {
                api_key: capability.api_key as i16,
                name: capability.name,
                supported: capability.supported,
                kafka_4_3: capability.kafka_4_3,
                proof_scenarios: capability.proof_scenarios,
            })
            .collect(),
    };
    let mut json = serde_json::to_string_pretty(&manifest)?;
    json.push('\n');
    Ok(json)
}

#[cfg(test)]
mod tests {
    use super::{ApiCapability, ApiKey, CAPABILITIES, VersionWindow, manifest_json};
    use crate::kafka::api_versions;

    #[test]
    fn registry_is_sorted_unique_and_has_nonempty_contained_windows() {
        assert_eq!(CAPABILITIES.len(), 17);
        assert!(
            CAPABILITIES
                .windows(2)
                .all(|pair| { (pair[0].api_key as i16) < pair[1].api_key as i16 })
        );
        assert!(
            CAPABILITIES
                .iter()
                .all(|capability| capability.supported.min <= capability.supported.max)
        );
        assert!(CAPABILITIES.iter().all(|capability| {
            capability.kafka_4_3.min <= capability.supported.min
                && capability.supported.max <= capability.kafka_4_3.max
        }));
    }

    #[test]
    fn registry_has_exact_kafka_targets_and_sorted_proof_scenarios() {
        let actual = CAPABILITIES
            .iter()
            .map(|capability| {
                (
                    capability.api_key,
                    capability.name,
                    (capability.kafka_4_3.min, capability.kafka_4_3.max),
                    capability.proof_scenarios,
                )
            })
            .collect::<Vec<_>>();

        assert_eq!(
            actual,
            vec![
                (
                    ApiKey::Produce,
                    "Produce",
                    (3, 13),
                    &[
                        "apache-kafka-java-4.3.1",
                        "confluent-kafka-2.15.0",
                        "confluent-kafka-flow-2.13.2",
                        "franz-go-1.21.6",
                        "rskafka-0.6.0",
                    ][..]
                ),
                (
                    ApiKey::Fetch,
                    "Fetch",
                    (4, 18),
                    &[
                        "apache-kafka-java-4.3.1",
                        "confluent-kafka-2.15.0",
                        "confluent-kafka-flow-2.13.2",
                        "franz-go-1.21.6",
                        "kafbat-1.5.0",
                        "rskafka-0.6.0",
                    ][..]
                ),
                (
                    ApiKey::ListOffsets,
                    "ListOffsets",
                    (1, 11),
                    &[
                        "apache-kafka-java-4.3.1",
                        "confluent-kafka-2.15.0",
                        "confluent-kafka-flow-2.13.2",
                        "franz-go-1.21.6",
                        "kafbat-1.5.0",
                        "rskafka-0.6.0",
                    ][..]
                ),
                (
                    ApiKey::Metadata,
                    "Metadata",
                    (0, 13),
                    &[
                        "apache-kafka-java-4.3.1",
                        "confluent-kafka-2.15.0",
                        "confluent-kafka-flow-2.13.2",
                        "franz-go-1.21.6",
                        "kafbat-1.5.0",
                        "rskafka-0.6.0",
                    ][..]
                ),
                (
                    ApiKey::OffsetCommit,
                    "OffsetCommit",
                    (2, 10),
                    &["confluent-kafka-2.15.0",][..]
                ),
                (
                    ApiKey::OffsetFetch,
                    "OffsetFetch",
                    (1, 10),
                    &[
                        "confluent-kafka-2.15.0",
                        "confluent-kafka-flow-2.13.2",
                        "kafbat-1.5.0",
                    ][..]
                ),
                (
                    ApiKey::FindCoordinator,
                    "FindCoordinator",
                    (0, 6),
                    &[
                        "confluent-kafka-2.15.0",
                        "confluent-kafka-flow-2.13.2",
                        "kafbat-1.5.0",
                    ][..]
                ),
                (
                    ApiKey::JoinGroup,
                    "JoinGroup",
                    (0, 9),
                    &["confluent-kafka-2.15.0", "confluent-kafka-flow-2.13.2",][..]
                ),
                (
                    ApiKey::Heartbeat,
                    "Heartbeat",
                    (0, 4),
                    &["confluent-kafka-2.15.0", "confluent-kafka-flow-2.13.2",][..]
                ),
                (
                    ApiKey::LeaveGroup,
                    "LeaveGroup",
                    (0, 5),
                    &["confluent-kafka-2.15.0", "confluent-kafka-flow-2.13.2",][..]
                ),
                (
                    ApiKey::SyncGroup,
                    "SyncGroup",
                    (0, 5),
                    &["confluent-kafka-2.15.0", "confluent-kafka-flow-2.13.2",][..]
                ),
                (
                    ApiKey::DescribeGroups,
                    "DescribeGroups",
                    (0, 6),
                    &["kafbat-1.5.0",][..]
                ),
                (
                    ApiKey::ListGroups,
                    "ListGroups",
                    (0, 5),
                    &["kafbat-1.5.0",][..]
                ),
                (
                    ApiKey::ApiVersions,
                    "ApiVersions",
                    (0, 4),
                    &[
                        "apache-kafka-java-4.3.1",
                        "confluent-kafka-2.15.0",
                        "confluent-kafka-flow-2.13.2",
                        "franz-go-1.21.6",
                        "kafbat-1.5.0",
                        "rskafka-0.6.0",
                    ][..]
                ),
                (
                    ApiKey::CreateTopics,
                    "CreateTopics",
                    (2, 7),
                    &[
                        "apache-kafka-java-4.3.1",
                        "confluent-kafka-2.15.0",
                        "confluent-kafka-flow-2.13.2",
                        "franz-go-1.21.6",
                        "rskafka-0.6.0",
                    ][..]
                ),
                (
                    ApiKey::InitProducerId,
                    "InitProducerId",
                    (0, 6),
                    &["apache-kafka-java-4.3.1", "confluent-kafka-flow-2.13.2",][..]
                ),
                (
                    ApiKey::DescribeConfigs,
                    "DescribeConfigs",
                    (1, 4),
                    &["kafbat-1.5.0",][..]
                ),
            ]
        );
    }

    #[test]
    fn kafka_4_3_ranges_match_generated_schema_and_supported_windows_are_frozen() {
        for capability in CAPABILITIES {
            let generated = capability.api_key.valid_versions();
            assert_eq!(
                (generated.min, generated.max),
                (capability.kafka_4_3.min, capability.kafka_4_3.max),
                "generated Kafka 4.3 range drifted for {:?}",
                capability.api_key,
            );
        }

        let actual_supported = CAPABILITIES
            .iter()
            .map(|capability| {
                (
                    capability.api_key,
                    capability.supported.min,
                    capability.supported.max,
                )
            })
            .collect::<Vec<_>>();

        assert_eq!(
            actual_supported,
            vec![
                (ApiKey::Produce, 7, 7),
                (ApiKey::Fetch, 4, 4),
                (ApiKey::ListOffsets, 3, 3),
                (ApiKey::Metadata, 4, 9),
                (ApiKey::OffsetCommit, 7, 7),
                (ApiKey::OffsetFetch, 5, 5),
                (ApiKey::FindCoordinator, 2, 2),
                (ApiKey::JoinGroup, 5, 5),
                (ApiKey::Heartbeat, 3, 3),
                (ApiKey::LeaveGroup, 1, 3),
                (ApiKey::SyncGroup, 3, 3),
                (ApiKey::DescribeGroups, 0, 0),
                (ApiKey::ListGroups, 0, 0),
                (ApiKey::ApiVersions, 3, 4),
                (ApiKey::CreateTopics, 4, 6),
                (ApiKey::InitProducerId, 0, 0),
                (ApiKey::DescribeConfigs, 1, 1),
            ]
        );
    }

    #[test]
    fn api_versions_response_equals_registry_supported_ranges() {
        let actual = api_versions::response()
            .api_keys
            .into_iter()
            .map(|api| (api.api_key, api.min_version, api.max_version))
            .collect::<Vec<_>>();
        let expected = CAPABILITIES
            .iter()
            .map(|capability| {
                (
                    capability.api_key as i16,
                    capability.supported.min,
                    capability.supported.max,
                )
            })
            .collect::<Vec<_>>();

        assert_eq!(actual, expected);
    }

    #[test]
    fn supported_boundaries_are_accepted_and_adjacent_versions_are_rejected() {
        for capability in CAPABILITIES {
            assert_supported_boundaries_and_representable_neighbors(capability);
        }
    }

    #[test]
    fn boundary_assertions_handle_i16_extreme_windows() {
        for supported in [
            VersionWindow {
                min: i16::MIN,
                max: 0,
            },
            VersionWindow {
                min: 0,
                max: i16::MAX,
            },
        ] {
            assert_supported_boundaries_and_representable_neighbors(&ApiCapability {
                api_key: ApiKey::Produce,
                name: "extreme-test-window",
                supported,
                kafka_4_3: supported,
                proof_scenarios: &[],
            });
        }
    }

    fn assert_supported_boundaries_and_representable_neighbors(capability: &ApiCapability) {
        assert!(capability.supports(capability.supported.min));
        assert!(capability.supports(capability.supported.max));
        if let Some(version) = capability.supported.min.checked_sub(1) {
            assert!(!capability.supports(version));
        }
        if let Some(version) = capability.supported.max.checked_add(1) {
            assert!(!capability.supports(version));
        }
    }

    #[test]
    fn manifest_is_deterministic_complete_and_has_a_final_newline() {
        let first = manifest_json().expect("render capability manifest");
        let second = manifest_json().expect("render capability manifest again");
        let document: serde_json::Value =
            serde_json::from_str(&first).expect("parse manifest JSON");

        assert_eq!(first, second);
        assert!(first.ends_with('\n'));
        assert_eq!(document["schemaVersion"], 1);
        assert_eq!(document["kafkaBaseline"], "4.3");
        assert_eq!(document["apis"].as_array().map(Vec::len), Some(17));
        assert_eq!(document["apis"][0]["apiKey"], 0);
        assert_eq!(document["apis"][0]["name"], "Produce");
        assert_eq!(document["apis"][0]["supported"]["min"], 7);
        assert_eq!(document["apis"][0]["supported"]["max"], 7);
        assert_eq!(document["apis"][0]["kafka43"]["min"], 3);
        assert_eq!(document["apis"][0]["kafka43"]["max"], 13);
        assert_eq!(
            document["apis"][0]["proofScenarios"],
            serde_json::json!([
                "apache-kafka-java-4.3.1",
                "confluent-kafka-2.15.0",
                "confluent-kafka-flow-2.13.2",
                "franz-go-1.21.6",
                "rskafka-0.6.0"
            ])
        );
    }
}
