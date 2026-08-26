use bytes::{Buf, Bytes};
use kafka_protocol::{
    ResponseError,
    messages::{
        ConsumerProtocolSubscription, JoinGroupRequest, JoinGroupResponse,
        join_group_response::JoinGroupResponseMember,
    },
    protocol::{Decodable, StrBytes},
};

use crate::broker::{
    BrokerState,
    groups::{JoinProtocol, JoinRequest, JoinResult},
};

use super::group_error::response_error;

pub(crate) const VERSION_RANGE: std::ops::RangeInclusive<i16> = 0..=5;

pub(crate) async fn response(
    request: &JoinGroupRequest,
    version: i16,
    client_id: Option<&str>,
    broker: &BrokerState,
) -> JoinGroupResponse {
    if request.group_instance_id.is_some() {
        return error_response(ResponseError::UnsupportedVersion, request.member_id.clone());
    }

    let advertised_protocols = request
        .protocols
        .iter()
        .map(|protocol| protocol.name.as_str())
        .collect::<Vec<_>>();
    let owned_partitions = request
        .protocols
        .iter()
        .find(|protocol| protocol.name.as_str() == "cooperative-sticky")
        .map_or_else(Vec::new, |protocol| {
            decode_owned_partitions(&protocol.metadata)
        });
    tracing::debug!(
        group = request.group_id.as_str(),
        member = request.member_id.as_str(),
        ?advertised_protocols,
        ?owned_partitions,
        "consumer group member advertised assignment protocols"
    );

    let join = JoinRequest {
        group_id: request.group_id.as_str().to_owned(),
        member_id: request.member_id.as_str().to_owned(),
        client_id: client_id.unwrap_or("consumer").to_owned(),
        session_timeout_ms: request.session_timeout_ms,
        rebalance_timeout_ms: effective_rebalance_timeout_ms(
            version,
            request.session_timeout_ms,
            request.rebalance_timeout_ms,
        ),
        protocol_type: request.protocol_type.as_str().to_owned(),
        protocols: request
            .protocols
            .iter()
            .map(|protocol| JoinProtocol {
                name: protocol.name.as_str().to_owned(),
                metadata: protocol.metadata.clone(),
            })
            .collect(),
    };

    match broker.groups().join(join, version >= 4).await {
        Ok(JoinResult::MemberIdRequired { member_id }) => error_response(
            ResponseError::MemberIdRequired,
            StrBytes::from_string(member_id),
        ),
        Ok(JoinResult::Joined(joined)) => JoinGroupResponse::default()
            .with_throttle_time_ms(0)
            .with_error_code(0)
            .with_generation_id(joined.generation_id)
            .with_protocol_name(Some(StrBytes::from_string(joined.protocol_name)))
            .with_leader(StrBytes::from_string(joined.leader))
            .with_member_id(StrBytes::from_string(joined.member_id))
            .with_members(
                joined
                    .members
                    .into_iter()
                    .map(|member| {
                        JoinGroupResponseMember::default()
                            .with_member_id(StrBytes::from_string(member.member_id))
                            .with_group_instance_id(None)
                            .with_metadata(member.metadata)
                    })
                    .collect(),
            ),
        Err(error) => error_response(response_error(error), request.member_id.clone()),
    }
}

fn effective_rebalance_timeout_ms(
    version: i16,
    session_timeout_ms: i32,
    rebalance_timeout_ms: i32,
) -> i32 {
    if version == 0 {
        session_timeout_ms
    } else {
        rebalance_timeout_ms
    }
}

fn decode_owned_partitions(metadata: &Bytes) -> Vec<String> {
    let mut metadata = metadata.clone();
    if metadata.remaining() < 2 {
        return Vec::new();
    }
    let version = metadata.get_i16();
    let Ok(subscription) = ConsumerProtocolSubscription::decode(&mut metadata, version) else {
        return Vec::new();
    };
    let mut owned = subscription
        .owned_partitions
        .into_iter()
        .flat_map(|topic| {
            topic
                .partitions
                .into_iter()
                .map(move |partition| format!("{}[{partition}]", topic.topic.as_str()))
        })
        .collect::<Vec<_>>();
    owned.sort();
    owned
}

fn error_response(error: ResponseError, member_id: StrBytes) -> JoinGroupResponse {
    JoinGroupResponse::default()
        .with_throttle_time_ms(0)
        .with_error_code(error.code())
        .with_generation_id(-1)
        .with_protocol_name(None)
        .with_leader(StrBytes::default())
        .with_member_id(member_id)
        .with_members(Vec::new())
}

#[cfg(test)]
mod tests {
    use bytes::{BufMut, BytesMut};
    use kafka_protocol::{
        messages::{
            ConsumerProtocolSubscription, TopicName, consumer_protocol_subscription::TopicPartition,
        },
        protocol::{Encodable, StrBytes},
    };

    use super::{decode_owned_partitions, effective_rebalance_timeout_ms};

    #[test]
    fn join_v0_uses_session_timeout_as_the_rebalance_timeout() {
        assert_eq!(effective_rebalance_timeout_ms(0, 10_000, -1), 10_000);
        assert_eq!(effective_rebalance_timeout_ms(1, 10_000, 30_000), 30_000);
    }

    #[test]
    fn decodes_owned_partitions_for_cooperative_debug_logging() {
        let subscription = ConsumerProtocolSubscription::default().with_owned_partitions(vec![
            TopicPartition::default()
                .with_topic(TopicName::from(StrBytes::from_static_str("events")))
                .with_partitions(vec![2, 0]),
        ]);
        let mut metadata = BytesMut::new();
        metadata.put_i16(1);
        subscription.encode(&mut metadata, 1).unwrap();

        assert_eq!(
            decode_owned_partitions(&metadata.freeze()),
            vec!["events[0]", "events[2]"]
        );
    }
}
