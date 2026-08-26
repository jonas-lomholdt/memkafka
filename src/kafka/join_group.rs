use kafka_protocol::{
    ResponseError,
    messages::{JoinGroupRequest, JoinGroupResponse, join_group_response::JoinGroupResponseMember},
    protocol::StrBytes,
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

    let join = JoinRequest {
        group_id: request.group_id.as_str().to_owned(),
        member_id: request.member_id.as_str().to_owned(),
        client_id: client_id.unwrap_or("consumer").to_owned(),
        session_timeout_ms: request.session_timeout_ms,
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
        Ok(JoinResult::Joined(joined)) => {
            if joined.protocol_name == "cooperative-sticky" {
                tracing::info!(
                    group = request.group_id.as_str(),
                    generation = joined.generation_id,
                    protocol = joined.protocol_name,
                    rebalance = "cooperative",
                    members = joined.members.len(),
                    "Using cooperative incremental rebalancing"
                );
            }
            JoinGroupResponse::default()
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
                )
        }
        Err(error) => error_response(response_error(error), request.member_id.clone()),
    }
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
