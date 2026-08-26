use kafka_protocol::ResponseError;

use crate::broker::groups::GroupError;

pub(crate) fn response_error(error: GroupError) -> ResponseError {
    match error {
        GroupError::UnknownMemberId => ResponseError::UnknownMemberId,
        GroupError::IllegalGeneration => ResponseError::IllegalGeneration,
        GroupError::RebalanceInProgress => ResponseError::RebalanceInProgress,
        GroupError::InconsistentGroupProtocol => ResponseError::InconsistentGroupProtocol,
        GroupError::InvalidAssignment => ResponseError::InvalidRequest,
    }
}
