use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use bytes::Bytes;
use tokio::{
    sync::{Mutex, RwLock},
    time::Instant,
};

#[derive(Clone, Debug)]
pub(crate) struct GroupCoordinator {
    inner: Arc<CoordinatorInner>,
}

#[derive(Debug)]
struct CoordinatorInner {
    groups: RwLock<HashMap<String, Arc<Mutex<Group>>>>,
    pending_member_ids: Mutex<HashSet<(String, String)>>,
    next_member_id: AtomicU64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GroupState {
    Empty,
    PreparingRebalance,
    CompletingRebalance,
    Stable,
}

#[derive(Debug)]
struct Group {
    state: GroupState,
    generation_id: i32,
    protocol_type: String,
    selected_protocol: String,
    leader_member_id: String,
    members: HashMap<String, Member>,
    assignments: HashMap<String, Bytes>,
    committed_offsets: HashMap<TopicPartition, StoredOffset>,
}

#[derive(Clone, Debug)]
struct Member {
    client_id: String,
    session_timeout_ms: i32,
    last_heartbeat: Instant,
    protocols: Vec<JoinProtocol>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct JoinProtocol {
    pub(crate) name: String,
    pub(crate) metadata: Bytes,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct JoinRequest {
    pub(crate) group_id: String,
    pub(crate) member_id: String,
    pub(crate) client_id: String,
    pub(crate) session_timeout_ms: i32,
    pub(crate) protocol_type: String,
    pub(crate) protocols: Vec<JoinProtocol>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum JoinResult {
    MemberIdRequired { member_id: String },
    Joined(JoinedGroup),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct JoinedGroup {
    pub(crate) generation_id: i32,
    pub(crate) protocol_type: String,
    pub(crate) protocol_name: String,
    pub(crate) leader: String,
    pub(crate) member_id: String,
    pub(crate) members: Vec<JoinedMember>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct JoinedMember {
    pub(crate) member_id: String,
    pub(crate) metadata: Bytes,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct GroupSummary {
    pub(crate) group_id: String,
    pub(crate) protocol_type: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SyncAssignment {
    pub(crate) member_id: String,
    pub(crate) assignment: Bytes,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct TopicPartition {
    pub(crate) topic: String,
    pub(crate) partition: i32,
}

impl TopicPartition {
    pub(crate) fn new(topic: impl Into<String>, partition: i32) -> Self {
        Self {
            topic: topic.into(),
            partition,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct OffsetCommit {
    pub(crate) topic: String,
    pub(crate) partition: i32,
    pub(crate) offset: i64,
    pub(crate) metadata: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct FetchedOffset {
    pub(crate) topic: String,
    pub(crate) partition: i32,
    pub(crate) offset: Option<i64>,
    pub(crate) metadata: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct StoredOffset {
    offset: i64,
    metadata: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GroupError {
    UnknownMemberId,
    IllegalGeneration,
    RebalanceInProgress,
    InconsistentGroupProtocol,
    InvalidAssignment,
}

impl GroupCoordinator {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(CoordinatorInner {
                groups: RwLock::new(HashMap::new()),
                pending_member_ids: Mutex::new(HashSet::new()),
                next_member_id: AtomicU64::new(1),
            }),
        }
    }

    pub(crate) async fn list(&self) -> Vec<GroupSummary> {
        let mut groups = self
            .inner
            .groups
            .read()
            .await
            .iter()
            .map(|(group_id, group)| (group_id.clone(), Arc::clone(group)))
            .collect::<Vec<_>>();
        groups.sort_unstable_by(|left, right| left.0.cmp(&right.0));

        let mut summaries = Vec::with_capacity(groups.len());
        for (group_id, group) in groups {
            let group = group.lock().await;
            summaries.push(GroupSummary {
                group_id,
                protocol_type: group.protocol_type.clone(),
            });
        }
        summaries
    }

    pub(crate) async fn join(
        &self,
        mut request: JoinRequest,
        require_known_member_id: bool,
    ) -> Result<JoinResult, GroupError> {
        if request.protocols.is_empty() || request.protocol_type.is_empty() {
            return Err(GroupError::InconsistentGroupProtocol);
        }

        if request.member_id.is_empty() {
            request.member_id = self.allocate_member_id(&request.client_id);
            if require_known_member_id {
                self.inner
                    .pending_member_ids
                    .lock()
                    .await
                    .insert((request.group_id, request.member_id.clone()));
                return Ok(JoinResult::MemberIdRequired {
                    member_id: request.member_id,
                });
            }
        } else if !self.member_is_known_or_pending(&request).await {
            return Err(GroupError::UnknownMemberId);
        }

        let group = self.group_or_insert(&request.group_id).await;
        let mut group = group.lock().await;
        if !group.members.is_empty() && group.protocol_type != request.protocol_type {
            tracing::warn!(
                group = request.group_id.as_str(),
                member = request.member_id.as_str(),
                requested_protocol_type = request.protocol_type.as_str(),
                group_protocol_type = group.protocol_type.as_str(),
                "consumer group protocol type is inconsistent"
            );
            return Err(GroupError::InconsistentGroupProtocol);
        }

        let member_id = request.member_id;
        let advertised_protocols = request
            .protocols
            .iter()
            .map(|protocol| protocol.name.clone())
            .collect::<Vec<_>>();
        tracing::debug!(
            group = request.group_id.as_str(),
            member = member_id.as_str(),
            ?advertised_protocols,
            "consumer group member advertised assignment protocols"
        );
        let member = Member {
            client_id: request.client_id,
            session_timeout_ms: request.session_timeout_ms,
            last_heartbeat: Instant::now(),
            protocols: request.protocols,
        };
        let mut prospective_members = group.members.clone();
        prospective_members.insert(member_id.clone(), member);
        let selected_protocol = match select_protocol(&prospective_members) {
            Ok(protocol) => protocol,
            Err(error) => {
                tracing::warn!(
                    group = request.group_id.as_str(),
                    member = member_id.as_str(),
                    ?advertised_protocols,
                    "consumer group members have no common assignment protocol"
                );
                return Err(error);
            }
        };

        self.inner
            .pending_member_ids
            .lock()
            .await
            .remove(&(request.group_id.clone(), member_id.clone()));
        group.state = GroupState::PreparingRebalance;
        group.members = prospective_members;
        group.selected_protocol = selected_protocol;
        group.protocol_type = request.protocol_type;
        if group.leader_member_id.is_empty() {
            group.leader_member_id.clone_from(&member_id);
        }
        group.generation_id = group.generation_id.saturating_add(1);
        group.assignments.clear();
        group.state = GroupState::CompletingRebalance;

        tracing::info!(
            group = request.group_id.as_str(),
            member = member_id.as_str(),
            generation = group.generation_id,
            members = group.members.len(),
            "consumer group member joined"
        );
        tracing::info!(
            group = request.group_id.as_str(),
            generation = group.generation_id,
            members = group.members.len(),
            "consumer group rebalance started"
        );
        tracing::info!(
            group = request.group_id.as_str(),
            generation = group.generation_id,
            protocol = group.selected_protocol.as_str(),
            "consumer group assignment protocol selected"
        );

        let members = if group.leader_member_id == member_id {
            members_for_leader(&group)
        } else {
            Vec::new()
        };
        Ok(JoinResult::Joined(JoinedGroup {
            generation_id: group.generation_id,
            protocol_type: group.protocol_type.clone(),
            protocol_name: group.selected_protocol.clone(),
            leader: group.leader_member_id.clone(),
            member_id,
            members,
        }))
    }

    pub(crate) async fn sync(
        &self,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        assignments: Vec<SyncAssignment>,
    ) -> Result<Bytes, GroupError> {
        let group = self
            .group(group_id)
            .await
            .ok_or(GroupError::UnknownMemberId)?;
        let mut group = group.lock().await;
        validate_member_and_generation(&group, generation_id, member_id)?;

        if group.state == GroupState::Stable {
            return group
                .assignments
                .get(member_id)
                .cloned()
                .ok_or(GroupError::RebalanceInProgress);
        }
        if group.state != GroupState::CompletingRebalance {
            return Err(GroupError::RebalanceInProgress);
        }

        if member_id == group.leader_member_id {
            let assigned_members = assignments
                .iter()
                .map(|assignment| assignment.member_id.as_str())
                .collect::<HashSet<_>>();
            if assigned_members.len() != assignments.len()
                || assigned_members.len() != group.members.len()
                || !group
                    .members
                    .keys()
                    .all(|member_id| assigned_members.contains(member_id.as_str()))
            {
                return Err(GroupError::InvalidAssignment);
            }
            group.assignments = assignments
                .into_iter()
                .map(|assignment| (assignment.member_id, assignment.assignment))
                .collect();
            group.state = GroupState::Stable;
            tracing::info!(
                group = group_id,
                generation = group.generation_id,
                protocol = group.selected_protocol,
                members = group.members.len(),
                "consumer group generation is stable"
            );
        }

        group
            .assignments
            .get(member_id)
            .cloned()
            .ok_or(GroupError::RebalanceInProgress)
    }

    pub(crate) async fn heartbeat(
        &self,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
    ) -> Result<(), GroupError> {
        let group = self
            .group(group_id)
            .await
            .ok_or(GroupError::UnknownMemberId)?;
        let mut group = group.lock().await;
        validate_member_and_generation(&group, generation_id, member_id)?;
        if group.state != GroupState::Stable {
            return Err(GroupError::RebalanceInProgress);
        }
        let member = group
            .members
            .get_mut(member_id)
            .ok_or(GroupError::UnknownMemberId)?;
        member.last_heartbeat = Instant::now();
        tracing::debug!(
            group = group_id,
            member = member_id,
            client = member.client_id,
            session_timeout_ms = member.session_timeout_ms,
            "consumer group heartbeat"
        );
        Ok(())
    }

    pub(crate) async fn leave(&self, group_id: &str, member_id: &str) -> Result<(), GroupError> {
        let group = self
            .group(group_id)
            .await
            .ok_or(GroupError::UnknownMemberId)?;
        let mut group = group.lock().await;
        if group.members.remove(member_id).is_none() {
            return Err(GroupError::UnknownMemberId);
        }
        tracing::info!(
            group = group_id,
            member = member_id,
            generation = group.generation_id,
            members = group.members.len(),
            "consumer group member left"
        );
        group.assignments.remove(member_id);
        if group.members.is_empty() {
            group.state = GroupState::Empty;
            group.leader_member_id.clear();
            group.protocol_type.clear();
            group.selected_protocol.clear();
            group.assignments.clear();
            tracing::info!(group = group_id, "consumer group is empty");
        } else {
            group.state = GroupState::PreparingRebalance;
            if group.leader_member_id == member_id {
                group.leader_member_id = group.members.keys().min().cloned().unwrap_or_default();
            }
            tracing::info!(
                group = group_id,
                generation = group.generation_id,
                members = group.members.len(),
                "consumer group rebalance started"
            );
        }
        Ok(())
    }

    pub(crate) async fn commit_offsets(
        &self,
        group_id: &str,
        generation_id: i32,
        member_id: &str,
        offsets: Vec<OffsetCommit>,
    ) -> Result<(), GroupError> {
        let group = self
            .group(group_id)
            .await
            .ok_or(GroupError::UnknownMemberId)?;
        let mut group = group.lock().await;
        validate_member_and_generation(&group, generation_id, member_id)?;
        if group.state != GroupState::Stable {
            return Err(GroupError::RebalanceInProgress);
        }
        for offset in offsets {
            group.committed_offsets.insert(
                TopicPartition::new(offset.topic, offset.partition),
                StoredOffset {
                    offset: offset.offset,
                    metadata: offset.metadata,
                },
            );
        }
        Ok(())
    }

    pub(crate) async fn fetch_offsets(
        &self,
        group_id: &str,
        requested: Option<&[TopicPartition]>,
    ) -> Vec<FetchedOffset> {
        let Some(group) = self.group(group_id).await else {
            return requested
                .unwrap_or_default()
                .iter()
                .map(|topic_partition| FetchedOffset {
                    topic: topic_partition.topic.clone(),
                    partition: topic_partition.partition,
                    offset: None,
                    metadata: None,
                })
                .collect();
        };
        let group = group.lock().await;
        let mut offsets = match requested {
            Some(requested) => requested
                .iter()
                .map(|topic_partition| {
                    let stored = group.committed_offsets.get(topic_partition);
                    FetchedOffset {
                        topic: topic_partition.topic.clone(),
                        partition: topic_partition.partition,
                        offset: stored.map(|stored| stored.offset),
                        metadata: stored.and_then(|stored| stored.metadata.clone()),
                    }
                })
                .collect::<Vec<_>>(),
            None => group
                .committed_offsets
                .iter()
                .map(|(topic_partition, stored)| FetchedOffset {
                    topic: topic_partition.topic.clone(),
                    partition: topic_partition.partition,
                    offset: Some(stored.offset),
                    metadata: stored.metadata.clone(),
                })
                .collect::<Vec<_>>(),
        };
        offsets.sort_by(|left, right| {
            left.topic
                .cmp(&right.topic)
                .then(left.partition.cmp(&right.partition))
        });
        offsets
    }

    fn allocate_member_id(&self, client_id: &str) -> String {
        let sequence = self.inner.next_member_id.fetch_add(1, Ordering::Relaxed);
        format!("{client_id}-{sequence}")
    }

    async fn member_is_known_or_pending(&self, request: &JoinRequest) -> bool {
        if self
            .inner
            .pending_member_ids
            .lock()
            .await
            .contains(&(request.group_id.clone(), request.member_id.clone()))
        {
            return true;
        }
        let Some(group) = self.group(&request.group_id).await else {
            return false;
        };
        group.lock().await.members.contains_key(&request.member_id)
    }

    async fn group(&self, group_id: &str) -> Option<Arc<Mutex<Group>>> {
        self.inner.groups.read().await.get(group_id).cloned()
    }

    async fn group_or_insert(&self, group_id: &str) -> Arc<Mutex<Group>> {
        if let Some(group) = self.group(group_id).await {
            return group;
        }
        self.inner
            .groups
            .write()
            .await
            .entry(group_id.to_owned())
            .or_insert_with(|| Arc::new(Mutex::new(Group::new())))
            .clone()
    }

    #[cfg(test)]
    async fn snapshot(&self, group_id: &str) -> Option<GroupSnapshot> {
        let group = self.group(group_id).await?;
        let group = group.lock().await;
        Some(GroupSnapshot {
            state: group.state,
            generation_id: group.generation_id,
            member_count: group.members.len(),
            assignment_count: group.assignments.len(),
            selected_protocol: group.selected_protocol.clone(),
        })
    }
}

impl Default for GroupCoordinator {
    fn default() -> Self {
        Self::new()
    }
}

impl Group {
    fn new() -> Self {
        Self {
            state: GroupState::Empty,
            generation_id: 0,
            protocol_type: String::new(),
            selected_protocol: String::new(),
            leader_member_id: String::new(),
            members: HashMap::new(),
            assignments: HashMap::new(),
            committed_offsets: HashMap::new(),
        }
    }
}

fn select_protocol(members: &HashMap<String, Member>) -> Result<String, GroupError> {
    if members.is_empty() {
        return Err(GroupError::InconsistentGroupProtocol);
    }

    let common_protocols = members
        .values()
        .flat_map(|member| {
            member
                .protocols
                .iter()
                .map(|protocol| protocol.name.clone())
        })
        .filter(|candidate| {
            members.values().all(|member| {
                member
                    .protocols
                    .iter()
                    .any(|protocol| protocol.name == *candidate)
            })
        })
        .collect::<BTreeSet<_>>();
    let mut votes = BTreeMap::<String, usize>::new();
    for member in members.values() {
        let preferred = member
            .protocols
            .iter()
            .find(|protocol| common_protocols.contains(&protocol.name))
            .ok_or(GroupError::InconsistentGroupProtocol)?;
        *votes.entry(preferred.name.clone()).or_default() += 1;
    }

    votes
        .into_iter()
        .max_by(|(left_name, left_votes), (right_name, right_votes)| {
            left_votes
                .cmp(right_votes)
                .then_with(|| right_name.cmp(left_name))
        })
        .map(|(name, _)| name)
        .ok_or(GroupError::InconsistentGroupProtocol)
}

fn members_for_leader(group: &Group) -> Vec<JoinedMember> {
    let mut members = group
        .members
        .iter()
        .filter_map(|(member_id, member)| {
            member
                .protocols
                .iter()
                .find(|protocol| protocol.name == group.selected_protocol)
                .map(|protocol| JoinedMember {
                    member_id: member_id.clone(),
                    metadata: protocol.metadata.clone(),
                })
        })
        .collect::<Vec<_>>();
    members.sort_by(|left, right| left.member_id.cmp(&right.member_id));
    members
}

fn validate_member_and_generation(
    group: &Group,
    generation_id: i32,
    member_id: &str,
) -> Result<(), GroupError> {
    if !group.members.contains_key(member_id) {
        return Err(GroupError::UnknownMemberId);
    }
    if group.generation_id != generation_id {
        return Err(GroupError::IllegalGeneration);
    }
    Ok(())
}

#[cfg(test)]
#[derive(Clone, Debug, Eq, PartialEq)]
struct GroupSnapshot {
    state: GroupState,
    generation_id: i32,
    member_count: usize,
    assignment_count: usize,
    selected_protocol: String,
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use bytes::Bytes;
    use tokio::time::Instant;

    use super::{
        GroupCoordinator, GroupError, GroupState, JoinProtocol, JoinRequest, JoinResult, Member,
        OffsetCommit, SyncAssignment, TopicPartition, select_protocol,
    };

    fn join_request(member_id: &str) -> JoinRequest {
        JoinRequest {
            group_id: "orders".to_owned(),
            member_id: member_id.to_owned(),
            client_id: "consumer-a".to_owned(),
            session_timeout_ms: 10_000,
            protocol_type: "consumer".to_owned(),
            protocols: vec![JoinProtocol {
                name: "cooperative-sticky".to_owned(),
                metadata: Bytes::from_static(b"subscription-a"),
            }],
        }
    }

    async fn claim_member(coordinator: &GroupCoordinator) -> String {
        match coordinator.join(join_request(""), true).await.unwrap() {
            JoinResult::MemberIdRequired { member_id } => member_id,
            JoinResult::Joined(_) => panic!("expected member-ID handshake"),
        }
    }

    fn member_with_protocols(protocol_names: &[&str]) -> Member {
        Member {
            client_id: "test-client".to_owned(),
            session_timeout_ms: 10_000,
            last_heartbeat: Instant::now(),
            protocols: protocol_names
                .iter()
                .map(|name| JoinProtocol {
                    name: (*name).to_owned(),
                    metadata: Bytes::new(),
                })
                .collect(),
        }
    }

    async fn stabilize(coordinator: &GroupCoordinator, group_id: &str) -> (String, i32) {
        let mut request = join_request("");
        request.group_id = group_id.to_owned();
        let member_id = match coordinator.join(request, true).await.unwrap() {
            JoinResult::MemberIdRequired { member_id } => member_id,
            JoinResult::Joined(_) => unreachable!(),
        };
        let mut request = join_request(&member_id);
        request.group_id = group_id.to_owned();
        let joined = match coordinator.join(request, true).await.unwrap() {
            JoinResult::Joined(joined) => joined,
            JoinResult::MemberIdRequired { .. } => unreachable!(),
        };
        coordinator
            .sync(
                group_id,
                joined.generation_id,
                &member_id,
                vec![SyncAssignment {
                    member_id: member_id.clone(),
                    assignment: Bytes::new(),
                }],
            )
            .await
            .unwrap();
        (member_id, joined.generation_id)
    }

    #[tokio::test]
    async fn requires_a_generated_member_id_before_mutating_the_group() {
        let coordinator = GroupCoordinator::new();

        let member_id = claim_member(&coordinator).await;

        assert!(member_id.starts_with("consumer-a-"));
        assert_eq!(coordinator.snapshot("orders").await, None);
    }

    #[tokio::test]
    async fn first_member_becomes_leader_of_generation_one() {
        let coordinator = GroupCoordinator::new();
        let member_id = claim_member(&coordinator).await;

        let joined = match coordinator
            .join(join_request(&member_id), true)
            .await
            .unwrap()
        {
            JoinResult::Joined(joined) => joined,
            JoinResult::MemberIdRequired { .. } => panic!("member was already claimed"),
        };

        assert_eq!(joined.generation_id, 1);
        assert_eq!(joined.member_id, member_id);
        assert_eq!(joined.leader, joined.member_id);
        assert_eq!(joined.protocol_type, "consumer");
        assert_eq!(joined.protocol_name, "cooperative-sticky");
        assert_eq!(joined.members.len(), 1);
        assert_eq!(joined.members[0].member_id, joined.member_id);
        assert_eq!(joined.members[0].metadata, b"subscription-a"[..]);

        let snapshot = coordinator.snapshot("orders").await.unwrap();
        assert_eq!(snapshot.state, GroupState::CompletingRebalance);
        assert_eq!(snapshot.generation_id, 1);
    }

    #[test]
    fn protocol_selection_uses_member_preferences_with_a_deterministic_vote() {
        let mut members = HashMap::from([
            (
                "member-a".to_owned(),
                member_with_protocols(&["cooperative-sticky", "range"]),
            ),
            (
                "member-b".to_owned(),
                member_with_protocols(&["cooperative-sticky", "range"]),
            ),
            (
                "member-c".to_owned(),
                member_with_protocols(&["cooperative-sticky", "range"]),
            ),
        ]);
        let first_member_id = members.keys().next().unwrap().clone();
        members.get_mut(&first_member_id).unwrap().protocols = vec![
            JoinProtocol {
                name: "range".to_owned(),
                metadata: Bytes::new(),
            },
            JoinProtocol {
                name: "cooperative-sticky".to_owned(),
                metadata: Bytes::new(),
            },
        ];

        assert_eq!(select_protocol(&members).unwrap(), "cooperative-sticky");
    }

    #[tokio::test]
    async fn leader_sync_stabilizes_the_group_and_heartbeats_are_fenced() {
        let coordinator = GroupCoordinator::new();
        let member_id = claim_member(&coordinator).await;
        let joined = match coordinator
            .join(join_request(&member_id), true)
            .await
            .unwrap()
        {
            JoinResult::Joined(joined) => joined,
            JoinResult::MemberIdRequired { .. } => unreachable!(),
        };
        let assignment = Bytes::from_static(b"assignment-a");

        let returned = coordinator
            .sync(
                "orders",
                joined.generation_id,
                &member_id,
                vec![SyncAssignment {
                    member_id: member_id.clone(),
                    assignment: assignment.clone(),
                }],
            )
            .await
            .unwrap();

        assert_eq!(returned, assignment);
        assert_eq!(
            coordinator.snapshot("orders").await.unwrap().state,
            GroupState::Stable
        );
        assert_eq!(
            coordinator
                .heartbeat("orders", joined.generation_id, &member_id)
                .await,
            Ok(())
        );
        assert_eq!(
            coordinator
                .heartbeat("orders", joined.generation_id + 1, &member_id)
                .await,
            Err(GroupError::IllegalGeneration)
        );
        assert_eq!(
            coordinator
                .heartbeat("orders", joined.generation_id, "missing")
                .await,
            Err(GroupError::UnknownMemberId)
        );
    }

    #[tokio::test]
    async fn incomplete_leader_assignments_are_rejected_without_partial_installation() {
        let coordinator = GroupCoordinator::new();
        let leader_id = claim_member(&coordinator).await;
        coordinator
            .join(join_request(&leader_id), true)
            .await
            .unwrap();

        let mut second_handshake = join_request("");
        second_handshake.client_id = "consumer-b".to_owned();
        let second_id = match coordinator.join(second_handshake, true).await.unwrap() {
            JoinResult::MemberIdRequired { member_id } => member_id,
            JoinResult::Joined(_) => unreachable!(),
        };
        let mut second_join = join_request(&second_id);
        second_join.client_id = "consumer-b".to_owned();
        coordinator.join(second_join, true).await.unwrap();

        let joined = match coordinator
            .join(join_request(&leader_id), true)
            .await
            .unwrap()
        {
            JoinResult::Joined(joined) => joined,
            JoinResult::MemberIdRequired { .. } => unreachable!(),
        };
        assert_eq!(joined.members.len(), 2);
        let before = coordinator.snapshot("orders").await.unwrap();

        assert!(
            coordinator
                .sync(
                    "orders",
                    joined.generation_id,
                    &leader_id,
                    vec![SyncAssignment {
                        member_id: leader_id.clone(),
                        assignment: Bytes::from_static(b"leader-only"),
                    }],
                )
                .await
                .is_err()
        );
        assert_eq!(coordinator.snapshot("orders").await.unwrap(), before);
    }

    #[tokio::test]
    async fn graceful_leave_empties_members_but_retains_the_group() {
        let coordinator = GroupCoordinator::new();
        let member_id = claim_member(&coordinator).await;
        let joined = match coordinator
            .join(join_request(&member_id), true)
            .await
            .unwrap()
        {
            JoinResult::Joined(joined) => joined,
            JoinResult::MemberIdRequired { .. } => unreachable!(),
        };
        coordinator
            .sync(
                "orders",
                joined.generation_id,
                &member_id,
                vec![SyncAssignment {
                    member_id: member_id.clone(),
                    assignment: Bytes::new(),
                }],
            )
            .await
            .unwrap();

        coordinator.leave("orders", &member_id).await.unwrap();

        let snapshot = coordinator.snapshot("orders").await.unwrap();
        assert_eq!(snapshot.state, GroupState::Empty);
        assert_eq!(snapshot.member_count, 0);
    }

    #[tokio::test]
    async fn committed_offsets_are_isolated_and_overwrite_only_the_same_key() {
        let coordinator = GroupCoordinator::new();
        let (member_id, generation_id) = stabilize(&coordinator, "orders").await;
        let (other_member_id, other_generation_id) = stabilize(&coordinator, "billing").await;

        coordinator
            .commit_offsets(
                "orders",
                generation_id,
                &member_id,
                vec![
                    OffsetCommit {
                        topic: "events".to_owned(),
                        partition: 0,
                        offset: 4,
                        metadata: Some("first".to_owned()),
                    },
                    OffsetCommit {
                        topic: "events".to_owned(),
                        partition: 1,
                        offset: 9,
                        metadata: None,
                    },
                ],
            )
            .await
            .unwrap();
        coordinator
            .commit_offsets(
                "orders",
                generation_id,
                &member_id,
                vec![OffsetCommit {
                    topic: "events".to_owned(),
                    partition: 0,
                    offset: 5,
                    metadata: Some("second".to_owned()),
                }],
            )
            .await
            .unwrap();
        coordinator
            .commit_offsets(
                "billing",
                other_generation_id,
                &other_member_id,
                vec![OffsetCommit {
                    topic: "events".to_owned(),
                    partition: 0,
                    offset: 2,
                    metadata: None,
                }],
            )
            .await
            .unwrap();

        let orders = coordinator
            .fetch_offsets(
                "orders",
                Some(&[
                    TopicPartition::new("events", 0),
                    TopicPartition::new("events", 1),
                    TopicPartition::new("missing", 0),
                ]),
            )
            .await;
        assert_eq!(orders[0].offset, Some(5));
        assert_eq!(orders[0].metadata.as_deref(), Some("second"));
        assert_eq!(orders[1].offset, Some(9));
        assert_eq!(orders[2].offset, None);

        let billing = coordinator
            .fetch_offsets("billing", Some(&[TopicPartition::new("events", 0)]))
            .await;
        assert_eq!(billing[0].offset, Some(2));
    }

    #[tokio::test]
    async fn offset_commits_are_fenced_and_survive_the_last_member_leaving() {
        let coordinator = GroupCoordinator::new();
        let (member_id, generation_id) = stabilize(&coordinator, "orders").await;
        let commit = || OffsetCommit {
            topic: "events".to_owned(),
            partition: 0,
            offset: 7,
            metadata: None,
        };

        assert_eq!(
            coordinator
                .commit_offsets("orders", generation_id + 1, &member_id, vec![commit()])
                .await,
            Err(GroupError::IllegalGeneration)
        );
        assert_eq!(
            coordinator
                .commit_offsets("orders", generation_id, "missing", vec![commit()])
                .await,
            Err(GroupError::UnknownMemberId)
        );
        coordinator
            .commit_offsets("orders", generation_id, &member_id, vec![commit()])
            .await
            .unwrap();
        coordinator.leave("orders", &member_id).await.unwrap();

        let offsets = coordinator
            .fetch_offsets("orders", Some(&[TopicPartition::new("events", 0)]))
            .await;
        assert_eq!(offsets[0].offset, Some(7));
    }

    #[tokio::test]
    async fn incompatible_join_does_not_mutate_a_stable_group() {
        let coordinator = GroupCoordinator::new();
        let (member_id, generation_id) = stabilize(&coordinator, "orders").await;
        let before = coordinator.snapshot("orders").await.unwrap();
        let mut handshake = join_request("");
        handshake.client_id = "consumer-b".to_owned();
        let rejected_member = match coordinator.join(handshake, true).await.unwrap() {
            JoinResult::MemberIdRequired { member_id } => member_id,
            JoinResult::Joined(_) => unreachable!(),
        };
        let mut incompatible = join_request(&rejected_member);
        incompatible.client_id = "consumer-b".to_owned();
        incompatible.protocols = vec![JoinProtocol {
            name: "range".to_owned(),
            metadata: Bytes::from_static(b"subscription-b"),
        }];

        assert_eq!(
            coordinator.join(incompatible, true).await,
            Err(GroupError::InconsistentGroupProtocol)
        );
        assert_eq!(coordinator.snapshot("orders").await.unwrap(), before);
        assert_eq!(
            coordinator
                .heartbeat("orders", generation_id, &member_id)
                .await,
            Ok(())
        );
    }
}
