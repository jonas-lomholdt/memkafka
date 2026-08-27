use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use bytes::Bytes;
use tokio::{
    sync::{Mutex, Notify, RwLock},
    time::{Duration, Instant, timeout_at},
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
    rebalance_epoch: u64,
    protocol_type: String,
    selected_protocol: String,
    leader_member_id: String,
    members: HashMap<String, Member>,
    assignments: HashMap<String, Bytes>,
    committed_offsets: HashMap<TopicPartition, StoredOffset>,
    rebalance_deadline: Option<Instant>,
    change: Arc<Notify>,
}

#[derive(Clone, Debug)]
struct Member {
    client_id: String,
    session_timeout_ms: i32,
    rebalance_timeout_ms: i32,
    last_heartbeat: Instant,
    protocols: Vec<JoinProtocol>,
    joined_epoch: u64,
    synced_generation: i32,
    completed_join: Option<CompletedJoin>,
}

#[derive(Clone, Debug)]
struct CompletedJoin {
    rebalance_epoch: u64,
    result: JoinedGroup,
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
    pub(crate) rebalance_timeout_ms: i32,
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
pub(crate) struct GroupDescription {
    pub(crate) group_id: String,
    pub(crate) state: &'static str,
    pub(crate) protocol_type: String,
    pub(crate) protocol_name: String,
    pub(crate) members: Vec<GroupMemberDescription>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct GroupMemberDescription {
    pub(crate) member_id: String,
    pub(crate) client_id: String,
    pub(crate) metadata: Bytes,
    pub(crate) assignment: Bytes,
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

    pub(crate) async fn describe(&self, group_id: &str) -> Option<GroupDescription> {
        let group = self.group(group_id).await?;
        let mut group = group.lock().await;
        expire_stale_members(&mut group, Instant::now(), group_id, None);

        let mut members = group
            .members
            .iter()
            .map(|(member_id, member)| GroupMemberDescription {
                member_id: member_id.clone(),
                client_id: member.client_id.clone(),
                metadata: member
                    .protocols
                    .iter()
                    .find(|protocol| protocol.name == group.selected_protocol)
                    .map_or_else(Bytes::new, |protocol| protocol.metadata.clone()),
                assignment: group
                    .assignments
                    .get(member_id)
                    .cloned()
                    .unwrap_or_default(),
            })
            .collect::<Vec<_>>();
        members.sort_unstable_by(|left, right| left.member_id.cmp(&right.member_id));

        Some(GroupDescription {
            group_id: group_id.to_owned(),
            state: match group.state {
                GroupState::Empty => "Empty",
                GroupState::PreparingRebalance => "PreparingRebalance",
                GroupState::CompletingRebalance => "CompletingRebalance",
                GroupState::Stable => "Stable",
            },
            protocol_type: group.protocol_type.clone(),
            protocol_name: group.selected_protocol.clone(),
            members,
        })
    }

    pub(crate) async fn join(
        &self,
        mut request: JoinRequest,
        require_known_member_id: bool,
    ) -> Result<JoinResult, GroupError> {
        if request.protocols.is_empty() || request.protocol_type.is_empty() {
            return Err(GroupError::InconsistentGroupProtocol);
        }

        let may_add_member;
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
            may_add_member = true;
        } else {
            may_add_member = self
                .inner
                .pending_member_ids
                .lock()
                .await
                .contains(&(request.group_id.clone(), request.member_id.clone()));
            if !may_add_member {
                let Some(group) = self.group(&request.group_id).await else {
                    return Err(GroupError::UnknownMemberId);
                };
                if !group.lock().await.members.contains_key(&request.member_id) {
                    return Err(GroupError::UnknownMemberId);
                }
            }
        }

        let group = self.group_or_insert(&request.group_id).await;
        let member_id = request.member_id.clone();
        let target_epoch = {
            let mut group = group.lock().await;
            let now = Instant::now();
            expire_stale_members(&mut group, now, &request.group_id, None);
            if !may_add_member && !group.members.contains_key(&member_id) {
                return Err(GroupError::UnknownMemberId);
            }
            if !group.members.is_empty() && group.protocol_type != request.protocol_type {
                tracing::warn!(
                    group = request.group_id.as_str(),
                    member = member_id.as_str(),
                    requested_protocol_type = request.protocol_type.as_str(),
                    group_protocol_type = group.protocol_type.as_str(),
                    "consumer group protocol type is inconsistent"
                );
                return Err(GroupError::InconsistentGroupProtocol);
            }

            let can_reuse_completed_join = group.state == GroupState::CompletingRebalance
                || (group.state == GroupState::Stable && group.leader_member_id != member_id);
            let cached_join = group
                .members
                .get(&member_id)
                .filter(|member| join_matches(member, &request))
                .and_then(|member| member.completed_join.as_ref())
                .filter(|completed| completed.rebalance_epoch == group.rebalance_epoch)
                .map(|completed| completed.result.clone());
            if can_reuse_completed_join && let Some(cached_join) = cached_join {
                group
                    .members
                    .get_mut(&member_id)
                    .expect("cached join belongs to member")
                    .last_heartbeat = now;
                return Ok(JoinResult::Joined(cached_join));
            }

            let advertised_protocols = request
                .protocols
                .iter()
                .map(|protocol| protocol.name.clone())
                .collect::<Vec<_>>();
            let starts_rebalance = group.state != GroupState::PreparingRebalance;
            let target_epoch = if starts_rebalance {
                group.rebalance_epoch.saturating_add(1)
            } else {
                group.rebalance_epoch
            };
            let prior_member = group.members.get(&member_id);
            let completed_join = prior_member.and_then(|member| member.completed_join.clone());
            let synced_generation = prior_member.map_or(-1, |member| member.synced_generation);
            let member = Member {
                client_id: request.client_id,
                session_timeout_ms: request.session_timeout_ms,
                rebalance_timeout_ms: request.rebalance_timeout_ms,
                last_heartbeat: now,
                protocols: request.protocols,
                joined_epoch: target_epoch,
                synced_generation,
                completed_join,
            };
            let mut prospective_members = group.members.clone();
            prospective_members.insert(member_id.clone(), member);
            if let Err(error) = select_protocol(&prospective_members) {
                tracing::warn!(
                    group = request.group_id.as_str(),
                    member = member_id.as_str(),
                    ?advertised_protocols,
                    "consumer group members have no common assignment protocol"
                );
                return Err(error);
            }

            group.members = prospective_members;
            if starts_rebalance {
                begin_rebalance(&mut group, &request.group_id);
            } else if may_add_member {
                extend_rebalance_deadline(&mut group, now);
            }
            group.protocol_type = request.protocol_type;
            if group.leader_member_id.is_empty() {
                group.leader_member_id.clone_from(&member_id);
            }

            tracing::info!(
                group = request.group_id.as_str(),
                member = member_id.as_str(),
                rebalance_epoch = target_epoch,
                members = group.members.len(),
                "consumer group member joined"
            );
            complete_rebalance_if_ready(&mut group, &request.group_id)?;
            target_epoch
        };

        self.inner
            .pending_member_ids
            .lock()
            .await
            .remove(&(request.group_id.clone(), member_id.clone()));

        loop {
            let (notified, deadline) = {
                let mut group = group.lock().await;
                if let Some(result) = completed_join(&group, &member_id, target_epoch) {
                    return Ok(JoinResult::Joined(result));
                }
                if !group.members.contains_key(&member_id) {
                    return Err(GroupError::UnknownMemberId);
                }
                if group.rebalance_epoch != target_epoch {
                    return Err(GroupError::RebalanceInProgress);
                }

                expire_stale_members(
                    &mut group,
                    Instant::now(),
                    &request.group_id,
                    Some(&member_id),
                );
                complete_rebalance_if_ready(&mut group, &request.group_id)?;
                if let Some(result) = completed_join(&group, &member_id, target_epoch) {
                    return Ok(JoinResult::Joined(result));
                }
                let notified = group.change.clone().notified_owned();
                let deadline = next_expiry_deadline(&group, Some(&member_id));
                (notified, deadline)
            };

            if let Some(deadline) = deadline {
                let _ = timeout_at(deadline, notified).await;
            } else {
                notified.await;
            }
        }
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
        let mut assignments = Some(assignments);
        let mut refresh_member = true;
        loop {
            let (notified, deadline) = {
                let mut group = group.lock().await;
                let now = Instant::now();
                let active_member_id = (!refresh_member).then_some(member_id);
                expire_stale_members(&mut group, now, group_id, active_member_id);
                validate_member_and_generation(&group, generation_id, member_id)?;
                if refresh_member {
                    group
                        .members
                        .get_mut(member_id)
                        .expect("validated member")
                        .last_heartbeat = now;
                    refresh_member = false;
                }

                match group.state {
                    GroupState::Stable => {
                        group
                            .members
                            .get_mut(member_id)
                            .expect("validated member")
                            .synced_generation = generation_id;
                        clear_rebalance_deadline_if_all_synced(&mut group);
                        group.change.notify_waiters();
                        return group
                            .assignments
                            .get(member_id)
                            .cloned()
                            .ok_or(GroupError::RebalanceInProgress);
                    }
                    GroupState::PreparingRebalance | GroupState::Empty => {
                        return Err(GroupError::RebalanceInProgress);
                    }
                    GroupState::CompletingRebalance => {}
                }

                if member_id == group.leader_member_id {
                    let assignments = assignments.take().unwrap_or_default();
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
                    group
                        .members
                        .get_mut(member_id)
                        .expect("validated leader")
                        .synced_generation = generation_id;
                    group.assignments = assignments
                        .into_iter()
                        .map(|assignment| (assignment.member_id, assignment.assignment))
                        .collect();
                    for member in group
                        .members
                        .values_mut()
                        .filter(|member| member.synced_generation == generation_id)
                    {
                        member.last_heartbeat = now;
                    }
                    group.state = GroupState::Stable;
                    clear_rebalance_deadline_if_all_synced(&mut group);
                    tracing::info!(
                        group = group_id,
                        generation = group.generation_id,
                        protocol = group.selected_protocol,
                        members = group.members.len(),
                        "consumer group generation is stable"
                    );
                    group.change.notify_waiters();
                    return group
                        .assignments
                        .get(member_id)
                        .cloned()
                        .ok_or(GroupError::RebalanceInProgress);
                }

                group
                    .members
                    .get_mut(member_id)
                    .expect("validated follower")
                    .synced_generation = generation_id;

                let notified = group.change.clone().notified_owned();
                let deadline = next_expiry_deadline(&group, Some(member_id));
                (notified, deadline)
            };

            if let Some(deadline) = deadline {
                let _ = timeout_at(deadline, notified).await;
            } else {
                notified.await;
            }
        }
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
        let now = Instant::now();
        expire_stale_members(&mut group, now, group_id, None);
        validate_member_and_generation(&group, generation_id, member_id)?;
        let member = group
            .members
            .get_mut(member_id)
            .ok_or(GroupError::UnknownMemberId)?;
        member.last_heartbeat = now;
        tracing::debug!(
            group = group_id,
            member = member_id,
            client = member.client_id,
            session_timeout_ms = member.session_timeout_ms,
            "consumer group heartbeat"
        );
        match group.state {
            GroupState::CompletingRebalance | GroupState::Stable => Ok(()),
            GroupState::Empty | GroupState::PreparingRebalance => {
                Err(GroupError::RebalanceInProgress)
            }
        }
    }

    pub(crate) async fn leave(&self, group_id: &str, member_id: &str) -> Result<(), GroupError> {
        let group = self
            .group(group_id)
            .await
            .ok_or(GroupError::UnknownMemberId)?;
        let mut group = group.lock().await;
        expire_stale_members(&mut group, Instant::now(), group_id, None);
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
        transition_after_removal(&mut group, group_id);
        complete_rebalance_if_ready(&mut group, group_id)?;
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
        expire_stale_members(&mut group, Instant::now(), group_id, None);
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

    async fn group(&self, group_id: &str) -> Option<Arc<Mutex<Group>>> {
        self.inner.groups.read().await.get(group_id).cloned()
    }

    async fn group_or_insert(&self, group_id: &str) -> Arc<Mutex<Group>> {
        if let Some(group) = self.group(group_id).await {
            return group;
        }
        let mut groups = self.inner.groups.write().await;
        match groups.entry(group_id.to_owned()) {
            std::collections::hash_map::Entry::Occupied(entry) => entry.get().clone(),
            std::collections::hash_map::Entry::Vacant(entry) => {
                let group = Arc::new(Mutex::new(Group::new()));
                entry.insert(Arc::clone(&group));
                tokio::spawn(drive_group_deadlines(
                    group_id.to_owned(),
                    Arc::clone(&group),
                ));
                group
            }
        }
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

async fn drive_group_deadlines(group_id: String, group: Arc<Mutex<Group>>) {
    loop {
        let (notified, deadline) = {
            let mut group = group.lock().await;
            expire_stale_members(&mut group, Instant::now(), &group_id, None);
            if let Err(error) = complete_rebalance_if_ready(&mut group, &group_id) {
                tracing::error!(
                    group = group_id.as_str(),
                    ?error,
                    "consumer group deadline driver could not complete rebalance"
                );
            }
            (
                group.change.clone().notified_owned(),
                next_expiry_deadline(&group, None),
            )
        };

        if let Some(deadline) = deadline {
            let _ = timeout_at(deadline, notified).await;
        } else {
            notified.await;
        }
    }
}

impl Group {
    fn new() -> Self {
        Self {
            state: GroupState::Empty,
            generation_id: 0,
            rebalance_epoch: 0,
            protocol_type: String::new(),
            selected_protocol: String::new(),
            leader_member_id: String::new(),
            members: HashMap::new(),
            assignments: HashMap::new(),
            committed_offsets: HashMap::new(),
            rebalance_deadline: None,
            change: Arc::new(Notify::new()),
        }
    }
}

fn begin_rebalance(group: &mut Group, group_id: &str) {
    group.rebalance_epoch = group.rebalance_epoch.saturating_add(1);
    group.state = GroupState::PreparingRebalance;
    group.assignments.clear();
    group.rebalance_deadline = Some(Instant::now() + group_rebalance_timeout(group));
    tracing::info!(
        group = group_id,
        generation = group.generation_id.saturating_add(1),
        members = group.members.len(),
        "consumer group rebalance started"
    );
    group.change.notify_waiters();
}

fn complete_rebalance_if_ready(group: &mut Group, group_id: &str) -> Result<bool, GroupError> {
    if group.state != GroupState::PreparingRebalance
        || group.members.is_empty()
        || group
            .members
            .values()
            .any(|member| member.joined_epoch != group.rebalance_epoch)
    {
        return Ok(false);
    }

    let selected_protocol = select_protocol(&group.members)?;
    group.selected_protocol = selected_protocol;
    group.generation_id = group.generation_id.saturating_add(1);
    group.assignments.clear();
    group.state = GroupState::CompletingRebalance;
    group.rebalance_deadline = Some(Instant::now() + group_rebalance_timeout(group));

    let generation_id = group.generation_id;
    let rebalance_epoch = group.rebalance_epoch;
    let protocol_type = group.protocol_type.clone();
    let protocol_name = group.selected_protocol.clone();
    let leader = group.leader_member_id.clone();
    let leader_members = members_for_leader(group);
    let member_ids = group.members.keys().cloned().collect::<Vec<_>>();
    for member_id in member_ids {
        let members = if member_id == leader {
            leader_members.clone()
        } else {
            Vec::new()
        };
        group
            .members
            .get_mut(&member_id)
            .expect("member ID came from group")
            .completed_join = Some(CompletedJoin {
            rebalance_epoch,
            result: JoinedGroup {
                generation_id,
                protocol_type: protocol_type.clone(),
                protocol_name: protocol_name.clone(),
                leader: leader.clone(),
                member_id: member_id.clone(),
                members,
            },
        });
    }

    tracing::info!(
        group = group_id,
        generation = generation_id,
        protocol = protocol_name.as_str(),
        "consumer group assignment protocol selected"
    );
    if protocol_name == "cooperative-sticky" {
        tracing::info!(
            group = group_id,
            generation = generation_id,
            protocol = protocol_name.as_str(),
            rebalance = "cooperative",
            members = group.members.len(),
            "Using cooperative incremental rebalancing"
        );
    }
    group.change.notify_waiters();
    Ok(true)
}

fn completed_join(group: &Group, member_id: &str, rebalance_epoch: u64) -> Option<JoinedGroup> {
    group
        .members
        .get(member_id)?
        .completed_join
        .as_ref()
        .filter(|completed| completed.rebalance_epoch == rebalance_epoch)
        .map(|completed| completed.result.clone())
}

fn join_matches(member: &Member, request: &JoinRequest) -> bool {
    member.client_id == request.client_id
        && member.session_timeout_ms == request.session_timeout_ms
        && member.rebalance_timeout_ms == request.rebalance_timeout_ms
        && member.protocols == request.protocols
}

fn expire_stale_members(
    group: &mut Group,
    now: Instant,
    group_id: &str,
    active_member_id: Option<&str>,
) {
    let rebalance_expired = group
        .rebalance_deadline
        .is_some_and(|deadline| deadline <= now);
    let expired = group
        .members
        .iter()
        .filter(|(member_id, member)| {
            if active_member_id == Some(member_id.as_str()) {
                return false;
            }
            match group.state {
                GroupState::Empty => false,
                GroupState::PreparingRebalance => {
                    rebalance_expired && member.joined_epoch != group.rebalance_epoch
                }
                GroupState::CompletingRebalance => {
                    rebalance_expired && member.synced_generation != group.generation_id
                }
                GroupState::Stable => {
                    let pending_sync = member.synced_generation != group.generation_id;
                    (rebalance_expired && pending_sync)
                        || (!pending_sync && member_deadline(member) <= now)
                }
            }
        })
        .map(|(member_id, _)| member_id.clone())
        .collect::<Vec<_>>();
    if expired.is_empty() {
        return;
    }

    for member_id in expired {
        group.members.remove(&member_id);
        group.assignments.remove(&member_id);
        tracing::info!(
            group = group_id,
            member = member_id.as_str(),
            generation = group.generation_id,
            members = group.members.len(),
            "consumer group member expired"
        );
    }
    transition_after_removal(group, group_id);
}

fn transition_after_removal(group: &mut Group, group_id: &str) {
    if group.members.is_empty() {
        group.state = GroupState::Empty;
        group.leader_member_id.clear();
        group.protocol_type.clear();
        group.selected_protocol.clear();
        group.assignments.clear();
        group.rebalance_deadline = None;
        group.change.notify_waiters();
        tracing::info!(group = group_id, "consumer group is empty");
        return;
    }

    if !group.members.contains_key(&group.leader_member_id) {
        group.leader_member_id = group.members.keys().min().cloned().unwrap_or_default();
    }
    if group.state != GroupState::PreparingRebalance {
        begin_rebalance(group, group_id);
    } else {
        group.assignments.clear();
        group.change.notify_waiters();
    }
}

fn extend_rebalance_deadline(group: &mut Group, now: Instant) {
    let candidate = now + group_rebalance_timeout(group);
    if group
        .rebalance_deadline
        .is_none_or(|deadline| candidate > deadline)
    {
        group.rebalance_deadline = Some(candidate);
    }
}

fn group_rebalance_timeout(group: &Group) -> Duration {
    let timeout_ms = group
        .members
        .values()
        .map(|member| member.rebalance_timeout_ms)
        .max()
        .unwrap_or_default();
    Duration::from_millis(u64::try_from(timeout_ms.max(0)).unwrap_or_default())
}

fn clear_rebalance_deadline_if_all_synced(group: &mut Group) {
    if group
        .members
        .values()
        .all(|member| member.synced_generation == group.generation_id)
    {
        group.rebalance_deadline = None;
    }
}

fn next_expiry_deadline(group: &Group, active_member_id: Option<&str>) -> Option<Instant> {
    let rebalance_deadline = group.rebalance_deadline.filter(|_| {
        group.members.iter().any(|(member_id, member)| {
            active_member_id != Some(member_id.as_str())
                && match group.state {
                    GroupState::PreparingRebalance => member.joined_epoch != group.rebalance_epoch,
                    GroupState::CompletingRebalance | GroupState::Stable => {
                        member.synced_generation != group.generation_id
                    }
                    GroupState::Empty => false,
                }
        })
    });
    let session_deadline = (group.state == GroupState::Stable)
        .then(|| {
            group
                .members
                .iter()
                .filter(|(member_id, member)| {
                    active_member_id != Some(member_id.as_str())
                        && member.synced_generation == group.generation_id
                })
                .map(|(_, member)| member_deadline(member))
                .min()
        })
        .flatten();
    rebalance_deadline.into_iter().chain(session_deadline).min()
}

fn member_deadline(member: &Member) -> Instant {
    let timeout_ms = u64::try_from(member.session_timeout_ms.max(0)).unwrap_or_default();
    member.last_heartbeat + Duration::from_millis(timeout_ms)
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
    use tokio::{
        task::yield_now,
        time::{Duration, Instant, advance, timeout},
    };

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
            rebalance_timeout_ms: 30_000,
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
            rebalance_timeout_ms: 30_000,
            last_heartbeat: Instant::now(),
            protocols: protocol_names
                .iter()
                .map(|name| JoinProtocol {
                    name: (*name).to_owned(),
                    metadata: Bytes::new(),
                })
                .collect(),
            joined_epoch: 0,
            synced_generation: -1,
            completed_join: None,
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

    #[tokio::test]
    async fn describes_stable_group_without_mutating_it() {
        let coordinator = GroupCoordinator::new();
        let mut handshake = join_request("");
        handshake.group_id = "described-group".to_owned();
        handshake.client_id = "describe-client".to_owned();
        handshake.protocols[0].metadata = Bytes::from_static(b"subscription");
        let member_id = match coordinator.join(handshake, true).await.unwrap() {
            JoinResult::MemberIdRequired { member_id } => member_id,
            JoinResult::Joined(_) => unreachable!(),
        };
        let mut request = join_request(&member_id);
        request.group_id = "described-group".to_owned();
        request.client_id = "describe-client".to_owned();
        request.protocols[0].metadata = Bytes::from_static(b"subscription");
        let joined = match coordinator.join(request, true).await.unwrap() {
            JoinResult::Joined(joined) => joined,
            JoinResult::MemberIdRequired { .. } => unreachable!(),
        };
        coordinator
            .sync(
                "described-group",
                joined.generation_id,
                &member_id,
                vec![SyncAssignment {
                    member_id: member_id.clone(),
                    assignment: Bytes::from_static(b"assignment"),
                }],
            )
            .await
            .unwrap();

        let before = coordinator.snapshot("described-group").await.unwrap();
        let description = coordinator
            .describe("described-group")
            .await
            .expect("group exists");
        assert_eq!(description.group_id, "described-group");
        assert_eq!(description.state, "Stable");
        assert_eq!(description.protocol_type, "consumer");
        assert_eq!(description.protocol_name, "cooperative-sticky");
        assert_eq!(description.members.len(), 1);
        assert_eq!(description.members[0].client_id, "describe-client");
        assert_eq!(
            description.members[0].metadata,
            Bytes::from_static(b"subscription")
        );
        assert_eq!(
            description.members[0].assignment,
            Bytes::from_static(b"assignment")
        );
        assert_eq!(
            coordinator.snapshot("described-group").await.unwrap(),
            before
        );
        assert!(coordinator.describe("missing-group").await.is_none());
    }

    #[tokio::test]
    async fn repeated_join_while_completing_returns_the_cached_generation() {
        let coordinator = GroupCoordinator::new();
        let member_id = claim_member(&coordinator).await;
        let request = join_request(&member_id);
        let first = coordinator.join(request.clone(), true).await.unwrap();
        let before = coordinator.snapshot("orders").await.unwrap();

        let repeated = coordinator.join(request, true).await.unwrap();

        assert_eq!(repeated, first);
        assert_eq!(coordinator.snapshot("orders").await.unwrap(), before);
    }

    #[tokio::test(start_paused = true)]
    async fn heartbeat_is_accepted_and_refreshed_while_completing_rebalance() {
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

        advance(Duration::from_millis(9_000)).await;
        assert_eq!(
            coordinator
                .heartbeat("orders", joined.generation_id, &member_id)
                .await,
            Ok(())
        );
        advance(Duration::from_millis(9_000)).await;
        assert_eq!(
            coordinator
                .heartbeat("orders", joined.generation_id, &member_id)
                .await,
            Ok(())
        );
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
        let second_coordinator = coordinator.clone();
        let second =
            tokio::spawn(async move { second_coordinator.join(second_join, true).await.unwrap() });
        yield_now().await;

        let joined = match coordinator
            .join(join_request(&leader_id), true)
            .await
            .unwrap()
        {
            JoinResult::Joined(joined) => joined,
            JoinResult::MemberIdRequired { .. } => unreachable!(),
        };
        second.await.unwrap();
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

    #[tokio::test]
    async fn rebalance_waits_for_all_members_and_returns_one_generation() {
        let coordinator = GroupCoordinator::new();
        let (leader_id, first_generation) = stabilize(&coordinator, "orders").await;
        let mut second_handshake = join_request("");
        second_handshake.client_id = "consumer-b".to_owned();
        let second_id = match coordinator.join(second_handshake, true).await.unwrap() {
            JoinResult::MemberIdRequired { member_id } => member_id,
            JoinResult::Joined(_) => unreachable!(),
        };
        let mut second_join = join_request(&second_id);
        second_join.client_id = "consumer-b".to_owned();
        second_join.protocols[0].metadata = Bytes::from_static(b"subscription-b");
        let second_coordinator = coordinator.clone();
        let second =
            tokio::spawn(async move { second_coordinator.join(second_join, true).await.unwrap() });

        yield_now().await;
        assert!(!second.is_finished());

        let leader = match coordinator
            .join(join_request(&leader_id), true)
            .await
            .unwrap()
        {
            JoinResult::Joined(joined) => joined,
            JoinResult::MemberIdRequired { .. } => unreachable!(),
        };
        let second = match second.await.unwrap() {
            JoinResult::Joined(joined) => joined,
            JoinResult::MemberIdRequired { .. } => unreachable!(),
        };

        assert_eq!(leader.generation_id, first_generation + 1);
        assert_eq!(second.generation_id, leader.generation_id);
        assert_eq!(leader.leader, leader_id);
        assert_eq!(leader.members.len(), 2);
        assert!(second.members.is_empty());
    }

    #[tokio::test]
    async fn follower_sync_waits_for_the_leaders_complete_assignment() {
        let coordinator = GroupCoordinator::new();
        let (leader_id, _) = stabilize(&coordinator, "orders").await;
        let mut second_handshake = join_request("");
        second_handshake.client_id = "consumer-b".to_owned();
        let second_id = match coordinator.join(second_handshake, true).await.unwrap() {
            JoinResult::MemberIdRequired { member_id } => member_id,
            JoinResult::Joined(_) => unreachable!(),
        };
        let mut second_join = join_request(&second_id);
        second_join.client_id = "consumer-b".to_owned();
        let second_coordinator = coordinator.clone();
        let second =
            tokio::spawn(async move { second_coordinator.join(second_join, true).await.unwrap() });
        yield_now().await;
        let leader = match coordinator
            .join(join_request(&leader_id), true)
            .await
            .unwrap()
        {
            JoinResult::Joined(joined) => joined,
            JoinResult::MemberIdRequired { .. } => unreachable!(),
        };
        let second = match second.await.unwrap() {
            JoinResult::Joined(joined) => joined,
            JoinResult::MemberIdRequired { .. } => unreachable!(),
        };

        let follower_coordinator = coordinator.clone();
        let follower_id = second.member_id.clone();
        let generation_id = leader.generation_id;
        let follower = tokio::spawn(async move {
            follower_coordinator
                .sync("orders", generation_id, &follower_id, Vec::new())
                .await
        });
        yield_now().await;
        assert!(!follower.is_finished());

        let leader_assignment = Bytes::from_static(b"leader-assignment");
        let follower_assignment = Bytes::from_static(b"follower-assignment");
        let returned = coordinator
            .sync(
                "orders",
                generation_id,
                &leader_id,
                vec![
                    SyncAssignment {
                        member_id: leader_id.clone(),
                        assignment: leader_assignment.clone(),
                    },
                    SyncAssignment {
                        member_id: second.member_id,
                        assignment: follower_assignment.clone(),
                    },
                ],
            )
            .await
            .unwrap();

        assert_eq!(returned, leader_assignment);
        assert_eq!(follower.await.unwrap().unwrap(), follower_assignment);
    }

    #[tokio::test]
    async fn unchanged_stable_follower_join_returns_the_current_generation() {
        let coordinator = GroupCoordinator::new();
        let (leader_id, _) = stabilize(&coordinator, "orders").await;
        let mut second_handshake = join_request("");
        second_handshake.client_id = "consumer-b".to_owned();
        let second_id = match coordinator.join(second_handshake, true).await.unwrap() {
            JoinResult::MemberIdRequired { member_id } => member_id,
            JoinResult::Joined(_) => unreachable!(),
        };
        let mut second_request = join_request(&second_id);
        second_request.client_id = "consumer-b".to_owned();
        let second_coordinator = coordinator.clone();
        let joining_request = second_request.clone();
        let second = tokio::spawn(async move {
            second_coordinator
                .join(joining_request, true)
                .await
                .unwrap()
        });
        yield_now().await;
        let leader = match coordinator
            .join(join_request(&leader_id), true)
            .await
            .unwrap()
        {
            JoinResult::Joined(joined) => joined,
            JoinResult::MemberIdRequired { .. } => unreachable!(),
        };
        let second = match second.await.unwrap() {
            JoinResult::Joined(joined) => joined,
            JoinResult::MemberIdRequired { .. } => unreachable!(),
        };
        coordinator
            .sync(
                "orders",
                leader.generation_id,
                &leader_id,
                vec![
                    SyncAssignment {
                        member_id: leader_id.clone(),
                        assignment: Bytes::new(),
                    },
                    SyncAssignment {
                        member_id: second_id,
                        assignment: Bytes::new(),
                    },
                ],
            )
            .await
            .unwrap();
        let before = coordinator.snapshot("orders").await.unwrap();

        let repeated = timeout(
            Duration::from_millis(100),
            coordinator.join(second_request, true),
        )
        .await
        .expect("unchanged follower join should not wait")
        .unwrap();

        assert_eq!(repeated, JoinResult::Joined(second));
        assert_eq!(coordinator.snapshot("orders").await.unwrap(), before);
    }

    #[tokio::test(start_paused = true)]
    async fn rejoining_member_gets_the_rebalance_timeout_not_the_short_session_timeout() {
        let coordinator = GroupCoordinator::new();
        let leader_id = claim_member(&coordinator).await;
        let mut leader_request = join_request(&leader_id);
        leader_request.session_timeout_ms = 1_000;
        leader_request.rebalance_timeout_ms = 10_000;
        let first = match coordinator
            .join(leader_request.clone(), true)
            .await
            .unwrap()
        {
            JoinResult::Joined(joined) => joined,
            JoinResult::MemberIdRequired { .. } => unreachable!(),
        };
        coordinator
            .sync(
                "orders",
                first.generation_id,
                &leader_id,
                vec![SyncAssignment {
                    member_id: leader_id.clone(),
                    assignment: Bytes::new(),
                }],
            )
            .await
            .unwrap();

        let mut second_handshake = join_request("");
        second_handshake.client_id = "consumer-b".to_owned();
        let second_id = match coordinator.join(second_handshake, true).await.unwrap() {
            JoinResult::MemberIdRequired { member_id } => member_id,
            JoinResult::Joined(_) => unreachable!(),
        };
        let mut second_request = join_request(&second_id);
        second_request.client_id = "consumer-b".to_owned();
        second_request.session_timeout_ms = 1_000;
        second_request.rebalance_timeout_ms = 10_000;
        let second_coordinator = coordinator.clone();
        let second =
            tokio::spawn(
                async move { second_coordinator.join(second_request, true).await.unwrap() },
            );
        yield_now().await;

        advance(Duration::from_millis(1_500)).await;
        yield_now().await;
        assert!(!second.is_finished());
        let waiting = coordinator.snapshot("orders").await.unwrap();
        assert_eq!(waiting.state, GroupState::PreparingRebalance);
        assert_eq!(waiting.member_count, 2);

        let leader = match coordinator.join(leader_request, true).await.unwrap() {
            JoinResult::Joined(joined) => joined,
            JoinResult::MemberIdRequired { .. } => unreachable!(),
        };
        let second = match second.await.unwrap() {
            JoinResult::Joined(joined) => joined,
            JoinResult::MemberIdRequired { .. } => unreachable!(),
        };
        assert_eq!(leader.generation_id, first.generation_id + 1);
        assert_eq!(second.generation_id, leader.generation_id);
    }

    #[tokio::test(start_paused = true)]
    async fn follower_sync_waits_through_the_session_timeout_until_rebalance_deadline() {
        let coordinator = GroupCoordinator::new();
        let leader_id = claim_member(&coordinator).await;
        let mut leader_request = join_request(&leader_id);
        leader_request.session_timeout_ms = 1_000;
        leader_request.rebalance_timeout_ms = 10_000;
        let first = match coordinator
            .join(leader_request.clone(), true)
            .await
            .unwrap()
        {
            JoinResult::Joined(joined) => joined,
            JoinResult::MemberIdRequired { .. } => unreachable!(),
        };
        coordinator
            .sync(
                "orders",
                first.generation_id,
                &leader_id,
                vec![SyncAssignment {
                    member_id: leader_id.clone(),
                    assignment: Bytes::new(),
                }],
            )
            .await
            .unwrap();

        let mut second_handshake = join_request("");
        second_handshake.client_id = "consumer-b".to_owned();
        let second_id = match coordinator.join(second_handshake, true).await.unwrap() {
            JoinResult::MemberIdRequired { member_id } => member_id,
            JoinResult::Joined(_) => unreachable!(),
        };
        let mut second_request = join_request(&second_id);
        second_request.client_id = "consumer-b".to_owned();
        second_request.session_timeout_ms = 1_000;
        second_request.rebalance_timeout_ms = 10_000;
        let second_coordinator = coordinator.clone();
        let second =
            tokio::spawn(
                async move { second_coordinator.join(second_request, true).await.unwrap() },
            );
        yield_now().await;
        let leader = match coordinator.join(leader_request, true).await.unwrap() {
            JoinResult::Joined(joined) => joined,
            JoinResult::MemberIdRequired { .. } => unreachable!(),
        };
        let second = match second.await.unwrap() {
            JoinResult::Joined(joined) => joined,
            JoinResult::MemberIdRequired { .. } => unreachable!(),
        };

        let follower_coordinator = coordinator.clone();
        let follower_id = second.member_id.clone();
        let generation_id = leader.generation_id;
        let follower = tokio::spawn(async move {
            follower_coordinator
                .sync("orders", generation_id, &follower_id, Vec::new())
                .await
        });
        yield_now().await;
        advance(Duration::from_millis(1_500)).await;
        yield_now().await;
        assert!(!follower.is_finished());

        let leader_assignment = Bytes::from_static(b"leader-assignment");
        let follower_assignment = Bytes::from_static(b"follower-assignment");
        coordinator
            .sync(
                "orders",
                generation_id,
                &leader_id,
                vec![
                    SyncAssignment {
                        member_id: leader_id.clone(),
                        assignment: leader_assignment,
                    },
                    SyncAssignment {
                        member_id: second.member_id,
                        assignment: follower_assignment.clone(),
                    },
                ],
            )
            .await
            .unwrap();
        assert_eq!(follower.await.unwrap().unwrap(), follower_assignment);
    }

    #[tokio::test(start_paused = true)]
    async fn member_that_never_syncs_is_removed_at_the_rebalance_deadline() {
        let coordinator = GroupCoordinator::new();
        let (leader_id, _) = stabilize(&coordinator, "orders").await;
        let mut second_handshake = join_request("");
        second_handshake.client_id = "consumer-b".to_owned();
        let second_id = match coordinator.join(second_handshake, true).await.unwrap() {
            JoinResult::MemberIdRequired { member_id } => member_id,
            JoinResult::Joined(_) => unreachable!(),
        };
        let mut second_request = join_request(&second_id);
        second_request.client_id = "consumer-b".to_owned();
        second_request.rebalance_timeout_ms = 1_000;
        let second_coordinator = coordinator.clone();
        let second =
            tokio::spawn(
                async move { second_coordinator.join(second_request, true).await.unwrap() },
            );
        yield_now().await;
        let mut leader_request = join_request(&leader_id);
        leader_request.rebalance_timeout_ms = 1_000;
        let leader = match coordinator.join(leader_request, true).await.unwrap() {
            JoinResult::Joined(joined) => joined,
            JoinResult::MemberIdRequired { .. } => unreachable!(),
        };
        second.await.unwrap();
        coordinator
            .sync(
                "orders",
                leader.generation_id,
                &leader_id,
                vec![
                    SyncAssignment {
                        member_id: leader_id.clone(),
                        assignment: Bytes::new(),
                    },
                    SyncAssignment {
                        member_id: second_id,
                        assignment: Bytes::new(),
                    },
                ],
            )
            .await
            .unwrap();

        advance(Duration::from_millis(1_000)).await;
        assert_eq!(
            coordinator
                .heartbeat("orders", leader.generation_id, &leader_id)
                .await,
            Err(GroupError::RebalanceInProgress)
        );
        let snapshot = coordinator.snapshot("orders").await.unwrap();
        assert_eq!(snapshot.state, GroupState::PreparingRebalance);
        assert_eq!(snapshot.member_count, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn first_sync_after_the_rebalance_deadline_is_fenced() {
        let coordinator = GroupCoordinator::new();
        let member_id = claim_member(&coordinator).await;
        let mut request = join_request(&member_id);
        request.rebalance_timeout_ms = 1_000;
        let joined = match coordinator.join(request, true).await.unwrap() {
            JoinResult::Joined(joined) => joined,
            JoinResult::MemberIdRequired { .. } => unreachable!(),
        };

        advance(Duration::from_millis(1_000)).await;
        yield_now().await;

        assert_eq!(
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
                .await,
            Err(GroupError::UnknownMemberId)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn silent_stable_group_expires_without_follow_up_traffic() {
        let coordinator = GroupCoordinator::new();
        stabilize(&coordinator, "orders").await;

        advance(Duration::from_millis(10_000)).await;
        yield_now().await;

        let snapshot = coordinator.snapshot("orders").await.unwrap();
        assert_eq!(snapshot.state, GroupState::Empty);
        assert_eq!(snapshot.member_count, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn heartbeat_expires_only_the_silent_member_at_its_session_deadline() {
        let coordinator = GroupCoordinator::new();
        let (leader_id, generation_id) = stabilize(&coordinator, "orders").await;

        advance(Duration::from_millis(9_999)).await;
        assert_eq!(
            coordinator
                .heartbeat("orders", generation_id, &leader_id)
                .await,
            Ok(())
        );
        advance(Duration::from_millis(9_999)).await;
        assert_eq!(
            coordinator
                .heartbeat("orders", generation_id, &leader_id)
                .await,
            Ok(())
        );

        advance(Duration::from_millis(10_000)).await;
        assert_eq!(
            coordinator
                .heartbeat("orders", generation_id, &leader_id)
                .await,
            Err(GroupError::UnknownMemberId)
        );
        let snapshot = coordinator.snapshot("orders").await.unwrap();
        assert_eq!(snapshot.state, GroupState::Empty);
        assert_eq!(snapshot.member_count, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn survivor_heartbeat_expires_a_silent_peer_and_fences_the_old_generation() {
        let coordinator = GroupCoordinator::new();
        let (leader_id, _) = stabilize(&coordinator, "orders").await;
        let mut second_handshake = join_request("");
        second_handshake.client_id = "consumer-b".to_owned();
        let second_id = match coordinator.join(second_handshake, true).await.unwrap() {
            JoinResult::MemberIdRequired { member_id } => member_id,
            JoinResult::Joined(_) => unreachable!(),
        };
        let mut second_join = join_request(&second_id);
        second_join.client_id = "consumer-b".to_owned();
        let second_coordinator = coordinator.clone();
        let second =
            tokio::spawn(async move { second_coordinator.join(second_join, true).await.unwrap() });
        yield_now().await;
        let leader = match coordinator
            .join(join_request(&leader_id), true)
            .await
            .unwrap()
        {
            JoinResult::Joined(joined) => joined,
            JoinResult::MemberIdRequired { .. } => unreachable!(),
        };
        let second = match second.await.unwrap() {
            JoinResult::Joined(joined) => joined,
            JoinResult::MemberIdRequired { .. } => unreachable!(),
        };
        coordinator
            .sync(
                "orders",
                leader.generation_id,
                &leader_id,
                vec![
                    SyncAssignment {
                        member_id: leader_id.clone(),
                        assignment: Bytes::new(),
                    },
                    SyncAssignment {
                        member_id: second_id,
                        assignment: Bytes::new(),
                    },
                ],
            )
            .await
            .unwrap();
        coordinator
            .sync(
                "orders",
                leader.generation_id,
                &second.member_id,
                Vec::new(),
            )
            .await
            .unwrap();

        advance(Duration::from_millis(9_999)).await;
        assert_eq!(
            coordinator
                .heartbeat("orders", leader.generation_id, &leader_id)
                .await,
            Ok(())
        );
        advance(Duration::from_millis(1)).await;
        assert_eq!(
            coordinator
                .heartbeat("orders", leader.generation_id, &leader_id)
                .await,
            Err(GroupError::RebalanceInProgress)
        );
        let snapshot = coordinator.snapshot("orders").await.unwrap();
        assert_eq!(snapshot.state, GroupState::PreparingRebalance);
        assert_eq!(snapshot.member_count, 1);
        assert_eq!(snapshot.generation_id, leader.generation_id);

        let rejoined = match coordinator
            .join(join_request(&leader_id), true)
            .await
            .unwrap()
        {
            JoinResult::Joined(joined) => joined,
            JoinResult::MemberIdRequired { .. } => unreachable!(),
        };
        assert_eq!(rejoined.generation_id, leader.generation_id + 1);
        coordinator
            .sync(
                "orders",
                rejoined.generation_id,
                &leader_id,
                vec![SyncAssignment {
                    member_id: leader_id.clone(),
                    assignment: Bytes::new(),
                }],
            )
            .await
            .unwrap();
        assert_eq!(
            coordinator
                .heartbeat("orders", leader.generation_id, &leader_id)
                .await,
            Err(GroupError::IllegalGeneration)
        );
    }

    #[tokio::test]
    async fn offset_commit_is_rejected_while_a_new_member_waits_for_rebalance() {
        let coordinator = GroupCoordinator::new();
        let (leader_id, generation_id) = stabilize(&coordinator, "orders").await;
        let mut second_handshake = join_request("");
        second_handshake.client_id = "consumer-b".to_owned();
        let second_id = match coordinator.join(second_handshake, true).await.unwrap() {
            JoinResult::MemberIdRequired { member_id } => member_id,
            JoinResult::Joined(_) => unreachable!(),
        };
        let mut second_join = join_request(&second_id);
        second_join.client_id = "consumer-b".to_owned();
        let second_coordinator = coordinator.clone();
        let second =
            tokio::spawn(async move { second_coordinator.join(second_join, true).await.unwrap() });
        yield_now().await;

        assert_eq!(
            coordinator
                .commit_offsets(
                    "orders",
                    generation_id,
                    &leader_id,
                    vec![OffsetCommit {
                        topic: "events".to_owned(),
                        partition: 0,
                        offset: 1,
                        metadata: None,
                    }],
                )
                .await,
            Err(GroupError::RebalanceInProgress)
        );

        coordinator
            .join(join_request(&leader_id), true)
            .await
            .unwrap();
        second.await.unwrap();
    }
}
