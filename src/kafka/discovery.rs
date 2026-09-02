pub(crate) const CLUSTER_ID: &str = "memkafka";
pub(crate) const TOPIC_AUTHORIZED_OPERATIONS: i32 = 3576;
pub(crate) const CLUSTER_AUTHORIZED_OPERATIONS: i32 = 8096;

pub(crate) const fn optional_authorized_operations(include: bool, value: i32) -> i32 {
    if include { value } else { i32::MIN }
}
