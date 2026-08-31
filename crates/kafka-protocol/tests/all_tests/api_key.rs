use kafka_protocol::messages::ApiKey;

#[test]
fn api_key_iter() {
    assert_eq!(ApiKey::iter().count(), 89);
    assert_eq!(ApiKey::iter().next().unwrap(), ApiKey::Produce);
}
