use bytes::{Bytes, BytesMut};
use kafka_protocol::{
    messages::{
        ApiKey, InitProducerIdRequest, InitProducerIdResponse, ListOffsetsRequest,
        ListOffsetsResponse, OffsetCommitRequest, OffsetCommitResponse, OffsetFetchRequest,
        OffsetFetchResponse,
    },
    protocol::{Decodable, Encodable, Message},
};

#[test]
fn kafka_4_3_ranges_are_generated() {
    assert_eq!(ApiKey::ListOffsets.valid_versions().max, 11);
    assert_eq!(ListOffsetsRequest::VERSIONS.max, 11);
    assert_eq!(OffsetCommitRequest::VERSIONS.max, 10);
    assert_eq!(OffsetFetchRequest::VERSIONS.max, 10);
    assert_eq!(InitProducerIdRequest::VERSIONS.max, 6);

    assert_eq!(ListOffsetsResponse::VERSIONS.max, 11);
    assert_eq!(OffsetCommitResponse::VERSIONS.max, 10);
    assert_eq!(OffsetFetchResponse::VERSIONS.max, 10);
    assert_eq!(InitProducerIdResponse::VERSIONS.max, 6);
}

#[test]
fn list_offsets_v11_request_round_trips() {
    let request = ListOffsetsRequest::default().with_timeout_ms(1_234);
    let mut encoded = BytesMut::new();

    request.encode(&mut encoded, 11).unwrap();
    let decoded = ListOffsetsRequest::decode(&mut Bytes::from(encoded), 11).unwrap();

    assert_eq!(decoded, request);
    assert_eq!(decoded.timeout_ms, 1_234);
}
