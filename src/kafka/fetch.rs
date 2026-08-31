use std::time::Duration;

use bytes::Bytes;
use kafka_protocol::{
    ResponseError,
    messages::{
        FetchRequest, FetchResponse,
        fetch_request::FetchPartition,
        fetch_response::{FetchableTopicResponse, PartitionData},
    },
};
use tokio::time::{Instant, sleep_until};

use crate::broker::{BrokerState, partition::FetchError};

pub(crate) async fn response(request: &FetchRequest, broker: &BrokerState) -> FetchResponse {
    let max_wait = u64::try_from(request.max_wait_ms).unwrap_or(0);
    let min_bytes = usize::try_from(request.min_bytes).unwrap_or(0);
    let deadline = Instant::now() + Duration::from_millis(max_wait);

    loop {
        let notified = broker.append_notification().notified_owned();
        tokio::pin!(notified);
        notified.as_mut().enable();

        let snapshot = snapshot(request, broker).await;
        if snapshot.record_bytes >= min_bytes
            || min_bytes == 0
            || max_wait == 0
            || snapshot.valid_partitions == 0
            || Instant::now() >= deadline
        {
            return snapshot.response;
        }

        tokio::select! {
            () = notified.as_mut() => {}
            () = async {
                // The event and `sleep_until` registration happen in one cooperative poll.
                tracing::trace!(
                    target: "memkafka::kafka::fetch::wait_registered",
                    max_wait_ms = request.max_wait_ms,
                    min_bytes = request.min_bytes,
                    "registered Kafka Fetch wait"
                );
                sleep_until(deadline).await;
            } => return snapshot.response,
        }
    }
}

struct FetchSnapshot {
    response: FetchResponse,
    record_bytes: usize,
    valid_partitions: usize,
}

async fn snapshot(request: &FetchRequest, broker: &BrokerState) -> FetchSnapshot {
    let max_bytes = usize::try_from(request.max_bytes).unwrap_or(0);
    let mut record_bytes = 0_usize;
    let mut valid_partitions = 0_usize;
    let mut responses = Vec::with_capacity(request.topics.len());

    for topic in &request.topics {
        let mut partitions = Vec::with_capacity(topic.partitions.len());
        for partition in &topic.partitions {
            let remaining = max_bytes.saturating_sub(record_bytes);
            let partition_max = usize::try_from(partition.partition_max_bytes).unwrap_or(0);
            let mut response = partition_response(
                topic.topic.as_str(),
                partition,
                partition_max.min(remaining),
                broker,
            )
            .await;

            if response.error_code == 0 {
                valid_partitions += 1;
            }
            if let Some(records) = response.records.as_mut() {
                let exceeds_request_limit = record_bytes > 0
                    && record_bytes
                        .checked_add(records.len())
                        .is_none_or(|size| size > max_bytes);
                if exceeds_request_limit {
                    *records = Bytes::new();
                } else {
                    record_bytes = record_bytes.saturating_add(records.len());
                }
            }
            partitions.push(response);
        }
        responses.push(
            FetchableTopicResponse::default()
                .with_topic(topic.topic.clone())
                .with_partitions(partitions),
        );
    }

    FetchSnapshot {
        response: FetchResponse::default()
            .with_throttle_time_ms(0)
            .with_responses(responses),
        record_bytes,
        valid_partitions,
    }
}

async fn partition_response(
    topic: &str,
    request: &FetchPartition,
    max_bytes: usize,
    broker: &BrokerState,
) -> PartitionData {
    let Some(log) = broker.topics().partition(topic, request.partition).await else {
        return error_partition(request.partition, ResponseError::UnknownTopicOrPartition);
    };

    match log.fetch(request.fetch_offset, max_bytes).await {
        Ok(snapshot) => PartitionData::default()
            .with_partition_index(request.partition)
            .with_high_watermark(snapshot.high_watermark)
            .with_last_stable_offset(snapshot.high_watermark)
            .with_aborted_transactions(Some(Vec::new()))
            .with_records(Some(snapshot.records)),
        Err(FetchError::OutOfRange) => {
            error_partition(request.partition, ResponseError::OffsetOutOfRange)
        }
    }
}

fn error_partition(index: i32, error: ResponseError) -> PartitionData {
    PartitionData::default()
        .with_partition_index(index)
        .with_error_code(error.code())
        .with_high_watermark(-1)
        .with_last_stable_offset(-1)
        .with_aborted_transactions(Some(Vec::new()))
        .with_records(Some(Bytes::new()))
}

#[cfg(test)]
mod tests {
    use std::{io::ErrorKind, num::NonZeroU32, sync::mpsc as std_mpsc, thread, time::Duration};

    use bytes::{Bytes, BytesMut};
    use kafka_protocol::{
        messages::{
            ApiKey, BrokerId, FetchRequest, ProduceRequest, RequestHeader, RequestKind,
            ResponseHeader, ResponseKind, TopicName,
            fetch_request::{FetchPartition, FetchTopic},
            fetch_response::FetchResponse,
            produce_request::{PartitionProduceData, TopicProduceData},
        },
        protocol::{Decodable, StrBytes, encode_request_header_into_buffer},
        records::{
            Compression, NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH, NO_PRODUCER_ID, Record,
            RecordBatchDecoder, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
        },
    };
    use tokio::{
        net::{TcpListener, TcpStream},
        sync::{mpsc, oneshot, watch},
        task::JoinHandle,
        time::{advance, timeout},
    };
    use tracing::{Event, Subscriber, instrument::WithSubscriber};
    use tracing_subscriber::{
        Layer,
        layer::{Context, SubscriberExt},
    };

    use crate::{
        broker::BrokerState,
        config::AdvertisedAddress,
        kafka::{
            connection,
            dispatcher::Dispatcher,
            frame::{read_frame, write_frame},
            produce,
        },
    };

    const FETCH_VERSION: i16 = 4;
    const FETCH_WAIT_REGISTRATION_TARGET: &str = "memkafka::kafka::fetch::wait_registered";

    #[tokio::test(start_paused = true)]
    async fn fetch_v4_waits_for_min_bytes_and_wakes_after_appends() {
        let broker = test_broker_state();
        broker
            .topics()
            .create_explicit("events", 1, 1)
            .await
            .expect("create topic");
        let dispatcher = Dispatcher::new(broker.clone());
        let first = record_batch(&["first"]);
        let second = record_batch(&["second"]);
        let min_bytes = i32::try_from(first.len() + second.len()).expect("small batches");
        let mut fetch = ObservedFetchConnection::start(&dispatcher).await;
        fetch
            .send(&encode_fetch_request(164, 1_000, min_bytes))
            .await;
        fetch.wait_until_registered("initial Fetch wait").await;
        fetch.assert_no_response_bytes("before minimum bytes are available");

        append_records(&broker, first).await;
        fetch
            .wait_until_registered("Fetch wait after the first append")
            .await;
        fetch.assert_no_response_bytes("after only the first batch is available");

        append_records(&broker, second).await;
        let (header, response) = decode_fetch_response(
            fetch
                .receive("after the second append satisfied Fetch min_bytes")
                .await,
        );
        assert_eq!(header.correlation_id, 164);
        assert_eq!(
            decode_records(
                response.responses[0].partitions[0]
                    .records
                    .clone()
                    .expect("record bytes")
            )
            .len(),
            2
        );
        fetch.shutdown().await;
    }

    #[tokio::test(start_paused = true)]
    async fn fetch_v4_returns_empty_when_max_wait_expires() {
        let broker = test_broker_state();
        broker
            .topics()
            .create_explicit("events", 1, 1)
            .await
            .expect("create topic");
        let dispatcher = Dispatcher::new(broker);
        let mut fetch = ObservedFetchConnection::start(&dispatcher).await;
        fetch.send(&encode_fetch_request(167, 100, 1)).await;
        fetch.wait_until_registered("initial Fetch wait").await;
        fetch.assert_no_response_bytes("before the max-wait deadline");

        advance(Duration::from_millis(99)).await;
        fetch.assert_no_response_bytes("one millisecond before the max-wait deadline");
        advance(Duration::from_millis(1)).await;

        let (header, response) = decode_fetch_response(
            fetch
                .receive("after the 100 ms Fetch max-wait deadline elapsed")
                .await,
        );
        assert_eq!(header.correlation_id, 167);
        let partition = &response.responses[0].partitions[0];
        assert_eq!(partition.error_code, 0);
        assert!(partition.records.as_ref().is_none_or(Bytes::is_empty));
        fetch.shutdown().await;
    }

    #[tokio::test(start_paused = true)]
    #[should_panic(
        expected = "Fetch response timed out in real time while the server kept the socket open without a request"
    )]
    async fn observed_fetch_receive_times_out_in_real_time_while_socket_stays_open() {
        let dispatcher = Dispatcher::new(test_broker_state());
        let mut fetch = ObservedFetchConnection::start(&dispatcher).await;
        let (_outer_watchdog, outer_expired) = RealTimeWatchdog::start(Duration::from_secs(2));

        tokio::select! {
            response = fetch.receive(
                "while the server kept the socket open without a request"
            ) => panic!("silent Fetch connection returned {} response bytes", response.len()),
            _ = outer_expired => panic!(
                "outer watchdog observed an unbounded silent Fetch response wait"
            ),
        }
    }

    struct ObservedFetchConnection {
        client: TcpStream,
        registrations: mpsc::UnboundedReceiver<()>,
        shutdown: watch::Sender<bool>,
        server: Option<JoinHandle<anyhow::Result<()>>>,
    }

    impl ObservedFetchConnection {
        async fn start(dispatcher: &Dispatcher) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind loopback Kafka test listener");
            let address = listener
                .local_addr()
                .expect("read loopback listener address");
            let (shutdown_tx, shutdown_rx) = watch::channel(false);
            let (registration_tx, registration_rx) = mpsc::unbounded_channel();
            let dispatcher = dispatcher.clone();
            let server_future = async move {
                let (socket, _) = listener
                    .accept()
                    .await
                    .expect("accept loopback Kafka client");
                connection::serve(socket, dispatcher, shutdown_rx).await
            };
            let subscriber = tracing_subscriber::registry().with(FetchWaitRegistrationLayer {
                registrations: registration_tx,
            });
            let server = tokio::spawn(server_future.with_subscriber(subscriber));
            let client = TcpStream::connect(address)
                .await
                .expect("connect loopback Kafka test client");

            Self {
                client,
                registrations: registration_rx,
                shutdown: shutdown_tx,
                server: Some(server),
            }
        }

        async fn send(&mut self, request: &Bytes) {
            write_frame(&mut self.client, request)
                .await
                .expect("write loopback Fetch request");
        }

        async fn wait_until_registered(&mut self, context: &str) {
            let (_watchdog, expired) = RealTimeWatchdog::start(Duration::from_secs(1));
            tokio::select! {
                registration = self.registrations.recv() => match registration {
                    Some(()) => {}
                    None => panic!("Fetch wait-registration signal channel closed during {context}"),
                },
                _ = expired => panic!("Fetch wait-registration signal timed out during {context}"),
            }
        }

        fn assert_no_response_bytes(&self, context: &str) {
            let mut byte = [0_u8; 1];
            match self.client.try_read(&mut byte) {
                Err(error) if error.kind() == ErrorKind::WouldBlock => {}
                Err(error) => panic!("failed to inspect Kafka response {context}: {error}"),
                Ok(0) => panic!("Kafka connection closed {context}"),
                Ok(count) => {
                    panic!("received {count} unexpected Kafka response byte(s) {context}")
                }
            }
        }

        async fn receive(&mut self, context: &str) -> Bytes {
            let (_watchdog, expired) = RealTimeWatchdog::start(Duration::from_secs(1));
            tokio::select! {
                response = read_frame(&mut self.client) => response
                    .expect("read loopback Fetch response")
                    .expect("loopback Kafka connection closed before its Fetch response"),
                _ = expired => panic!("Fetch response timed out in real time {context}"),
            }
        }

        async fn shutdown(&mut self) {
            self.shutdown
                .send(true)
                .expect("stop loopback Kafka connection");
            let result = timeout(
                Duration::from_secs(1),
                self.server
                    .as_mut()
                    .expect("loopback Kafka server must be running"),
            )
            .await
            .expect("loopback Kafka task did not stop")
            .expect("loopback Kafka task panicked");
            self.server.take();
            result.expect("loopback Kafka connection failed");
        }
    }

    impl Drop for ObservedFetchConnection {
        fn drop(&mut self) {
            if let Some(server) = &self.server {
                server.abort();
            }
        }
    }

    struct FetchWaitRegistrationLayer {
        registrations: mpsc::UnboundedSender<()>,
    }

    impl<S> Layer<S> for FetchWaitRegistrationLayer
    where
        S: Subscriber,
    {
        fn on_event(&self, event: &Event<'_>, _context: Context<'_, S>) {
            if event.metadata().target() == FETCH_WAIT_REGISTRATION_TARGET {
                let _ = self.registrations.send(());
            }
        }
    }

    struct RealTimeWatchdog {
        cancel: Option<std_mpsc::Sender<()>>,
        thread: Option<thread::JoinHandle<()>>,
    }

    impl RealTimeWatchdog {
        fn start(duration: Duration) -> (Self, oneshot::Receiver<()>) {
            let (cancel_tx, cancel_rx) = std_mpsc::channel();
            let (expired_tx, expired_rx) = oneshot::channel();
            let thread = thread::spawn(move || {
                if cancel_rx.recv_timeout(duration).is_err() {
                    let _ = expired_tx.send(());
                }
            });
            (
                Self {
                    cancel: Some(cancel_tx),
                    thread: Some(thread),
                },
                expired_rx,
            )
        }
    }

    impl Drop for RealTimeWatchdog {
        fn drop(&mut self) {
            if let Some(cancel) = self.cancel.take() {
                let _ = cancel.send(());
            }
            if let Some(thread) = self.thread.take() {
                thread.join().expect("Fetch registration watchdog panicked");
            }
        }
    }

    fn test_broker_state() -> BrokerState {
        BrokerState::new(
            1,
            AdvertisedAddress::new("127.0.0.1", 9092).expect("valid test address"),
            false,
            false,
            NonZeroU32::new(1).expect("nonzero literal"),
        )
    }

    fn encode_fetch_request(correlation_id: i32, max_wait_ms: i32, min_bytes: i32) -> Bytes {
        let header = RequestHeader::default()
            .with_request_api_key(ApiKey::Fetch as i16)
            .with_request_api_version(FETCH_VERSION)
            .with_correlation_id(correlation_id)
            .with_client_id(Some(StrBytes::from_static_str("fetch-timing-test")));
        let request = FetchRequest::default()
            .with_replica_id(BrokerId::from(-1))
            .with_max_wait_ms(max_wait_ms)
            .with_min_bytes(min_bytes)
            .with_max_bytes(i32::MAX)
            .with_isolation_level(0)
            .with_topics(vec![
                FetchTopic::default()
                    .with_topic(TopicName::from(StrBytes::from_static_str("events")))
                    .with_partitions(vec![
                        FetchPartition::default()
                            .with_partition(0)
                            .with_current_leader_epoch(-1)
                            .with_fetch_offset(0)
                            .with_log_start_offset(0)
                            .with_partition_max_bytes(i32::MAX),
                    ]),
            ]);
        let mut encoded = BytesMut::new();
        encode_request_header_into_buffer(&mut encoded, &header).expect("encode request header");
        RequestKind::Fetch(request)
            .encode(&mut encoded, FETCH_VERSION)
            .expect("encode Fetch body");
        encoded.freeze()
    }

    async fn append_records(broker: &BrokerState, records: Bytes) {
        let request = ProduceRequest::default()
            .with_acks(-1)
            .with_timeout_ms(1_000)
            .with_topic_data(vec![
                TopicProduceData::default()
                    .with_name(TopicName::from(StrBytes::from_static_str("events")))
                    .with_partition_data(vec![
                        PartitionProduceData::default()
                            .with_index(0)
                            .with_records(Some(records)),
                    ]),
            ]);
        let response = produce::response(&request, broker).await;
        assert_eq!(response.responses[0].partition_responses[0].error_code, 0);
    }

    fn decode_fetch_response(mut encoded: Bytes) -> (ResponseHeader, FetchResponse) {
        let header = ResponseHeader::decode(
            &mut encoded,
            ApiKey::Fetch.response_header_version(FETCH_VERSION),
        )
        .expect("decode response header");
        let response = ResponseKind::decode(ApiKey::Fetch, &mut encoded, FETCH_VERSION)
            .expect("decode Fetch response body");
        let ResponseKind::Fetch(response) = response else {
            panic!("expected Fetch response");
        };
        assert!(encoded.is_empty(), "response has trailing bytes");
        (header, response)
    }

    fn record_batch(values: &[&str]) -> Bytes {
        let records = values
            .iter()
            .enumerate()
            .map(|(offset, value)| Record {
                transactional: false,
                control: false,
                delete_horizon: false,
                partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
                producer_id: NO_PRODUCER_ID,
                producer_epoch: NO_PRODUCER_EPOCH,
                timestamp_type: TimestampType::Creation,
                offset: offset as i64,
                sequence: offset as i32,
                timestamp: 1_700_000_000_000 + offset as i64,
                key: Some(Bytes::from(format!("key-{offset}"))),
                value: Some(Bytes::copy_from_slice(value.as_bytes())),
                headers: [(
                    StrBytes::from_static_str("source"),
                    Some(Bytes::from_static(b"fetch-timing-test")),
                )]
                .into_iter()
                .collect(),
            })
            .collect::<Vec<_>>();
        let mut encoded = BytesMut::new();
        RecordBatchEncoder::encode(
            &mut encoded,
            &records,
            &RecordEncodeOptions {
                version: 2,
                compression: Compression::None,
            },
        )
        .expect("encode RecordBatch");
        encoded.freeze()
    }

    fn decode_records(mut encoded: Bytes) -> Vec<Record> {
        RecordBatchDecoder::decode_all(&mut encoded)
            .expect("decode RecordBatches")
            .into_iter()
            .flat_map(|batch| batch.records)
            .collect()
    }
}
