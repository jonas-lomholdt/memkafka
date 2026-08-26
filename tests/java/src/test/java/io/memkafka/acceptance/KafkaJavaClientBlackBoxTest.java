package io.memkafka.acceptance;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

final class KafkaJavaClientBlackBoxTest {
    private static final String BOOTSTRAP_SERVERS = System.getenv().getOrDefault(
            "MEMKAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092");

    @Test
    void producerMetadataAutoCreatesTwoPartitions() {
        var topic = uniqueTopic("java-auto");

        try (var producer = new KafkaProducer<byte[], byte[]>(producerConfiguration())) {
            assertEquals(2, producer.partitionsFor(topic).size());
        }
    }

    @Test
    void adminCreatesSixPartitionTopic() throws Exception {
        var topic = uniqueTopic("java-explicit");

        try (var admin = Admin.create(adminConfiguration())) {
            admin.createTopics(List.of(new NewTopic(topic, 6, (short) 1)))
                    .all()
                    .get(5, SECONDS);

            var description = admin.describeTopics(List.of(topic))
                    .allTopicNames()
                    .get(5, SECONDS)
                    .get(topic);
            assertEquals(6, description.partitions().size());
        }
    }

    @Test
    void adminRejectsReplicationFactorTwo() {
        var topic = uniqueTopic("java-invalid-rf");

        try (var admin = Admin.create(adminConfiguration())) {
            var failure = assertThrows(ExecutionException.class, () ->
                    admin.createTopics(List.of(new NewTopic(topic, 2, (short) 2)))
                            .all()
                            .get(5, SECONDS));
            assertInstanceOf(InvalidReplicationFactorException.class, failure.getCause());
        }
    }

    @Test
    void publishesAndConsumesInOrderThenReadsUncommittedRecordsAgain() throws Exception {
        var topic = uniqueTopic("java-delivery");
        try (var admin = Admin.create(adminConfiguration())) {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1)))
                    .all()
                    .get(5, SECONDS);
        }

        try (var producer = new KafkaProducer<String, String>(deliveryProducerConfiguration())) {
            for (var index = 0; index < 10; index++) {
                var metadata = producer.send(new ProducerRecord<>(
                                topic, 0, "key-" + index, "message-" + index))
                        .get(5, SECONDS);
                assertEquals(index, metadata.offset());
            }
        }

        var partition = new TopicPartition(topic, 0);
        try (var consumer = new KafkaConsumer<String, String>(deliveryConsumerConfiguration())) {
            consumer.assign(List.of(partition));
            consumer.seekToBeginning(List.of(partition));
            assertSequence(consumeExactly(consumer, 10), topic);

            consumer.seek(partition, 0L);
            assertSequence(consumeExactly(consumer, 10), topic);
        }
    }

    private static Map<String, Object> adminConfiguration() {
        return Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS,
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5_000,
                AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 5_000);
    }

    private static Map<String, Object> producerConfiguration() {
        return Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class,
                ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5_000,
                ProducerConfig.MAX_BLOCK_MS_CONFIG, 5_000);
    }

    private static Map<String, Object> deliveryProducerConfiguration() {
        return Map.ofEntries(
                Map.entry(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS),
                Map.entry(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class),
                Map.entry(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class),
                Map.entry(ProducerConfig.ACKS_CONFIG, "all"),
                Map.entry(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false),
                Map.entry(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1),
                Map.entry(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5_000),
                Map.entry(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 10_000),
                Map.entry(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5_000));
    }

    private static Map<String, Object> deliveryConsumerConfiguration() {
        return Map.ofEntries(
                Map.entry(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS),
                Map.entry(ConsumerConfig.GROUP_ID_CONFIG, uniqueTopic("java-direct")),
                Map.entry(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class),
                Map.entry(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class),
                Map.entry(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false),
                Map.entry(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"),
                Map.entry(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false),
                Map.entry(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 5_000));
    }

    private static List<ConsumerRecord<String, String>> consumeExactly(
            KafkaConsumer<String, String> consumer, int count) {
        var records = new ArrayList<ConsumerRecord<String, String>>(count);
        var deadline = System.nanoTime() + SECONDS.toNanos(5);
        while (records.size() < count && System.nanoTime() < deadline) {
            consumer.poll(Duration.ofMillis(250)).forEach(records::add);
        }
        assertEquals(count, records.size(), "consumer did not receive every record before timeout");
        return records;
    }

    private static void assertSequence(List<ConsumerRecord<String, String>> records, String topic) {
        for (var index = 0; index < records.size(); index++) {
            var record = records.get(index);
            assertEquals(topic, record.topic());
            assertEquals(0, record.partition());
            assertEquals(index, record.offset());
            assertEquals("key-" + index, record.key());
            assertEquals("message-" + index, record.value());
        }
    }

    private static String uniqueTopic(String prefix) {
        return prefix + "-" + UUID.randomUUID().toString().replace("-", "");
    }
}
