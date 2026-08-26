package io.memkafka.acceptance;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
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

    private static String uniqueTopic(String prefix) {
        return prefix + "-" + UUID.randomUUID().toString().replace("-", "");
    }
}
