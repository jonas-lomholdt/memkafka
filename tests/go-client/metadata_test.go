package acceptance

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestMetadataAutoCreatesTwoPartitions(t *testing.T) {
	admin := adminClient(t)
	topic := uniqueTopic("go-auto")

	details, err := admin.ListTopics(testContext(t), topic)
	if err != nil {
		t.Fatalf("load topic metadata: %v", err)
	}
	assertPartitionCount(t, details, topic, 2)
}

func TestAdminCreatesSixPartitionTopic(t *testing.T) {
	admin := adminClient(t)
	topic := uniqueTopic("go-explicit")

	if _, err := admin.CreateTopic(testContext(t), 6, 1, nil, topic); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	details, err := admin.ListTopics(testContext(t), topic)
	if err != nil {
		t.Fatalf("load created topic metadata: %v", err)
	}
	assertPartitionCount(t, details, topic, 6)
}

func TestAdminRejectsReplicationFactorTwo(t *testing.T) {
	admin := adminClient(t)
	topic := uniqueTopic("go-invalid-rf")

	_, err := admin.CreateTopic(testContext(t), 2, 2, nil, topic)
	if !errors.Is(err, kerr.InvalidReplicationFactor) {
		t.Fatalf("expected InvalidReplicationFactor, got %v", err)
	}
}

func TestPublishConsumeOrderAndUncommittedRedelivery(t *testing.T) {
	admin := adminClient(t)
	topic := uniqueTopic("go-delivery")
	if _, err := admin.CreateTopic(testContext(t), 1, 1, nil, topic); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	producer, err := kgo.NewClient(
		kgo.SeedBrokers(bootstrapServers()),
		kgo.DisableIdempotentWrite(),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.MaxProduceRequestsInflightPerBroker(1),
		kgo.RequestTimeoutOverhead(5*time.Second),
	)
	if err != nil {
		t.Fatalf("create producer: %v", err)
	}
	for index := 0; index < 10; index++ {
		record, produceErr := producer.ProduceSync(testContext(t), &kgo.Record{
			Topic:     topic,
			Partition: 0,
			Key:       []byte(fmt.Sprintf("key-%d", index)),
			Value:     []byte(fmt.Sprintf("message-%d", index)),
			Headers: []kgo.RecordHeader{{
				Key:   "source",
				Value: []byte("go-test"),
			}},
		}).First()
		if produceErr != nil {
			producer.Close()
			t.Fatalf("produce record %d: %v", index, produceErr)
		}
		if record.Offset != int64(index) {
			producer.Close()
			t.Fatalf("record %d received offset %d", index, record.Offset)
		}
	}
	producer.Close()

	first := consumeDirect(t, topic, kgo.NewOffset().AtStart())
	assertRecordSequence(t, first, topic)

	repeated := consumeDirect(t, topic, kgo.NewOffset().At(0))
	assertRecordSequence(t, repeated, topic)
}

func adminClient(t *testing.T) *kadm.Client {
	t.Helper()
	client, err := kgo.NewClient(
		kgo.SeedBrokers(bootstrapServers()),
		kgo.AllowAutoTopicCreation(),
		kgo.RequestTimeoutOverhead(5*time.Second),
	)
	if err != nil {
		t.Fatalf("create Kafka client: %v", err)
	}
	t.Cleanup(client.Close)
	return kadm.NewClient(client)
}

func assertPartitionCount(t *testing.T, details kadm.TopicDetails, topic string, expected int) {
	t.Helper()
	detail, exists := details[topic]
	if !exists {
		t.Fatalf("metadata omitted topic %q", topic)
	}
	if detail.Err != nil {
		t.Fatalf("metadata for %q failed: %v", topic, detail.Err)
	}
	if len(detail.Partitions) != expected {
		t.Fatalf("topic %q has %d partitions, expected %d", topic, len(detail.Partitions), expected)
	}
}

func consumeDirect(t *testing.T, topic string, offset kgo.Offset) []*kgo.Record {
	t.Helper()
	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(bootstrapServers()),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			topic: {0: offset},
		}),
		kgo.RequestTimeoutOverhead(5*time.Second),
	)
	if err != nil {
		t.Fatalf("create direct consumer: %v", err)
	}
	defer consumer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	records := make([]*kgo.Record, 0, 10)
	for len(records) < 10 && ctx.Err() == nil {
		fetches := consumer.PollRecords(ctx, 10-len(records))
		if errors := fetches.Errors(); len(errors) > 0 {
			t.Fatalf("fetch records: %v", errors)
		}
		records = append(records, fetches.Records()...)
	}
	if len(records) != 10 {
		t.Fatalf("consumed %d records, expected 10: %v", len(records), ctx.Err())
	}
	return records
}

func assertRecordSequence(t *testing.T, records []*kgo.Record, topic string) {
	t.Helper()
	for index, record := range records {
		if record.Topic != topic || record.Partition != 0 || record.Offset != int64(index) {
			t.Fatalf(
				"record %d position = %s[%d]@%d",
				index,
				record.Topic,
				record.Partition,
				record.Offset,
			)
		}
		if string(record.Key) != fmt.Sprintf("key-%d", index) {
			t.Fatalf("record %d key = %q", index, record.Key)
		}
		if string(record.Value) != fmt.Sprintf("message-%d", index) {
			t.Fatalf("record %d value = %q", index, record.Value)
		}
		if len(record.Headers) != 1 ||
			record.Headers[0].Key != "source" ||
			string(record.Headers[0].Value) != "go-test" {
			t.Fatalf("record %d headers = %#v", index, record.Headers)
		}
	}
}

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func bootstrapServers() string {
	if value := os.Getenv("MEMKAFKA_BOOTSTRAP_SERVERS"); value != "" {
		return value
	}
	return "127.0.0.1:9092"
}

func uniqueTopic(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
}
