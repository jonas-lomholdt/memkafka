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
