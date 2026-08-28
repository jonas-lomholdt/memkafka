package acceptance

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestMetadataAutoCreatesTwoPartitions(t *testing.T) {
	probeMode := apiVersionProbeEnabled(os.LookupEnv)
	var options []kgo.Opt
	if probeMode {
		options = append(options, kgo.MetadataMinAge(25*time.Millisecond))
	}
	client := kafkaClient(t, options...)
	admin := kadm.NewClient(client)
	topic := uniqueTopic("go-auto")

	details := autoCreatedTopicDetails(t, admin, topic, probeMode)
	assertPartitionCount(t, details, topic, 2)
}

func autoCreatedTopicDetails(
	t *testing.T,
	admin *kadm.Client,
	topic string,
	probeMode bool,
) kadm.TopicDetails {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for {
		details, err := admin.ListTopics(ctx, topic)
		if err != nil {
			t.Fatalf("load topic metadata: %v", err)
		}
		detail, exists := details[topic]
		if !probeMode || !exists || !errors.Is(detail.Err, kerr.UnknownTopicOrPartition) {
			return details
		}
		select {
		case <-time.After(25 * time.Millisecond):
		case <-ctx.Done():
			t.Fatalf("load auto-created topic metadata within five seconds: %v", ctx.Err())
		}
	}
}

func apiVersionProbeEnabled(lookup func(string) (string, bool)) bool {
	value, _ := lookup("MEMKAFKA_API_VERSION_PROBE")
	return strings.EqualFold(value, "true")
}

func TestAPIVersionProbeEnabledUsesLookup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value string
		set   bool
		want  bool
	}{
		{name: "missing", want: false},
		{name: "non-true", value: "1", set: true, want: false},
		{name: "case-insensitive true", value: "TrUe", set: true, want: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			lookup := func(name string) (string, bool) {
				if name != "MEMKAFKA_API_VERSION_PROBE" || !test.set {
					return "", false
				}
				return test.value, true
			}
			if got := apiVersionProbeEnabled(lookup); got != test.want {
				t.Fatalf("apiVersionProbeEnabled() = %t, want %t", got, test.want)
			}
		})
	}
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
	return kadm.NewClient(kafkaClient(t))
}

func kafkaClient(t *testing.T, options ...kgo.Opt) *kgo.Client {
	t.Helper()
	options = append(options,
		kgo.SeedBrokers(bootstrapServers()),
		kgo.AllowAutoTopicCreation(),
		kgo.RequestTimeoutOverhead(5*time.Second),
	)
	client, err := kgo.NewClient(options...)
	if err != nil {
		t.Fatalf("create Kafka client: %v", err)
	}
	t.Cleanup(client.Close)
	return client
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
