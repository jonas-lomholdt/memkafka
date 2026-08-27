package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	bootstrapServers := requiredEnvironment("MEMKAFKA_BOOTSTRAP_SERVERS")
	topic := requiredEnvironment("MEMKAFKA_KAFBAT_TOPIC")
	key := requiredEnvironment("MEMKAFKA_KAFBAT_KEY")
	value := requiredEnvironment("MEMKAFKA_KAFBAT_VALUE")
	groupID := requiredEnvironment("MEMKAFKA_KAFBAT_GROUP")

	client, err := kgo.NewClient(
		kgo.SeedBrokers(bootstrapServers),
		kgo.DisableIdempotentWrite(),
		kgo.ConsumerGroup(groupID),
		kgo.ConsumeTopics(topic),
		kgo.SessionTimeout(60*time.Second),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.MaxProduceRequestsInflightPerBroker(1),
		kgo.RequestTimeoutOverhead(5*time.Second),
	)
	if err != nil {
		panic(fmt.Errorf("create Kafka client: %w", err))
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := kadm.NewClient(client).CreateTopic(ctx, 1, 1, nil, topic); err != nil {
		panic(fmt.Errorf("create probe topic: %w", err))
	}
	record, err := client.ProduceSync(ctx, &kgo.Record{
		Topic:     topic,
		Partition: 0,
		Key:       []byte(key),
		Value:     []byte(value),
	}).First()
	if err != nil {
		panic(fmt.Errorf("produce probe record: %w", err))
	}
	if record.Offset != 0 {
		panic(fmt.Errorf("probe record offset = %d, expected 0", record.Offset))
	}
	fmt.Printf("seeded %s[0]@0\n", topic)

	for {
		fetches := client.PollRecords(context.Background(), 10)
		if errs := fetches.Errors(); len(errs) != 0 {
			panic(fmt.Errorf("group poll: %v", errs))
		}
		for _, consumed := range fetches.Records() {
			if consumed.Topic == topic && string(consumed.Key) == key && string(consumed.Value) == value {
				fmt.Printf("group active %s\n", groupID)
				for {
					if errs := client.PollRecords(context.Background(), 10).Errors(); len(errs) != 0 {
						panic(fmt.Errorf("group heartbeat poll: %v", errs))
					}
				}
			}
		}
	}
}

func requiredEnvironment(name string) string {
	value := os.Getenv(name)
	if value == "" {
		panic(fmt.Errorf("%s is required", name))
	}
	return value
}
