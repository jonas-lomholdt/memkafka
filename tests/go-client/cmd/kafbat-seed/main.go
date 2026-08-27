package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
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
	registryURL := requiredEnvironment("MEMKAFKA_SCHEMA_REGISTRY_URL")
	avroTopic := requiredEnvironment("MEMKAFKA_KAFBAT_AVRO_TOPIC")
	avroValue := requiredEnvironment("MEMKAFKA_KAFBAT_AVRO_VALUE")

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
	for _, topicName := range []string{topic, avroTopic} {
		if _, err := kadm.NewClient(client).CreateTopic(ctx, 1, 1, nil, topicName); err != nil {
			panic(fmt.Errorf("create probe topic %s: %w", topicName, err))
		}
	}
	schemaID, err := registerAvroSchema(ctx, registryURL, avroTopic+"-value")
	if err != nil {
		panic(err)
	}
	results := client.ProduceSync(ctx,
		&kgo.Record{
			Topic:     topic,
			Partition: 0,
			Key:       []byte(key),
			Value:     []byte(value),
		},
		&kgo.Record{
			Topic:     avroTopic,
			Partition: 0,
			Key:       []byte(key),
			Value:     encodeAvroRecord(schemaID, avroValue),
		},
	)
	for _, result := range results {
		if result.Err != nil {
			panic(fmt.Errorf("produce probe record to %s: %w", result.Record.Topic, result.Err))
		}
		if result.Record.Offset != 0 {
			panic(fmt.Errorf(
				"probe record %s offset = %d, expected 0",
				result.Record.Topic,
				result.Record.Offset,
			))
		}
	}
	fmt.Printf("seeded %s[0]@0\n", topic)
	fmt.Printf("seeded Avro %s[0]@0 schema=%d\n", avroTopic, schemaID)

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

const avroSchema = `{"type":"record","name":"KafbatProbe","fields":[{"name":"message","type":"string"}]}`

func registerAvroSchema(ctx context.Context, registryURL, subject string) (int, error) {
	payload, err := json.Marshal(struct {
		Schema string `json:"schema"`
	}{Schema: avroSchema})
	if err != nil {
		return 0, fmt.Errorf("encode Avro schema registration: %w", err)
	}
	endpoint := fmt.Sprintf(
		"%s/subjects/%s/versions",
		strings.TrimRight(registryURL, "/"),
		url.PathEscape(subject),
	)
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return 0, fmt.Errorf("create Avro schema registration request: %w", err)
	}
	request.Header.Set("Content-Type", "application/vnd.schemaregistry.v1+json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return 0, fmt.Errorf("register Avro schema: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(response.Body)
		return 0, fmt.Errorf("register Avro schema: HTTP %d: %s", response.StatusCode, body)
	}
	var registered struct {
		ID int `json:"id"`
	}
	if err := json.NewDecoder(response.Body).Decode(&registered); err != nil {
		return 0, fmt.Errorf("decode Avro schema registration: %w", err)
	}
	if registered.ID <= 0 {
		return 0, fmt.Errorf("register Avro schema: invalid ID %d", registered.ID)
	}
	return registered.ID, nil
}

func encodeAvroRecord(schemaID int, value string) []byte {
	payload := make([]byte, 5, 5+binary.MaxVarintLen64+len(value))
	binary.BigEndian.PutUint32(payload[1:5], uint32(schemaID))
	payload = binary.AppendUvarint(payload, uint64(len(value))<<1)
	return append(payload, value...)
}

func requiredEnvironment(name string) string {
	value := os.Getenv(name)
	if value == "" {
		panic(fmt.Errorf("%s is required", name))
	}
	return value
}
