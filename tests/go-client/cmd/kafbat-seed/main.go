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
	configuration, err := configurationFromEnvironment(os.LookupEnv)
	if err != nil {
		panic(err)
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(configuration.bootstrapServers),
		kgo.DisableIdempotentWrite(),
		kgo.ConsumerGroup(configuration.groupID),
		kgo.ConsumeTopics(configuration.topic),
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
	topics := []string{configuration.topic}
	if !configuration.stringOnly {
		topics = append(topics, configuration.avroTopic)
	}
	for _, topicName := range topics {
		if _, err := kadm.NewClient(client).CreateTopic(ctx, 1, 1, nil, topicName); err != nil {
			panic(fmt.Errorf("create probe topic %s: %w", topicName, err))
		}
	}
	records := []*kgo.Record{
		{
			Topic:     configuration.topic,
			Partition: 0,
			Key:       []byte(configuration.key),
			Value:     []byte(configuration.value),
		},
	}
	var schemaID int
	if !configuration.stringOnly {
		schemaID, err = registerAvroSchema(
			ctx,
			configuration.registryURL,
			configuration.avroTopic+"-value",
		)
		if err != nil {
			panic(err)
		}
		records = append(records, &kgo.Record{
			Topic:     configuration.avroTopic,
			Partition: 0,
			Key:       []byte(configuration.key),
			Value:     encodeAvroRecord(schemaID, configuration.avroValue),
		})
	}
	results := client.ProduceSync(ctx, records...)
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
	fmt.Printf("seeded %s[0]@0\n", configuration.topic)
	if !configuration.stringOnly {
		fmt.Printf("seeded Avro %s[0]@0 schema=%d\n", configuration.avroTopic, schemaID)
	}

	for {
		fetches := client.PollRecords(context.Background(), 10)
		if errs := fetches.Errors(); len(errs) != 0 {
			panic(fmt.Errorf("group poll: %v", errs))
		}
		for _, consumed := range fetches.Records() {
			if consumed.Topic == configuration.topic &&
				string(consumed.Key) == configuration.key &&
				string(consumed.Value) == configuration.value {
				fmt.Printf("group active %s\n", configuration.groupID)
				for {
					if errs := client.PollRecords(context.Background(), 10).Errors(); len(errs) != 0 {
						panic(fmt.Errorf("group heartbeat poll: %v", errs))
					}
				}
			}
		}
	}
}

type seedConfiguration struct {
	bootstrapServers string
	topic            string
	key              string
	value            string
	groupID          string
	registryURL      string
	avroTopic        string
	avroValue        string
	stringOnly       bool
}

func configurationFromEnvironment(lookup func(string) (string, bool)) (seedConfiguration, error) {
	configuration := seedConfiguration{}
	var err error
	configuration.bootstrapServers, err = requiredEnvironment(lookup, "MEMKAFKA_BOOTSTRAP_SERVERS")
	if err != nil {
		return seedConfiguration{}, err
	}
	configuration.topic, err = requiredEnvironment(lookup, "MEMKAFKA_KAFBAT_TOPIC")
	if err != nil {
		return seedConfiguration{}, err
	}
	configuration.key, err = requiredEnvironment(lookup, "MEMKAFKA_KAFBAT_KEY")
	if err != nil {
		return seedConfiguration{}, err
	}
	configuration.value, err = requiredEnvironment(lookup, "MEMKAFKA_KAFBAT_VALUE")
	if err != nil {
		return seedConfiguration{}, err
	}
	configuration.groupID, err = requiredEnvironment(lookup, "MEMKAFKA_KAFBAT_GROUP")
	if err != nil {
		return seedConfiguration{}, err
	}
	stringOnly, _ := lookup("MEMKAFKA_KAFBAT_STRING_ONLY")
	configuration.stringOnly = strings.EqualFold(stringOnly, "true")
	if configuration.stringOnly {
		return configuration, nil
	}
	configuration.registryURL, err = requiredEnvironment(lookup, "MEMKAFKA_SCHEMA_REGISTRY_URL")
	if err != nil {
		return seedConfiguration{}, err
	}
	configuration.avroTopic, err = requiredEnvironment(lookup, "MEMKAFKA_KAFBAT_AVRO_TOPIC")
	if err != nil {
		return seedConfiguration{}, err
	}
	configuration.avroValue, err = requiredEnvironment(lookup, "MEMKAFKA_KAFBAT_AVRO_VALUE")
	if err != nil {
		return seedConfiguration{}, err
	}
	return configuration, nil
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

func requiredEnvironment(lookup func(string) (string, bool), name string) (string, error) {
	value, ok := lookup(name)
	if !ok || value == "" {
		return "", fmt.Errorf("%s is required", name)
	}
	return value, nil
}
