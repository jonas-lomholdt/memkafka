package main

import (
	"strings"
	"testing"
)

func TestConfigurationFromEnvironmentStringOnlyDoesNotRequireAvro(t *testing.T) {
	t.Parallel()

	values := map[string]string{
		"MEMKAFKA_BOOTSTRAP_SERVERS":  "kafka:9092",
		"MEMKAFKA_KAFBAT_TOPIC":       "probe-topic",
		"MEMKAFKA_KAFBAT_KEY":         "probe-key",
		"MEMKAFKA_KAFBAT_VALUE":       "probe-value",
		"MEMKAFKA_KAFBAT_GROUP":       "probe-group",
		"MEMKAFKA_KAFBAT_STRING_ONLY": "TrUe",
	}

	got, err := configurationFromEnvironment(mapLookup(values))
	if err != nil {
		t.Fatalf("configurationFromEnvironment() error = %v", err)
	}
	want := seedConfiguration{
		bootstrapServers: "kafka:9092",
		topic:            "probe-topic",
		key:              "probe-key",
		value:            "probe-value",
		groupID:          "probe-group",
		stringOnly:       true,
	}
	if got != want {
		t.Fatalf("configurationFromEnvironment() = %#v, want %#v", got, want)
	}
}

func TestConfigurationFromEnvironmentAvroModeRequiresAndReturnsAvroSettings(t *testing.T) {
	t.Parallel()

	values := map[string]string{
		"MEMKAFKA_BOOTSTRAP_SERVERS":   "kafka:9092",
		"MEMKAFKA_KAFBAT_TOPIC":        "probe-topic",
		"MEMKAFKA_KAFBAT_KEY":          "probe-key",
		"MEMKAFKA_KAFBAT_VALUE":        "probe-value",
		"MEMKAFKA_KAFBAT_GROUP":        "probe-group",
		"MEMKAFKA_SCHEMA_REGISTRY_URL": "http://registry:8081",
		"MEMKAFKA_KAFBAT_AVRO_TOPIC":   "avro-topic",
		"MEMKAFKA_KAFBAT_AVRO_VALUE":   "avro-value",
	}

	got, err := configurationFromEnvironment(mapLookup(values))
	if err != nil {
		t.Fatalf("configurationFromEnvironment() error = %v", err)
	}
	want := seedConfiguration{
		bootstrapServers: "kafka:9092",
		topic:            "probe-topic",
		key:              "probe-key",
		value:            "probe-value",
		groupID:          "probe-group",
		registryURL:      "http://registry:8081",
		avroTopic:        "avro-topic",
		avroValue:        "avro-value",
	}
	if got != want {
		t.Fatalf("configurationFromEnvironment() = %#v, want %#v", got, want)
	}
}

func TestConfigurationFromEnvironmentAvroModeRejectsMissingRegistry(t *testing.T) {
	t.Parallel()

	values := map[string]string{
		"MEMKAFKA_BOOTSTRAP_SERVERS": "kafka:9092",
		"MEMKAFKA_KAFBAT_TOPIC":      "probe-topic",
		"MEMKAFKA_KAFBAT_KEY":        "probe-key",
		"MEMKAFKA_KAFBAT_VALUE":      "probe-value",
		"MEMKAFKA_KAFBAT_GROUP":      "probe-group",
		"MEMKAFKA_KAFBAT_AVRO_TOPIC": "avro-topic",
		"MEMKAFKA_KAFBAT_AVRO_VALUE": "avro-value",
	}

	_, err := configurationFromEnvironment(mapLookup(values))
	if err == nil || !strings.Contains(err.Error(), "MEMKAFKA_SCHEMA_REGISTRY_URL is required") {
		t.Fatalf("configurationFromEnvironment() error = %v, want missing registry error", err)
	}
}

func mapLookup(values map[string]string) func(string) (string, bool) {
	return func(name string) (string, bool) {
		value, ok := values[name]
		return value, ok
	}
}
