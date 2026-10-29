package config

import (
	"os"
	"testing"
)

func buildValidConsumer() Consumer {
	return Consumer{
		Brokers:           []string{"localhost:9092"},
		Topic:             "t",
		Group:             "g",
		BatchMaxRecords:   10,
		ScyllaHosts:       []string{"localhost:9042"},
		ScyllaKeyspace:    "ks",
		AuthEnabled:       false,
		SchemaRegistryURL: "http://redpanda:8081",
		Workers:           3,
		ShutdownTimeout:   10,
	}
}

func TestConsumerValidateMethodAcceptsValidConfig(t *testing.T) {
	if err := buildValidConsumer().Validate(); err != nil {
		t.Fatalf("expected valid consumer config: %v", err)
	}
}

func TestConsumerValidateMethodRejectsMissingKeyspaceAndHosts(t *testing.T) {
	cfg := buildValidConsumer()
	cfg.ScyllaKeyspace = ""
	cfg.ScyllaHosts = []string{""}

	if err := cfg.Validate(); err == nil {
		t.Fatalf("expected validation error for missing keyspace and hosts")
	}
}

func TestConsumerValidateMethodRejectsBatchSizeOfZeroOrLess(t *testing.T) {
	cfg := buildValidConsumer()
	cfg.BatchMaxRecords = 0

	if err := cfg.Validate(); err == nil {
		t.Fatalf("expected validation error for batch size of zero or less")
	}
}

func TestLoadConsumerUsesDefaultsWhenMissing(t *testing.T) {
	_ = os.Unsetenv("HTTP_ADDR")
	_ = os.Unsetenv("BATCH_MAX_RECORDS")

	cfg := LoadConsumer()
	if cfg.HTTPAddr != ":7000" {
		t.Fatalf("HTTPAddr = %q, expected :7000", cfg.HTTPAddr)
	}
	if cfg.BatchMaxRecords != 500 {
		t.Fatalf("BatchMaxRecords = %d, expected 500", cfg.BatchMaxRecords)
	}
}
