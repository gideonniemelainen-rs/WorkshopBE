package config

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// Setting up event driven architecture producers and consumers
type Producer struct {
	Brokers           []string
	Topic             string
	SchemaRegistryURL string
	StreamURL         string
	UserAgent         string
	FlushTimeout      time.Duration
	MetricsAddr       string
}

type Consumer struct {
	Brokers           []string
	Topic             string
	SchemaRegistryURL string
	Group             string
	BatchMaxRecords   int
	Workers           int
	ShutdownTimeout   time.Duration
	HTTPAddr          string
	JWTSecret         string
	AuthEnabled       bool
	DefaultUser       string
	DefaultPass       string

	// Start of scylla implementation
	ScyllaHosts       []string
	ScyllaKeyspace    string
	ScyllaUsername    string
	ScyllaPassword    string
	ScyllaConsistency string
	ScyllaTimeout     time.Duration
}

func LoadProducer() Producer {
	return Producer{
		Brokers:           splitCSV(getEnv("REDPANDA_BROKERS", "127.0.0.1:9092")),
		Topic:             getEnv("REDPANDA_TOPIC", "wikistream.recentchange.proto"),
		SchemaRegistryURL: getEnv("SCHEMA_REGISTRY_URL", "http://127.0.0.1:8081"),
		StreamURL:         getEnv("STREAM_URL", "https://stream.wikimedia.org/v2/stream/recentchange"),
		UserAgent:         getEnv("USER_AGENT", "wikistream/0.1"),
		FlushTimeout:      getDuration("FLUSH_TIMEOUT", 10*time.Second),
		MetricsAddr:       getEnv("PRODUCER_METRICS_ADDR", ":9091"),
	}
}

func LoadConsumer() Consumer {
	return Consumer{
		Brokers:           splitCSV(getEnv("REDPANDA_BROKERS", "127.0.0.1:9092")),
		Topic:             getEnv("REDPANDA_TOPIC", "wikistream.recentchange.proto"),
		SchemaRegistryURL: getEnv("SCHEMA_REGISTRY_URL", "http://127.0.0.1:8081"),
		Group:             getEnv("CONSUMER_GROUP", "wikistream-consumer"),
		BatchMaxRecords:   getEnvInt("BATCH_MAX_RECORDS", 500),
		Workers:           getEnvInt("CONSUMER_WORKERS", 3),
		ShutdownTimeout:   getDuration("SHUTDOWN_TIMEOUT", 15*time.Second),

		HTTPAddr:    getEnv("HTTP_ADDR", ":7000"),
		JWTSecret:   getEnv("JWT_SECRET", "dear-god-no"),
		AuthEnabled: getEnvBool("AUTH_ENABLED", true),
		DefaultUser: getEnv("DEFAULT_USER", "definitelynotroot"),
		DefaultPass: getEnv("DEFAULT_PASS", "definitelynotpassword"),

		ScyllaHosts:       splitCSV(getEnv("SCYLLA_HOSTS", "127.0.0.1")),
		ScyllaKeyspace:    getEnv("SCYLLA_KEYSPACE", "wikistream"),
		ScyllaUsername:    getEnv("SCYLLA_USERNAME", ""),
		ScyllaPassword:    getEnv("SCYLLA_PASSWORD", ""),
		ScyllaConsistency: getEnv("SCYLLA_CONSISTENCY", "LOCAL_QUORUM"),
		ScyllaTimeout:     getDuration("SCYLLA_TIMEOUT", 5*time.Second),
	}
}

// Helper for pulling out schema registry subject
func AppendValueToSubject(topic string) string {
	return topic + "-value"
}

func validateHTTPUrl(name string, raw string) error {
	if raw == "" {
		return fmt.Errorf("%s is required", name)
	}
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("%s is not a valid URL: %w", name, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("%s must use http or https protocol", name)
	}
	if u.Host == "" {
		return fmt.Errorf("%s must include a host", name)
	}
	return nil
}

func (p Producer) Validate() error {
	if len(p.Brokers) == 0 {
		return fmt.Errorf("REDPANDA_BROKERS is required")
	}
	if p.Topic == "" {
		return fmt.Errorf("REDPANDA_TOPIC is required")
	}
	if p.StreamURL == "" {
		return fmt.Errorf("STREAM_URL is required")
	}
	if err := validateHTTPUrl("SCHEMA_REGISTRY_URL", p.SchemaRegistryURL); err != nil {
		return err
	}
	return nil
}

func (c Consumer) Validate() error {
	if len(c.Brokers) == 0 {
		return fmt.Errorf("REDPANDA_BROKERS is required")
	}
	if c.Topic == "" {
		return fmt.Errorf("REDPANDA_TOPIC is required")
	}
	if c.Group == "" {
		return fmt.Errorf("CONSUMER_GROUP is required")
	}
	if c.BatchMaxRecords <= 0 {
		return fmt.Errorf("BATCH_MAX_RECORDS must be greater than 0")
	}
	if c.ScyllaKeyspace == "" {
		return fmt.Errorf("SCYLLA_KEYSPACE is required")
	}
	if len(c.ScyllaHosts) == 0 || c.ScyllaHosts[0] == "" {
		return fmt.Errorf("SCYLLA_HOSTS is required")
	}
	if c.AuthEnabled && c.JWTSecret == "" {
		return fmt.Errorf("JWT_SECRET is required when AUTH_ENABLED=true")
	}
	if err := validateHTTPUrl("SCHEMA_REGISTRY_URL", c.SchemaRegistryURL); err != nil {
		return err
	}
	if c.Workers <= 0 {
		return fmt.Errorf("CONSUMER_WORKERS must be greater than 0")
	}
	if c.ShutdownTimeout <= 0 {
		return fmt.Errorf("SHUTDOWN_TIMEOUT must be greater than 0")
	}

	return nil
}

func getEnv(key string, fallback string) string {
	val := os.Getenv(key)
	if val == "" {
		return fallback
	}
	return val
}

func getEnvBool(key string, fallback bool) bool {
	val := os.Getenv(key)
	// More return cases needed for this one, don't really have JS style type coercion
	if val == "" {
		return fallback
	}
	switch strings.ToLower(val) {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return fallback
	}
}

func getEnvInt(key string, fallback int) int {
	val := os.Getenv(key)
	if val == "" {
		return fallback
	}

	num, err := strconv.Atoi(val)
	if err != nil {
		return fallback
	}

	return num
}

func getDuration(key string, fallback time.Duration) time.Duration {
	val := os.Getenv(key)
	if val == "" {
		return fallback
	}

	duration, err := time.ParseDuration(val)
	if err != nil {
		return fallback
	}

	return duration
}

func splitCSV(val string) []string {
	var output []string
	for _, part := range strings.Split(val, ",") {
		if p := strings.TrimSpace(part); p != "" {
			output = append(output, p)
		}
	}
	return output
}
