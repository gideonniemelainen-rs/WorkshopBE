package config

import (
	"fmt"
	"os"
	"strings"
	"time"
)

type Config struct {
	HTTPAddr    string
	StreamURL   string
	UserAgent   string
	JWTSecret   string
	AuthEnabled bool
	DefaultUser string
	DefaultPass string
	StorageMode string

	// Start of scylla implementation
	ScyllaHosts       []string
	ScyllaKeyspace    string
	ScyllaUsername    string
	ScyllaPassword    string
	ScyllaConsistency string
	ScyllaTimeout     time.Duration
}

func LoadConfig() Config {
	return Config{
		HTTPAddr:    getEnv("HTTP_ADDR", ":7000"),
		StreamURL:   getEnv("STREAM_URL", "https://stream.wikimedia.org/v2/stream/recentchange"),
		UserAgent:   getEnv("USER_AGENT", "wikistream/0.1"),
		JWTSecret:   getEnv("JWT_SECRET", "dear-god-no"),
		AuthEnabled: getEnvBool("AUTH_ENABLED", true),
		DefaultUser: getEnv("DEFAULT_USER", "definitelynotroot"),
		DefaultPass: getEnv("DEFAULT_PASS", "definitelynotpassword"),
		StorageMode: getEnv("STORAGE_MODE", "memory"),

		ScyllaHosts:       strings.Split(getEnv("SCYLLA_HOSTS", "127.0.0.1"), ","),
		ScyllaKeyspace:    getEnv("SCYLLA_KEYSPACE", "wikistream"),
		ScyllaUsername:    getEnv("SCYLLA_USERNAME", ""),
		ScyllaPassword:    getEnv("SCYLLA_PASSWORD", ""),
		ScyllaConsistency: getEnv("SCYLLA_CONSISTENCY", "LOCAL_QUORUM"),
		ScyllaTimeout:     getDuration("SCYLLA_TIMEOUT", 5*time.Second),
	}
}

func (c Config) Validate() error {
	switch c.StorageMode {
	case "memory", "scylla":
	default:
		return fmt.Errorf("unsupported STORAGE_MODE: %q", c.StorageMode)
	}

	if c.StorageMode == "scylla" {
		if c.ScyllaKeyspace == "" {
			return fmt.Errorf("SCYLLA_KEYSPACE is required when STORAGE_MODE=scylla")
		}
		if len(c.ScyllaHosts) == 0 || c.ScyllaHosts[0] == "" {
			return fmt.Errorf("SCYLLA_HOSTS is required when STORAGE_MODE=scylla")
		}
	}

	if c.AuthEnabled && c.JWTSecret == "" {
		return fmt.Errorf("JWT_SECRET is required when AUTH_ENABLED=true")
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
