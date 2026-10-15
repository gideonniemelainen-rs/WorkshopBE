package main

import (
	"os"
	"strings"
)

type Config struct {
	HTTPAddr    string
	StreamURL   string
	UserAgent   string
	JWTSecret   string
	AuthEnabled bool
	DefaultUser string
	DefaultPass string
}

func loadConfig() Config {
	return Config{
		HTTPAddr:    getEnv("HTTP_ADDR", ":7000"),
		StreamURL:   getEnv("STREAM_URL", "https://stream.wikimedia.org/v2/stream/recentchange"),
		UserAgent:   getEnv("USER_AGENT", "wikistream/0.1"),
		JWTSecret:   getEnv("JWT_SECRET", "dear-god-no"),
		AuthEnabled: getEnvBool("AUTH_ENABLED", true),
		DefaultUser: getEnv("DEFAULT_USER", "definitelynotroot"),
		DefaultPass: getEnv("DEFAULT_PASS", "definitelynotpassword"),
	}
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
