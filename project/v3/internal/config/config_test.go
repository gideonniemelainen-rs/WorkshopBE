package config

import (
	"os"
	"testing"
)

func TestConfigValidateMemoryMode(t *testing.T) {
	cfg := Config{
		StorageMode: "memory",
		AuthEnabled: false,
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected memory mode config to validate: %v", err)
	}
}

func TestConfigValidateScyllaModeRequiresKeyspaceAndHosts(t *testing.T) {
	cfg := Config{
		StorageMode:    "scylla",
		ScyllaHosts:    []string{""},
		ScyllaKeyspace: "",
		AuthEnabled:    false,
	}

	err := cfg.Validate()
	if err == nil {
		t.Fatalf("expected validation error for scylla mode missing required props")
	}
}

func TestLoadConfigUsesDefaultsWhenMissing(t *testing.T) {
	_ = os.Unsetenv("HTTP_ADDR")
	_ = os.Unsetenv("STORAGE_MODE")

	cfg := loadConfig()
	if cfg.HTTPAddr == "" {
		t.Fatal("expected HTTP_ADDR to be set with default value when left blank")
	}
	if cfg.StorageMode == "" {
		t.Fatal("expected STORAGE_MODE to be set with default value when left blank")
	}
}
