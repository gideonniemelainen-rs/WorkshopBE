// go:build !integration

package main

import (
	"os"
	"testing"
	"time"
)

func TestScyllaStoreInitAndFlushPersistsRow(t *testing.T) {
	host := os.Getenv("SCYLLA_HOST")
	if host == "" {
		host = "127.0.0.1"
	}

	cfg := Config{
		StorageMode:       "scylla",
		ScyllaHosts:       []string{host},
		ScyllaKeyspace:    "wikistream",
		ScyllaConsistency: "LOCAL_QUORUM",
		ScyllaTimeout:     10 * time.Second,
	}

	store, err := newScyllaStore(cfg)
	if err != nil {
		t.Fatalf("newScyllaStore returned error: %v", err)
	}
	defer store.Close()

	store.Record(RecentChange{User: "G-Diddy", Bot: false, ServerURL: "https://en.wikipedia.org"})
	store.Record(RecentChange{User: "Thelonious", Bot: true, ServerURL: "https://commons.wikipedia.org"})
	store.SetError(nil)

	if err := store.Flush(); err != nil {
		t.Fatalf("flush returned error: %v", err)
	}

	bucket := time.Now().UTC().Truncate(time.Minute).Format(time.RFC3339)
	var processed int64
	iterator := store.session.Query(
		`SELECT processed FROM server_stats_by_minute WHERE bucket_start = ? AND metric_name = ? AND instance_id = ? LIMIT 1`,
		bucket,
		"recentchange",
		store.instanceID,
	).Iter()

	foundRecord := iterator.Scan(&processed)
	if err := iterator.Close(); err != nil {
		t.Fatalf("query close returned error: %v", err)
	}
	if !foundRecord {
		t.Fatalf("expected at least one persisted row returned for bucket %s", bucket)
	}
	if processed < 1 {
		t.Fatalf("expected processed count > 0, got %d", processed)
	}
}
