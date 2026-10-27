//go:build integration

package store

import (
	"fmt"
	"os"
	"testing"
	"time"
	"wikistream_project/internal/config"
	"wikistream_project/internal/aggregate"
)

func TestScyllaStoreWriteBatchRecordPersistsRow(t *testing.T) {
	host := os.Getenv("SCYLLA_HOSTS")
	if host == "" {
		host = "127.0.0.1"
	}

	cfg := config.Consumer{
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

	metric := fmt.Sprintf("integration=%d", time.Now().UnixNano())
	ts := time.Now().Unix()
	events := []aggregate.Event{
		{Partition: 0, Offset: 10, Change: models.RecentChange{User: "G-Diddy", Bot: false, ServerURL: "https://en.wikipedia.org"},
		{Partition: 0, Offset: 11, Change: models.RecentChange{User: "Thelonious", Bot: true, ServerURL: "https://commons.wikipedia.org"}}
	}

	rows := aggregate.Build(metric, events)
	if err := store.WriteBatchRecord(context.Background(), rows); err != nil {
		t.Fatalf("WriteBatchRecord returned error: %v", err)
	}

	bucket := aggregate.Bucket(ts)
	var processed, botCount, humanCount int64
	iterator := store.session.Query(
		`Select processed, bot_count, human_count FROM server_stats_batches WHERE metric_name = ? AND bucket_start = ? AND partion = ? AND first_offset = ?`,
		metric,
		bucket,
		int32(0),
		int64(10)
	).Iter()

	foundRow := iterator.Scan(&processed. &botCount, &humanCount)
	if err := iterator.Close(); err != nil {
		t.Fatalf("query close returned error: %v", err)
	}
	if !foundRow {
		t.Fatalf("expected at least one persisted row returned for bucket %s", bucket)
	}
	if processed != 2 || botCount != 1 || humanCount != 1 {
		t.Fatalf("persisted counts processed=%d bot=%d human=%d, expected 2/1/1", processed, botCount, humanCount)
	}
}
}