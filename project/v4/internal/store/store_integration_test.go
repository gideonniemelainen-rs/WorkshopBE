//go:build integration

package store

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"
	"wikistream_project/internal/aggregate"
	"wikistream_project/internal/config"
	"wikistream_project/internal/models"
)

func TestScyllaStoreWriteBatchRecordPersistsRowsAndCheckpoints(t *testing.T) {
	host := os.Getenv("SCYLLA_HOSTS")
	if host == "" {
		host = "127.0.0.1"
	}

	cfg := config.Consumer{
		ScyllaHosts:       []string{host},
		ScyllaKeyspace:    "wikistream",
		ScyllaConsistency: "LOCAL_QUORUM",
		ScyllaTimeout:     10 * time.Second,
		Group:             "integration-test-group",
		Topic:             "integration-test-topic",
	}

	store, err := NewScyllaStore(cfg)
	if err != nil {
		t.Fatalf("newScyllaStore returned error: %v", err)
	}
	defer store.Close()

	metric := fmt.Sprintf("integration=%d", time.Now().UnixNano())
	ts := time.Now().Unix()
	events := []aggregate.Event{
		{Partition: 0, Offset: 10, Change: models.RecentChange{User: "G-Diddy", Bot: false, ServerURL: "https://en.wikipedia.org", Timestamp: ts}},
		{Partition: 0, Offset: 11, Change: models.RecentChange{User: "Thelonious", Bot: true, ServerURL: "https://commons.wikipedia.org", Timestamp: ts}},
	}

	rows := aggregate.Build(metric, events)
	if err := store.WriteBatchRecord(context.Background(), rows, map[int32]int64{0: 12}); err != nil {
		t.Fatalf("WriteBatchRecord returned error: %v", err)
	}

	bucket := aggregate.Bucket(ts)
	var processed, botCount, humanCount int64
	err = store.session.Query(
		`SELECT processed, bot_count, human_count FROM server_stats_batches WHERE metric_name = ? AND bucket_start = ? AND partition = ? AND first_offset = ?`,
		metric,
		bucket,
		int32(0),
		int64(10),
	).Scan(&processed, &botCount, &humanCount)
	if err != nil {
		t.Fatalf("query persisted row returned error: %v", err)
	}
	if processed != 2 || botCount != 1 || humanCount != 1 {
		t.Fatalf("persisted counts processed=%d bot=%d human=%d, expected 2/1/1", processed, botCount, humanCount)
	}

	checkpoints, err := store.LoadCheckpoints(context.Background())
	if err != nil {
		t.Fatalf("loadcheckpoints returned error: %v", err)
	}
	if got := checkpoints[0]; got != 12 {
		t.Fatalf("checkpoint partition 0 = %d, expected 12", got)
	}
}
