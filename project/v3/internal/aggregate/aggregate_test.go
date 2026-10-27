package aggregate

import (
	"testing"
	"wikistream_project/internal/models"
)

// Set up timestamps a minute apart to use in tests. We use int64 for the actual timestamps but the model needs to be able to parse into a more human readable format
const (
	tsMinuteA = 1704067265
	tsMinuteB = 1704067325
)

func TestBucketUsesEventTimeHandlesMissingTimestamp(t *testing.T) {
	if got := Bucket(tsMinuteA); got != "2024-01-01T00:01:00Z" {
		t.Fatalf("Bucket(%d) = %q", tsMinuteA, got)
	}
	if got := Bucket(0); got != UnknownBucket {
		t.Fatalf("Bucket(0) = %q, expected %q", got, UnknownBucket)
	}
}

func TestBuildGroupsByBucketAndPartition(t *testing.T) {
	events := []Event{
		{Partition: 0, Offset: 5, Change: models.RecentChange{User: "number1boy", Bot: false, ServerURL: "url1", Timestamp: tsMinuteA}},
		{Partition: 0, Offset: 6, Change: models.RecentChange{User: "number1boy", Bot: true, ServerURL: "url1", Timestamp: tsMinuteA}},
		{Partition: 0, Offset: 7, Change: models.RecentChange{User: "number2boy", Bot: false, ServerURL: "url2", Timestamp: tsMinuteB}},
		{Partition: 1, Offset: 3, Change: models.RecentChange{User: "number3boy", Bot: false, ServerURL: "url1", Timestamp: tsMinuteA}},
	}

	rows := Build(MetricName, events)
	if len(rows) != 3 {
		t.Fatalf("got %d rows, expected 3", len(rows))
	}

	first := rows[0]
	if first.Partition != 0 || first.BucketStart != "2024-01-01T00:01:00Z" {
		t.Fatalf("unexpected first row key: %+v", first)
	}
	if first.FirstOffset != 5 || first.LastOffset != 6 {
		t.Fatalf("offsets = [%d, %d], expected [5, 6]", first.FirstOffset, first.LastOffset)
	}
	if first.Processed != 2 || first.UniqueUsers != 1 || first.BotCount != 1 || first.HumanCount != 1 {
		t.Fatalf("unexpected counts: %+v", first)
	}
	if first.ByServerURL["url1"] != 2 {
		t.Fatalf("ByServerURL[url1] = %d, expected 2", first.ByServerURL["url1"])
	}

}

func TestBuiildIsDeterministic(t *testing.T) {
	events := []Event{
		{Partition: 0, Offset: 1, Change: models.RecentChange{User: "number1boy", Bot: false, ServerURL: "url1", Timestamp: tsMinuteA}},
		{Partition: 0, Offset: 2, Change: models.RecentChange{User: "number2boy", Bot: false, ServerURL: "url1", Timestamp: tsMinuteB}},
	}

	first := Build(MetricName, events)
	second := Build(MetricName, events)

	if len(first) != len(second) {
		t.Fatalf("row count differs between builds: %d vs %d", len(first), len(second))
	}
	for i := range first {
		if first[i].BucketStart != second[i].BucketStart || first[i].FirstOffset != second[i].FirstOffset {
			t.Fatalf("row %d differs between runs", i)
		}
	}
}

func TestBuildWithEmptyInput(t *testing.T) {
	if rows := Build(MetricName, nil); len(rows) != 0 {
		t.Fatalf("expected no rows, got %d", len(rows))
	}
}
