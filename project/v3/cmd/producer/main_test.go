package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"
	"wikistream_project/internal/codec"
	"wikistream_project/internal/metrics"
	"wikistream_project/internal/models"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

func assertCounter(t *testing.T, name string, c prometheus.Counter, want float64) {
	t.Helper()
	if got := testutil.ToFloat64(c); got != want {
		t.Fatalf("%s counter = %v, expected %v", name, got, want)
	}
}

func TestToRecordEncodesValidEvent(t *testing.T) {
	m := metrics.NewProducer()

	payload, err := json.Marshal(models.RecentChange{ServerURL: "https://en.wikipedia.org"})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}

	rec, ok := toRecord(payload, codec.New(1), m)
	if !ok {
		t.Fatal("valid event was skipped")
	}
	if string(rec.Key) != "https://en.wikipedia.org" {
		t.Fatalf("record key = %q", rec.Key)
	}
	if len(rec.Value) == 0 {
		t.Fatal("record value is empty")
	}
	assertCounter(t, "stream consumed", m.StreamConsumed, 1)
	assertCounter(t, "decode failed", m.Failed.WithLabelValues(metrics.StageDecode), 0)
	assertCounter(t, "persisted", m.Persisted, 0)
}

func TestToRecordSkipsUndecodablePayload(t *testing.T) {
	m := metrics.NewProducer()

	rec, ok := toRecord([]byte("not json"), codec.New(1), m)
	if ok {
		t.Fatalf("invalid payload produced a record: %+v", rec)
	}
	if rec != nil {
		t.Fatalf("expected nil record, got %+v", rec)
	}
	assertCounter(t, "stream consumed", m.StreamConsumed, 1)
	assertCounter(t, "decode failed", m.Failed.WithLabelValues(metrics.StageDecode), 1)
	assertCounter(t, "persisted", m.Persisted, 0)
}

func TestPublishCountsPersistedEvents(t *testing.T) {
	const topic = "wikistream.test"

	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, topic))
	if err != nil {
		t.Fatalf("start kfake: %v", err)
	}
	t.Cleanup(cluster.Close)

	cl, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...), kgo.DefaultProduceTopic(topic))
	if err != nil {
		t.Fatalf("producer client: %v", err)
	}
	defer cl.Close()

	m := metrics.NewProducer()
	for i := 0; i < 3; i++ {
		publish(cl, m, &kgo.Record{Key: []byte("https://en.wikipedia.org"), Value: []byte("v")})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := cl.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}

	assertCounter(t, "persisted", m.Persisted, 3)
	assertCounter(t, "produce failed", m.Failed.WithLabelValues(metrics.StageProduce), 0)
}
