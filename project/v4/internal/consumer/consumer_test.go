package consumer

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
	"wikistream_project/internal/aggregate"
	"wikistream_project/internal/codec"
	"wikistream_project/internal/config"
	"wikistream_project/internal/metrics"
	"wikistream_project/internal/models"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

type mockSink struct {
	mutex     sync.Mutex
	err       error
	processed int64
	onWrite   func(total int64)
}

func (m *mockSink) WriteBatchRecord(_ context.Context, rows []aggregate.Row) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.err != nil {
		return m.err
	}
	for _, r := range rows {
		m.processed += r.Processed
	}
	if m.onWrite != nil {
		m.onWrite(m.processed)
	}
	return nil
}

func (m *mockSink) total() int64 {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return m.processed
}

func startBroker(t *testing.T, topic string) []string {
	t.Helper()

	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, topic))
	if err != nil {
		t.Fatalf("start kfake: %v", err)
	}
	t.Cleanup(cluster.Close)
	return cluster.ListenAddrs()
}

func produce(t *testing.T, addrs []string, topic string, values ...string) {
	t.Helper()

	cluster, err := kgo.NewClient(kgo.SeedBrokers(addrs...), kgo.DefaultProduceTopic(topic))
	if err != nil {
		t.Fatalf("producer client: %v", err)
	}
	defer cluster.Close()

	records := make([]*kgo.Record, 0, len(values))
	for _, v := range values {
		records = append(records, &kgo.Record{Key: []byte("https://en.wikipedia.org"), Value: []byte(v)})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := cluster.ProduceSync(ctx, records...).FirstErr(); err != nil {
		t.Fatalf("produce: %v", err)
	}
}

// Set up test codec for new tests
var testCodec = codec.New(1)

func event(t *testing.T, ts int64, user string, bot bool) string {
	t.Helper()

	b, err := testCodec.EncodeEvent(models.RecentChange{
		User:      user,
		Bot:       bot,
		ServerURL: "https://en.wikipedia.org",
		Timestamp: ts,
	})
	if err != nil {
		t.Fatalf("marshal event failure: %v", err)
	}
	return string(b)
}

func testConfig(addrs []string, topic, group string) config.Consumer {
	return config.Consumer{Brokers: addrs, Topic: topic, Group: group}
}

func countUncommitted(t *testing.T, cfg config.Consumer) int {
	t.Helper()

	cl, err := NewClient(cfg)
	if err != nil {
		t.Fatalf("verify client failure: %v", err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	n := 0
	for ctx.Err() == nil {
		fetches := cl.PollRecords(ctx, 100)
		n += len(fetches.Records())
		cl.AllowRebalance()
		if fetches.IsClientClosed() {
			break
		}
	}
	return n
}

func TestRunWritesThenCommitsBatchRecord(t *testing.T) {
	const topic = "testing.recentchange"
	addrs := startBroker(t, topic)
	cfg := testConfig(addrs, topic, "group-commit")

	produce(t, addrs, topic,
		event(t, 1704067265, "bender_rodriguez", false),
		event(t, 1704067266, "fry", true),
		"not json",
	) // Putting this in here to fail json parse and confirm skip

	cl, err := NewClient(cfg)
	if err != nil {
		t.Fatalf("NewClient failure: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sink := &mockSink{onWrite: func(total int64) {
		if total >= 2 {
			cancel()
		}
	}}
	stats := models.NewServerStats()
	m := metrics.NewConsumer()

	done := make(chan error, 1)
	go func() { done <- Run(ctx, cl, sink, stats, 10, testCodec.DecodeEvent, m) }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned error: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("run did not finish")
	}
	cl.Close()

	if got := sink.total(); got != 2 {
		t.Fatalf("sink processed = %d, expected 2", got)
	}
	if n := countUncommitted(t, cfg); n != 0 {
		t.Fatalf("after commit, group still sees %d records, expected 0", n)
	}
}

func TestRunDoesNotCommitWhenWriteFails(t *testing.T) {
	const topic = "testing.recentchange.failure"
	addrs := startBroker(t, topic)
	cfg := testConfig(addrs, topic, "group-fail")

	produce(t, addrs, topic,
		event(t, 1704067265, "bender_rodriguez", false),
		event(t, 1704067266, "fry", true),
	)

	cl, err := NewClient(cfg)
	if err != nil {
		t.Fatalf("NewClient failure: %v", err)
	}

	sink := &mockSink{err: errors.New("Mock Scylla unavailable")}
	stats := models.NewServerStats()
	m := metrics.NewConsumer()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	err = Run(ctx, cl, sink, stats, 10, testCodec.DecodeEvent, m)
	cl.Close()

	if err == nil {
		t.Fatal("run returned nil, expected write error")
	}

	if n := countUncommitted(t, cfg); n != 2 {
		t.Fatalf("after failed write, group sees %d records, expected 2", n)
	}
}
