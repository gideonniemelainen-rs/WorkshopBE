package consumer

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"sync"
	"testing"
	"time"
	"wikistream_project/internal/aggregate"
	"wikistream_project/internal/codec"
	"wikistream_project/internal/config"
	"wikistream_project/internal/metrics"
	"wikistream_project/internal/models"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

type rowKey struct {
	bucket      string
	partition   int32
	firstOffset int64
}

type mockSink struct {
	mutex       sync.Mutex
	rows        map[rowKey]aggregate.Row
	checkpoints map[int32]int64

	err             error
	persistThenFail int
	delay           time.Duration
	onWrite         func(total int64)
}

func newMockSink() *mockSink {
	return &mockSink{
		rows:        make(map[rowKey]aggregate.Row),
		checkpoints: make(map[int32]int64),
	}
}

func (m *mockSink) LoadCheckpoints(_ context.Context) (map[int32]int64, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return maps.Clone(m.checkpoints), nil
}

func (m *mockSink) WriteBatchRecord(_ context.Context, rows []aggregate.Row, checkpoints map[int32]int64) error {
	if m.delay > 0 {
		time.Sleep(m.delay)
	}

	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.err != nil {
		return m.err
	}
	crash := m.persistThenFail > 0
	if crash {
		m.persistThenFail--
	}

	for _, r := range rows {
		m.rows[rowKey{bucket: r.BucketStart, partition: r.Partition, firstOffset: r.FirstOffset}] = r
	}
	for partition, next := range checkpoints {
		if next > m.checkpoints[partition] {
			m.checkpoints[partition] = next
		}
	}

	if crash {
		return errors.New("simulated crash after write")
	}
	if m.onWrite != nil {
		m.onWrite(m.processedLocked())
	}
	return nil
}

func (m *mockSink) processedLocked() int64 {
	var total int64
	for _, r := range m.rows {
		total += r.Processed
	}
	return total
}

// snapshot returns the total processed, the processed count per partition, and a copy of the checkpoints.
func (m *mockSink) snapshot() (int64, map[int32]int64, map[int32]int64) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	perPartition := make(map[int32]int64)
	for _, r := range m.rows {
		perPartition[r.Partition] += r.Processed
	}
	return m.processedLocked(), perPartition, maps.Clone(m.checkpoints)
}

func startBroker(t *testing.T, topic string) []string {
	t.Helper()
	return startBrokerPartitions(t, topic, 1)
}

func startBrokerPartitions(t *testing.T, topic string, partitions int32) []string {
	t.Helper()

	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(partitions, topic))
	if err != nil {
		t.Fatalf("start kfake: %v", err)
	}
	t.Cleanup(cluster.Close)
	return cluster.ListenAddrs()
}

// produceToPartitions writes values round robin across partitions using explicit partition numbers.
func produceToPartitions(t *testing.T, addrs []string, topic string, partitions int32, values [][]byte) {
	t.Helper()

	cluster, err := kgo.NewClient(
		kgo.SeedBrokers(addrs...),
		kgo.DefaultProduceTopic(topic),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	if err != nil {
		t.Fatalf("producer client: %v", err)
	}
	defer cluster.Close()

	records := make([]*kgo.Record, 0, len(values))
	for i, v := range values {
		records = append(records, &kgo.Record{Partition: int32(i) % partitions, Value: v})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := cluster.ProduceSync(ctx, records...).FirstErr(); err != nil {
		t.Fatalf("produce: %v", err)
	}
}

// makeEvents encodes n valid events with distinct timestamps and users.
func makeEvents(t *testing.T, n int) [][]byte {
	t.Helper()

	values := make([][]byte, 0, n)
	for i := range n {
		values = append(values, event(t, 1704067265+int64(i), fmt.Sprintf("user-%d", i%7), i%3 == 0))
	}
	return values
}

func produce(t *testing.T, addrs []string, topic string, values ...[]byte) {
	t.Helper()

	cluster, err := kgo.NewClient(kgo.SeedBrokers(addrs...), kgo.DefaultProduceTopic(topic))
	if err != nil {
		t.Fatalf("producer client: %v", err)
	}
	defer cluster.Close()

	records := make([]*kgo.Record, 0, len(values))
	for _, v := range values {
		records = append(records, &kgo.Record{Key: []byte("https://en.wikipedia.org"), Value: v})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := cluster.ProduceSync(ctx, records...).FirstErr(); err != nil {
		t.Fatalf("produce: %v", err)
	}
}

// testCodec uses a fixed schema ID so tests do not need a registry.
var testCodec = codec.New(1)

func event(t *testing.T, ts int64, user string, bot bool) []byte {
	t.Helper()

	b, err := testCodec.EncodeEvent(models.RecentChange{
		User:      user,
		Bot:       bot,
		ServerURL: "https://en.wikipedia.org",
		Timestamp: ts,
	})
	if err != nil {
		t.Fatalf("encode event failure: %v", err)
	}
	return b
}

func testConfig(addrs []string, topic, group string) config.Consumer {
	return config.Consumer{
		Brokers:         addrs,
		Topic:           topic,
		Group:           group,
		BatchMaxRecords: 10,
		Workers:         3,
		ShutdownTimeout: 5 * time.Second,
	}
}

func assertCounter(t *testing.T, name string, c prometheus.Counter, want float64) {
	t.Helper()
	if got := testutil.ToFloat64(c); got != want {
		t.Fatalf("%s counter = %v, expected %v", name, got, want)
	}
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
		[]byte("not framed"),
	) // Invalid frame to confirm the record is skipped

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sink := newMockSink()
	sink.onWrite = func(total int64) {
		if total >= 2 {
			cancel()
		}
	}
	stats := models.NewServerStats()
	m := metrics.NewConsumer()

	done := make(chan error, 1)
	go func() { done <- Run(ctx, cfg, sink, stats, testCodec.DecodeEvent, m) }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned error: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("run did not finish")
	}

	if got, _, _ := sink.snapshot(); got != 2 {
		t.Fatalf("sink processed = %d, expected 2", got)
	}
	assertCounter(t, "redpanda consumed", m.RedpandaConsumed, 3)
	assertCounter(t, "processed", m.Processed, 2)
	assertCounter(t, "decode failed", m.Failed.WithLabelValues(metrics.StageDecode), 1)
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

	sink := newMockSink()
	sink.err = errors.New("Mock Scylla unavailable")
	stats := models.NewServerStats()
	m := metrics.NewConsumer()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	err := Run(ctx, cfg, sink, stats, testCodec.DecodeEvent, m)

	if err == nil {
		t.Fatal("run returned nil, expected write error")
	}
	// The two records may arrive in one or two polls. The member stops at the first failed batch,
	// so only the lower bounds are fixed. The key guarantees are zero processed and zero commits.
	if got := testutil.ToFloat64(m.RedpandaConsumed); got < 1 {
		t.Fatalf("redpanda consumed = %v, expected at least 1", got)
	}
	assertCounter(t, "processed", m.Processed, 0)
	if got := testutil.ToFloat64(m.Failed.WithLabelValues(metrics.StageWrite)); got < 1 {
		t.Fatalf("write failed counter = %v, expected at least 1", got)
	}

	if n := countUncommitted(t, cfg); n != 2 {
		t.Fatalf("after failed write, group sees %d records, expected 2", n)
	}
}

// Run with several members and partitions. Each record must be processed exactly once.
// Run with -race to catch unsynchronized access between members.
func TestRunDeliversEveryRecordOnceWithManyWorkers(t *testing.T) {
	const topic = "testing.recentchange.race"
	const partitions = 6
	const total = 300

	addrs := startBrokerPartitions(t, topic, partitions)
	cfg := testConfig(addrs, topic, "group-race")
	produceToPartitions(t, addrs, topic, partitions, makeEvents(t, total))

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	sink := newMockSink()
	sink.onWrite = func(processed int64) {
		if processed >= total {
			cancel()
		}
	}
	m := metrics.NewConsumer()

	if err := Run(ctx, cfg, sink, models.NewServerStats(), testCodec.DecodeEvent, m); err != nil {
		t.Fatalf("Run returned error: %v", err)
	}

	processed, perPartition, checkpoints := sink.snapshot()
	if processed != total {
		t.Fatalf("processed = %d, expected %d", processed, total)
	}
	const want = total / partitions
	for p := int32(0); p < partitions; p++ {
		if perPartition[p] != want {
			t.Fatalf("partition %d processed = %d, expected %d", p, perPartition[p], want)
		}
		if checkpoints[p] != want {
			t.Fatalf("partition %d checkpoint = %d, expected %d", p, checkpoints[p], want)
		}
	}
	assertCounter(t, "processed", m.Processed, total)
}

// Cancelling mid-stream must finish in-flight batches, return within ShutdownTimeout,
// and a restart must finish the stream with no loss and no double counting.
func TestRunStopsGracefullyAndResumesWithoutLoss(t *testing.T) {
	const topic = "testing.recentchange.shutdown"
	const partitions = 2
	const total = 200

	addrs := startBrokerPartitions(t, topic, partitions)
	cfg := testConfig(addrs, topic, "group-shutdown")
	produceToPartitions(t, addrs, topic, partitions, makeEvents(t, total))

	sink := newMockSink()
	sink.delay = 20 * time.Millisecond

	// First run: cancel once some work is done, while more batches are still pending.
	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	var once sync.Once
	sink.onWrite = func(processed int64) {
		if processed >= 30 {
			once.Do(cancel1)
		}
	}

	start := time.Now()
	if err := Run(ctx1, cfg, sink, models.NewServerStats(), testCodec.DecodeEvent, metrics.NewConsumer()); err != nil {
		t.Fatalf("first run returned error: %v", err)
	}
	if elapsed := time.Since(start); elapsed > cfg.ShutdownTimeout {
		t.Fatalf("shutdown took %s, expected within %s", elapsed, cfg.ShutdownTimeout)
	}
	if processed, _, _ := sink.snapshot(); processed < 30 || processed >= total {
		t.Fatalf("first run processed %d, expected a partial run between 30 and %d", processed, total-1)
	}

	// Restart with the same sink. It must pick up from the checkpoints.
	ctx2, cancel2 := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel2()
	sink.onWrite = func(processed int64) {
		if processed >= total {
			cancel2()
		}
	}
	m := metrics.NewConsumer()
	if err := Run(ctx2, cfg, sink, models.NewServerStats(), testCodec.DecodeEvent, m); err != nil {
		t.Fatalf("second run returned error: %v", err)
	}

	processed, perPartition, checkpoints := sink.snapshot()
	if processed != total {
		t.Fatalf("processed = %d, expected %d", processed, total)
	}
	const want = total / partitions
	for p := int32(0); p < partitions; p++ {
		if perPartition[p] != want {
			t.Fatalf("partition %d processed = %d, expected %d", p, perPartition[p], want)
		}
		if checkpoints[p] != want {
			t.Fatalf("partition %d checkpoint = %d, expected %d", p, checkpoints[p], want)
		}
	}
	if n := countUncommitted(t, cfg); n != 0 {
		t.Fatalf("after restart, group sees %d uncommitted records, expected 0", n)
	}
}

// A crash after the batch is persisted but before the Kafka commit must not lose or double count records.
func TestRunRecoversAfterCrashMidBatch(t *testing.T) {
	const topic = "testing.recentchange.crash"
	const total = 20

	addrs := startBroker(t, topic)
	cfg := testConfig(addrs, topic, "group-crash")
	produceToPartitions(t, addrs, topic, 1, makeEvents(t, total))

	sink := newMockSink()
	// Every write attempt persists the batch and then fails. The member stops without committing.
	sink.persistThenFail = writeAttempts

	ctx1, cancel1 := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel1()
	if err := Run(ctx1, cfg, sink, models.NewServerStats(), testCodec.DecodeEvent, metrics.NewConsumer()); err == nil {
		t.Fatal("first run returned nil, expected write error")
	}
	if n := countUncommitted(t, cfg); n == 0 {
		t.Fatal("after crash, expected uncommitted records, got 0")
	}

	// Restart. Records already covered by the checkpoint must be skipped, the rest processed.
	ctx2, cancel2 := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel2()
	sink.onWrite = func(processed int64) {
		if processed >= total {
			cancel2()
		}
	}
	m := metrics.NewConsumer()
	if err := Run(ctx2, cfg, sink, models.NewServerStats(), testCodec.DecodeEvent, m); err != nil {
		t.Fatalf("restart returned error: %v", err)
	}

	processed, _, checkpoints := sink.snapshot()
	if processed != total {
		t.Fatalf("processed = %d, expected %d", processed, total)
	}
	if checkpoints[0] != total {
		t.Fatalf("checkpoint = %d, expected %d", checkpoints[0], total)
	}
	if n := countUncommitted(t, cfg); n != 0 {
		t.Fatalf("after restart, group sees %d uncommitted records, expected 0", n)
	}
}
