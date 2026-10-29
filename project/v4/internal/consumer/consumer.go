package consumer

import (
	"context"
	"errors"
	"fmt"
	"log"
	"maps"
	"sync"
	"time"
	"wikistream_project/internal/aggregate"
	"wikistream_project/internal/config"
	"wikistream_project/internal/metrics"
	"wikistream_project/internal/models"

	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	writeAttempts = 5
	writeBackoff  = 200 * time.Millisecond
)

type Decoder func(value []byte) (models.RecentChange, error)

// Weirdly enough, despite working with Kafka previously, I was not familiar with the term 'sink'. Neat
type Sink interface {
	WriteBatchRecord(ctx context.Context, rows []aggregate.Row, checkpoints map[int32]int64) error
	// Handler for checkpoints on write, to avoid data loss on restart
	LoadCheckpoints(ctx context.Context) (map[int32]int64, error)
}

func NewClient(cfg config.Consumer, options ...kgo.Opt) (*kgo.Client, error) {
	base := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(cfg.Group),
		kgo.ConsumeTopics(cfg.Topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.OnPartitionsRevoked(func(_ context.Context, _ *kgo.Client, lost map[string][]int32) {
			log.Printf("partitions revoked: %v", lost)
		}),
	}
	return kgo.NewClient(append(base, options...)...)
}

// Updating Run function to handle multiple workers
func Run(ctx context.Context, cfg config.Consumer, sink Sink, stats *models.ServerStats, decode Decoder, m *metrics.Consumer) error {
	pollCtx, stopPolling := context.WithCancel(ctx)
	defer stopPolling()

	// This feels a bit funky with the with and without cancel, this is to enable hopefully more graceful shutdown and allow a batch already polled to finish writing
	workCtx, abort := context.WithCancel(context.WithoutCancel(ctx))
	defer abort()

	members := make([]*consumerMember, 0, cfg.Workers)
	for i := range cfg.Workers {
		cmb, err := newConsumerMember(i, cfg, sink, stats, decode, m)
		if err != nil {
			for _, created := range members {
				created.cl.Close()
			}
			return fmt.Errorf("start member %d: %w", i, err)
		}
		members = append(members, cmb)
	}

	errs := make([]error, len(members))
	var wg sync.WaitGroup
	for i, cmb := range members {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = cmb.run(pollCtx, workCtx)
			if errs[i] != nil {
				stopPolling()
			}
		}()
	}

	finished := make(chan struct{})
	go func() {
		wg.Wait()
		close(finished)
	}()

	select {
	case <-finished:
	case <-pollCtx.Done():
		timer := time.NewTimer(cfg.ShutdownTimeout)
		defer timer.Stop()
		select {
		case <-finished:
		case <-timer.C:
			log.Printf("shutdown timeout %s reached, aborting in-flight batches", cfg.ShutdownTimeout)
			abort()
			<-finished
		}
	}

	return errors.Join(errs...)
}

type consumerMember struct {
	id          int
	cl          *kgo.Client
	sink        Sink
	stats       *models.ServerStats
	decode      Decoder
	m           *metrics.Consumer
	batchMax    int
	mutex       sync.Mutex
	checkpoints map[int32]int64
	loadErr     error
}

func newConsumerMember(id int, cfg config.Consumer, sink Sink, stats *models.ServerStats, decode Decoder, m *metrics.Consumer) (*consumerMember, error) {
	cmb := &consumerMember{
		id:       id,
		sink:     sink,
		stats:    stats,
		decode:   decode,
		m:        m,
		batchMax: cfg.BatchMaxRecords,
	}
	cl, err := NewClient(cfg, kgo.OnPartitionsAssigned(cmb.onAssigned))
	if err != nil {
		return nil, err
	}
	cmb.cl = cl
	return cmb, nil
}

func (cmb *consumerMember) run(pollCtx context.Context, workCtx context.Context) error {
	defer cmb.cl.CloseAllowingRebalance()

	for pollCtx.Err() == nil {
		fetches := cmb.cl.PollRecords(pollCtx, cmb.batchMax)
		if fetches.IsClientClosed() {
			return nil
		}

		fetches.EachError(func(topic string, partition int32, err error) {
			if errors.Is(err, context.Canceled) {
				return
			}
			log.Printf("member %d fetch error topic=%s partition=%d: %v", cmb.id, topic, partition, err)
		})

		err := cmb.processPoll(workCtx, fetches.Records())

		cmb.cl.AllowRebalance()

		if err != nil {
			return err
		}
	}

	return nil
}

func (cmb *consumerMember) processPoll(ctx context.Context, records []*kgo.Record) error {
	if len(records) == 0 {
		return nil
	}

	checkpoints, err := cmb.snapshotCheckpoints()
	if err != nil {
		return err
	}
	cmb.m.RedpandaConsumed.Add(float64(len(records)))

	events := make([]aggregate.Event, 0, len(records))
	next := make(map[int32]int64)
	for _, r := range records {
		if cp, ok := checkpoints[r.Partition]; ok && r.Offset < cp {
			continue
		}
		next[r.Partition] = max(next[r.Partition], r.Offset+1)

		rc, err := cmb.decode(r.Value)
		if err != nil {
			cmb.m.Failed.WithLabelValues(metrics.StageDecode).Inc()
			log.Printf("skipping record that could not be decoded %s/%d@%d: %v", r.Topic, r.Partition, r.Offset, err)
			continue
		}
		events = append(events, aggregate.Event{Partition: r.Partition, Offset: r.Offset, Change: rc})
	}

	if len(next) > 0 {
		var rows []aggregate.Row
		if len(events) > 0 {
			rows = aggregate.Build(aggregate.MetricName, events)
		}
		err := funcWithRetry(ctx, "write batch", func(ctx context.Context) error {
			return cmb.sink.WriteBatchRecord(ctx, rows, next)
		})
		if err != nil {
			cmb.m.Failed.WithLabelValues(metrics.StageWrite).Add(float64(len(events)))
			cmb.stats.SetError(err)
			return fmt.Errorf("write batch of %d rows: %w", len(rows), err)
		}
		cmb.advanceCheckpoints(next)
	}

	for _, e := range events {
		cmb.stats.Record(e.Change)
	}

	if err := cmb.cl.CommitRecords(ctx, records...); err != nil {
		cmb.m.Failed.WithLabelValues(metrics.StageCommit).Add(float64(len(events)))
		cmb.stats.SetError(err)
		return fmt.Errorf("commit offsets: %w", err)
	}

	// Finally, add to processed count after commit. If we've made it this far, the record is written and committed
	cmb.m.Processed.Add(float64(len(events)))
	cmb.stats.SetError(nil)
	return nil
}

func (cmb *consumerMember) onAssigned(ctx context.Context, _ *kgo.Client, assigned map[string][]int32) {
	if len(assigned) == 0 {
		return
	}

	var checkpoints map[int32]int64
	err := funcWithRetry(ctx, "load checkpoints", func(ctx context.Context) error {
		var err error
		checkpoints, err = cmb.sink.LoadCheckpoints(ctx)
		return err
	})

	cmb.mutex.Lock()
	defer cmb.mutex.Unlock()
	if err != nil {
		cmb.loadErr = fmt.Errorf("load checkpoints: %w", err)
		return
	}
	cmb.checkpoints = checkpoints
	cmb.loadErr = nil
}

func (cmb *consumerMember) snapshotCheckpoints() (map[int32]int64, error) {
	cmb.mutex.Lock()
	defer cmb.mutex.Unlock()
	if cmb.loadErr != nil {
		return nil, cmb.loadErr
	}
	return maps.Clone(cmb.checkpoints), nil
}

func (cmb *consumerMember) advanceCheckpoints(next map[int32]int64) {
	cmb.mutex.Lock()
	defer cmb.mutex.Unlock()
	if cmb.checkpoints == nil {
		cmb.checkpoints = make(map[int32]int64, len(next))
	}
	for partition, offset := range next {
		if offset > cmb.checkpoints[partition] {
			cmb.checkpoints[partition] = offset
		}
	}
}

func funcWithRetry(ctx context.Context, what string, fn func(context.Context) error) error {
	var err error
	for attempt := 1; attempt <= writeAttempts; attempt++ {
		if err = fn(ctx); err == nil {
			return nil
		}
		log.Printf("write attempt %d/%d failed: %v", attempt, writeAttempts, err)
		if attempt == writeAttempts {
			break
		}
		select {
		case <-ctx.Done():
			return err
		case <-time.After(writeBackoff * time.Duration(attempt)):
		}
	}
	return err
}
