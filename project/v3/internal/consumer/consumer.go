package consumer

import (
	"context"
	"errors"
	"fmt"
	"log"
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
	WriteBatchRecord(ctx context.Context, rows []aggregate.Row) error
}

func NewClient(cfg config.Consumer) (*kgo.Client, error) {
	return kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(cfg.Group),
		kgo.ConsumeTopics(cfg.Topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.OnPartitionsRevoked(func(_ context.Context, _ *kgo.Client, lost map[string][]int32) {
			log.Printf("partitions revoked: %v", lost)
		}),
	)
}

func Run(ctx context.Context, cl *kgo.Client, sink Sink, stats *models.ServerStats, batchMax int, decode Decoder, m *metrics.Consumer) error {
	for ctx.Err() == nil {
		fetches := cl.PollRecords(ctx, batchMax)
		if fetches.IsClientClosed() {
			return nil
		}

		fetches.EachError(func(topic string, partition int32, err error) {
			if errors.Is(err, context.Canceled) {
				return
			}
			log.Printf("fetch error topic=%s partition=%d: %v", topic, partition, err)
		})

		err := processBatch(context.WithoutCancel(ctx), cl, sink, stats, fetches.Records(), decode, m)

		cl.AllowRebalance()

		if err != nil {
			return err
		}
	}

	return nil
}

func processBatch(ctx context.Context, cl *kgo.Client, sink Sink, stats *models.ServerStats, records []*kgo.Record, decode Decoder, m *metrics.Consumer) error {
	if len(records) == 0 {
		return nil
	}
	m.RedpandaConsumed.Add(float64(len(records)))

	events := make([]aggregate.Event, 0, len(records))
	for _, r := range records {
		rc, err := decode(r.Value)
		if err != nil {
			m.Failed.WithLabelValues(metrics.StageDecode).Inc()
			log.Printf("skipping record that could not be decoded %s/%d@%d: %v", r.Topic, r.Partition, r.Offset, err)
			continue
		}
		events = append(events, aggregate.Event{Partition: r.Partition, Offset: r.Offset, Change: rc})
	}

	if len(events) > 0 {
		rows := aggregate.Build(aggregate.MetricName, events)
		if err := writeWithRetry(ctx, sink, rows); err != nil {
			m.Failed.WithLabelValues(metrics.StageWrite).Add(float64(len(events)))
			stats.SetError(err)
			return fmt.Errorf("write batch of %d rows: %w", len(rows), err)
		}
		for _, e := range events {
			stats.Record(e.Change)
		}
	}

	if err := cl.CommitRecords(ctx, records...); err != nil {
		m.Failed.WithLabelValues(metrics.StageCommit).Add(float64(len(events)))
		stats.SetError(err)
		return fmt.Errorf("commit offsets: %w", err)
	}

	m.Processed.Add(float64(len(events)))
	stats.SetError(nil)
	return nil
}

func writeWithRetry(ctx context.Context, sink Sink, rows []aggregate.Row) error {
	var err error
	for attempt := 1; attempt <= writeAttempts; attempt++ {
		if err = sink.WriteBatchRecord(ctx, rows); err == nil {
			return nil
		}
		log.Printf("write attempt %d/%d failed: %v", attempt, writeAttempts, err)
		if attempt < writeAttempts {
			time.Sleep(writeBackoff * time.Duration(attempt))
		}
	}
	return err
}
