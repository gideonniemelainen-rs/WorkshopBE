package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
	"wikistream_project/internal/codec"
	"wikistream_project/internal/config"
	"wikistream_project/internal/metrics"
	"wikistream_project/internal/models"
	"wikistream_project/internal/stream"

	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	if err := run(); err != nil {
		log.Printf("producer: %v", err)
		os.Exit(1)
	}
}

func run() error {
	cfg := config.LoadProducer()
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Add in new support looking up schema and building codec for protobuf
	schemaID, err := codec.LookupSchemaID(ctx, cfg.SchemaRegistryURL, config.AppendValueToSubject(cfg.Topic))
	if err != nil {
		return fmt.Errorf("resolve schema: %w", err)
	}
	encoder := codec.New(schemaID)

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.DefaultProduceTopic(cfg.Topic),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerLinger(20*time.Millisecond),
		kgo.ProducerBatchMaxBytes(1<<20),
	)
	if err != nil {
		return fmt.Errorf("create kafka client: %w", err)
	}
	defer cl.Close()

	m := metrics.NewProducer()
	metricsServer := &http.Server{
		Addr:              cfg.MetricsAddr,
		Handler:           m.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		log.Printf("metrics listening on %s", cfg.MetricsAddr)
		if err := metricsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("metrics server: %v", err)
		}
	}()
	// Gonna wrap this defer func in advance because I know the linter is gonna yell at me, and I'm sensitive
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = metricsServer.Shutdown(shutdownCtx)
	}()

	err = stream.ConsumeEventStream(ctx, &http.Client{}, cfg.StreamURL, cfg.UserAgent, func(payload []byte) {
		rec, ok := toRecord(payload, encoder, m)
		if !ok {
			return
		}
		publish(cl, m, rec)
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		return fmt.Errorf("stream stopped: %w", err)
	}

	log.Printf("shutting down, flushing buffered events (timeout %s)", cfg.FlushTimeout)
	flushCtx, cancel := context.WithTimeout(context.Background(), cfg.FlushTimeout)
	defer cancel()

	if err := cl.Flush(flushCtx); err != nil {
		return fmt.Errorf("flush buffered events: %w", err)
	}

	log.Printf("done")
	return nil
}

// decode a raw stream payload and prep it for redpanda consumption
func toRecord(payload []byte, encoder *codec.Codec, m *metrics.Producer) (*kgo.Record, bool) {
	m.StreamConsumed.Inc()

	var rc models.RecentChange
	if err := json.Unmarshal(payload, &rc); err != nil {
		m.Failed.WithLabelValues(metrics.StageDecode).Inc()
		log.Printf("skipping event that could not be decoded: %v", err)
		return nil, false
	}

	value, err := encoder.EncodeEvent(rc)
	if err != nil {
		m.Failed.WithLabelValues(metrics.StageEncode).Inc()
		log.Printf("skipping event that could not be encoded: %v", err)
		return nil, false
	}

	return &kgo.Record{Key: []byte(rc.ServerURL), Value: value}, true
}

func publish(cl *kgo.Client, m *metrics.Producer, rec *kgo.Record) {
	cl.Produce(context.Background(), rec, func(_ *kgo.Record, err error) {
		if err != nil {
			m.Failed.WithLabelValues(metrics.StageProduce).Inc()
			log.Printf("produce failed: %v", err)
			return
		}
		m.Persisted.Inc()
	})
}
