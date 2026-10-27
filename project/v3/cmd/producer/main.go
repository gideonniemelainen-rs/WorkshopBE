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
	"sync/atomic"
	"syscall"
	"time"
	"wikistream_project/internal/codec"
	"wikistream_project/internal/config"
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

	var produced, failed atomic.Int64

	err = stream.ConsumeEventStream(ctx, &http.Client{}, cfg.StreamURL, cfg.UserAgent, func(payload []byte) {
		var rc models.RecentChange
		if err := json.Unmarshal(payload, &rc); err != nil {
			log.Printf("skipping event that could not be decoded: %v", err)
			return
		}

		value, err := encoder.EncodeEvent(rc)
		if err != nil {
			log.Printf("skipping event that could not be encoded: %v", err)
		}

		cl.Produce(context.Background(), &kgo.Record{
			Key:   []byte(rc.ServerURL),
			Value: value,
		}, func(_ *kgo.Record, err error) {
			if err != nil {
				failed.Add(1)
				log.Printf("produce failed: %v", err)
				return
			}
			produced.Add(1)
		})
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

	log.Printf("done: produced=%d failed=%d", produced.Load(), failed.Load())
	return nil
}
