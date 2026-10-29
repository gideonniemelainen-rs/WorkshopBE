package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
	"wikistream_project/internal/api"
	"wikistream_project/internal/codec"
	"wikistream_project/internal/config"
	"wikistream_project/internal/consumer"
	"wikistream_project/internal/metrics"
	"wikistream_project/internal/models"
	"wikistream_project/internal/store"
)

func main() {
	if err := run(); err != nil {
		log.Printf("consumer: %v", err)
		os.Exit(1)
	}
}

func run() error {
	cfg := config.LoadConsumer()
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	schemaID, err := codec.LookupSchemaID(ctx, cfg.SchemaRegistryURL, config.AppendValueToSubject(cfg.Topic))
	if err != nil {
		return fmt.Errorf("resolve schema: %w", err)
	}
	decoder := codec.New(schemaID)

	sink, err := store.NewScyllaStore(cfg)
	if err != nil {
		return fmt.Errorf("connect to scylla: %w", err)
	}
	defer func() { _ = sink.Close() }()

	stats := models.NewServerStats()
	m := metrics.NewConsumer()

	httpServer := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           api.NewServer(cfg, stats, m).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	serverError := make(chan error, 1)
	go func() {
		log.Printf("HTTP listening on %s", cfg.HTTPAddr)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverError <- err
			stop()
		}
	}()

	runError := consumer.Run(ctx, cfg, sink, stats, decoder.DecodeEvent, m)

	stop()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("http shutdown: %v", err)
	}

	select {
	case err := <-serverError:
		return errors.Join(runError, fmt.Errorf("http server: %w", err))
	default:
	}

	if runError != nil {
		return fmt.Errorf("consumer stopped: %w", runError)
	}
	log.Printf("consumer stopped cleanly")
	return nil
}
