package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	// Load configuration on startup
	cfg := loadConfig()
	if err := cfg.Validate(); err != nil {
		log.Fatalf("invalid config: %v", err)
	}

	// Init app using new app helper
	app, err := newApp(cfg)
	if err != nil {
		log.Fatalf("failed to create app: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		if err := consumeEventStream(ctx, &http.Client{Timeout: 0}, cfg.StreamURL, cfg.UserAgent, app.stats); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("stream consumer error: %v", err)
		}
	}()

	go app.startFlushLoop(ctx, 30*time.Second)

	log.Printf("HTTP Server listening on %s", cfg.HTTPAddr)
	if err := http.ListenAndServe(cfg.HTTPAddr, app.router()); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}

}
