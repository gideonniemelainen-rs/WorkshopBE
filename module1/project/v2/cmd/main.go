package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	cfg := loadConfig()
	app := newApp(cfg)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		if err := consumeEventStream(ctx, &http.Client{Timeout: 0}, cfg.StreamURL, cfg.UserAgent, app.stats); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("stream consumer error: %v", err)
		}
	}()

	log.Printf("HTTP Server listening on %s", cfg.HTTPAddr)
	if err := http.ListenAndServe(cfg.HTTPAddr, app.router()); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}

}
