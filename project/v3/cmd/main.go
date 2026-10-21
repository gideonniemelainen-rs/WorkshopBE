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
	cfg := config.loadConfig()
	if err := cfg.Validate(); err != nil {
		log.Fatalf("invalid config: %v", err)
	}

	// Init app using new app helper
	app, err := newApp(cfg)
	if err != nil {
		log.Fatalf("failed to create app: %v", err)
	}

	// I tried setting up a shutdown signal but did not work as intended.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		if err := consumeEventStream(ctx, &http.Client{Timeout: 0}, cfg.StreamURL, cfg.UserAgent, app.store); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("stream consumer error: %v", err)
		}
	}()

	go app.startFlushLoop(ctx, 30*time.Second)

	httpServer := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           app.router(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	// Continued attempts at implementing graceful shutdown
	go func() {
		<-ctx.Done()
		log.Println("Shutting down server")

		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			log.Printf("Server shutdown error")
		}
	}()

	log.Printf("HTTP Server listening on %s", cfg.HTTPAddr)
	if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}

}
