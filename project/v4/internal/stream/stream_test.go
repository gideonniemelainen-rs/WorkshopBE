package stream

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestConsumeEventStreamSendsConfiguredUserAgent(t *testing.T) {
	const userAgent = "wikistream-test/1.0 (ops@example.com)"

	seen := make(chan string, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case seen <- r.Header.Get("User-Agent"):
		default:
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- ConsumeEventStream(ctx, srv.Client(), srv.URL, userAgent, func([]byte) {})
	}()

	select {
	case got := <-seen:
		if got != userAgent {
			t.Fatalf("User-Agent = %q, expected %q", got, userAgent)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no request reached the stream server")
	}

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("ConsumeEventStream did not return after cancel")
	}
}
