package stream

import (
	"bufio"
	"context"
	"errors"
	"log"
	"net/http"
	"strings"
	"time"
)

const (
	initialBackoff = time.Second
	maxBackoff     = 30 * time.Second
	maxLineBytes   = 2 * 1024 * 1024
)

func ConsumeEventStream(ctx context.Context, client *http.Client, url string, userAgent string, onEvent func(payload []byte)) error {
	// Set up tracking id and sensible limits to avoid hammering the API
	var lastEventID string
	backoff := initialBackoff

	for {
		// check if context is in error or not
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// Form request object
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return err
		}

		// Set request headers
		req.Header.Set("Accept", "text/event-stream")
		req.Header.Set("Cache-Control", "no-cache")
		// need to set a user agent header apparently, as per API spec
		req.Header.Set("User-Agent", "GoLearningStreamsDemo/0.1 (gideon.niemelainen@redspace.com)")
		// Another header for ID tracking, because why not
		if lastEventID != "" {
			req.Header.Set("Last-Event-ID", lastEventID)
		}

		// Make the request
		resp, err := client.Do(req)
		// Handle generic error
		if err != nil {
			if !sleep(ctx, backoff, "connect error", err) {
				return ctx.Err()
			}
			// Exponential backoff
			backoff = minDuration(backoff*2, maxBackoff)
			continue
		}

		// Handle not okay response
		if resp.StatusCode != http.StatusOK {
			_ = resp.Body.Close()
			if !sleep(ctx, backoff, "connect error", errors.New(resp.Status)) {
				return ctx.Err()
			}
			// Exponential backoff
			backoff = minDuration(backoff*2, maxBackoff)
			continue
		}

		// reset backoff if everything okay
		backoff = initialBackoff

		lastEventID = readEvents(resp, lastEventID, onEvent)
		_ = resp.Body.Close()

		if err := ctx.Err(); err != nil {
			return err
		}

		if !sleep(ctx, backoff, "stream closed", nil) {
			return ctx.Err()
		}
		backoff = minDuration(backoff*2, maxBackoff)
	}
}

func readEvents(resp *http.Response, lastEventID string, onEvent func(payload []byte)) string {
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 64*10124), maxLineBytes)

	var data []string
	for scanner.Scan() {
		line := scanner.Text()

		switch {
		case line == "":
			if len(data) > 0 {
				onEvent([]byte(strings.Join(data, "\n")))
				data = data[:0]
			}
		case strings.HasPrefix(line, ":"):
			continue
		case strings.HasPrefix(line, "id:"):
			lastEventID = strings.TrimSpace(line[len("id:"):])
			continue
		case strings.HasPrefix(line, "data:"):
			data = append(data, strings.TrimSpace(line[len("data:"):]))
			continue
		}
	}

	if err := scanner.Err(); err != nil {
		log.Printf("stream read error: %v", err)
	}

	return lastEventID
}

func sleep(ctx context.Context, d time.Duration, reason string, err error) bool {
	if err != nil {
		log.Printf("%s: %v (retrying in %s)", reason, err, d)
	} else {
		log.Printf("%s (reconnecting in %s)", reason, d)
	}

	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// Helper for normalizing sleep duration
func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}

	return b
}
