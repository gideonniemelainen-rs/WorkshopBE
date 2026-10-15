package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"
)

func consumeEventStream(ctx context.Context, client *http.Client, url string, userAgent string, stats *ServerStats) error {
	// Set up tracking id and sensible limits to avoid hammering the API
	var lastEventID string
	backoff := time.Second
	maxBackoff := time.Second * 30

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
			stats.SetError(err)
			log.Printf("Connect error: %v (retrying in %s)", err, backoff)
			// sleep thread (just like in the course!)
			time.Sleep(backoff)
			// Expontential backoff
			backoff = minDuration(backoff*2, maxBackoff)
			continue
		}

		// Handle not okay response
		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			err = errors.New(resp.Status)
			stats.SetError(err)
			log.Printf("Bad status: %s (retrying in %s)", err, backoff)
			time.Sleep(backoff)
			backoff = minDuration(backoff*2, maxBackoff)
			continue
		}

		// reset backoff if everything okay
		backoff = time.Second
		stats.SetError(nil)

		// create a scanner to read response body
		scanner := bufio.NewScanner(resp.Body)
		scanner.Buffer(make([]byte, 0, 64*1024), 2*1024*1024)

		var lines []string

		// iterate through scanner and body contents
		for scanner.Scan() {
			line := scanner.Text()

			if line == "" {
				if len(lines) > 0 {
					payload := strings.Join(lines, "\n")
					log.Printf("Incoming message: %s", payload)

					var rc RecentChange
					if err := json.Unmarshal([]byte(payload), &rc); err != nil {
						stats.SetError(err)
						log.Printf("json decode error: %v", err)
					} else {
						stats.Record(rc)
					}
					lines = lines[:0]
				}
				continue
			}

			if strings.HasPrefix(line, ":") {
				continue
			}

			if strings.HasPrefix(line, "id:") {
				lastEventID = strings.TrimSpace(line[len("id:"):])
				continue
			}

			if strings.HasPrefix(line, "data:") {
				data := strings.TrimSpace(line[len("data:"):])
				lines = append(lines, data)
				continue
			}

		}

		resp.Body.Close()

		if ctx.Err() != nil {
			return ctx.Err()
		}

		if err := scanner.Err(); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("stream read error: %v (reconnecting)", err)
		} else {
			log.Printf("stream closed (reconnecting)")
		}

		time.Sleep(backoff)
		backoff = minDuration(backoff*2, maxBackoff)
	}
}

func handleEvent(data []byte) error {
	var recent_change RecentChange
	if err := json.Unmarshal(data, &recent_change); err != nil {
		return fmt.Errorf("json decode: %w", err)
	}

	log.Printf("user=%q wiki=%q bot=%t", recent_change.User, recent_change.ServerURL, recent_change.Bot)
	return nil
}

// Helper for normalizing sleep duration
func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}

	return b
}
