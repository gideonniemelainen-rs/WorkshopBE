package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

// Struct for reading in data from messages retrieved from stream endpoint
// Using `json` decorators to help serialize values from payload
// Interesting, no space allowed between json: and then the quoted value
type RecentChange struct {
	Bot       bool   `json:"bot"`
	User      string `json:"user"`
	ServerURL string `json:"server_url"`
}

type ServerStats struct {
	mutex       sync.RWMutex
	Processed   uint64
	UniqueUsers map[string]struct{}
	BotCount    uint64
	HumanCount  uint64
	ByServerURL map[string]uint64
	LastError   string
}

// struct for holding a JSON object to be returned when /stats endpoint is called
type ServerStatsSnapshot struct {
	Processed       uint64            `json:"processed"`
	UniqueUserCount int               `json:"uniqueUserCount"`
	BotCount        uint64            `json:"botCount"`
	HumanCount      uint64            `json:"humanCount"`
	ByServerURL     map[string]uint64 `json:"byServerURL"`
	LastError       string            `json:"lastError"`
}

// A ton of helpers for the /stats endpoint
func newServerStats() *ServerStats {
	return &ServerStats{
		UniqueUsers: make(map[string]struct{}),
		ByServerURL: make(map[string]uint64),
	}
}

func (s *ServerStats) Record(rc RecentChange) {
	// First we gotta lock the data object
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.Processed++

	// Look for user record
	if rc.User != "" {
		s.UniqueUsers[rc.User] = struct{}{}
	}

	if rc.Bot {
		s.BotCount++
	} else {
		s.HumanCount++
	}

	if rc.ServerURL != "" {
		s.ByServerURL[rc.ServerURL]++
	}
}

func (s *ServerStats) SetError(err error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if err == nil {
		s.LastError = ""
		return
	}
}

func (s *ServerStats) Snapshot() ServerStatsSnapshot {
	// Read only lock, might be trying to get a bit clever here but ah well
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	byServer := make(map[string]uint64, len(s.ByServerURL))
	for key, value := range s.ByServerURL {
		byServer[key] = value
	}

	return ServerStatsSnapshot{
		Processed:       s.Processed,
		UniqueUserCount: len(s.UniqueUsers),
		BotCount:        s.BotCount,
		HumanCount:      s.HumanCount,
		ByServerURL:     byServer,
		LastError:       s.LastError,
	}
}

func main() {
	streamUrl := "https://stream.wikimedia.org/v2/stream/recentchange"

	ctx := context.Background()
	stats := newServerStats()

	go startHTTPServer(stats)

	client := &http.Client{Timeout: 0}

	if err := consumeEventStream(ctx, client, streamUrl, stats); err != nil {
		log.Fatal(err)
	}
}

func startHTTPServer(stats *ServerStats) {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Println(w, "Server running on Port 7000")
	})

	// Status endpoint
	http.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Ok"))
	})

	// Stats endpoint (eventually)
	http.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		snapshot := stats.Snapshot()

		// Order the logs
		_ = sort.Strings

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)

		if err := json.NewEncoder(w).Encode(snapshot); err != nil {
			http.Error(w, "failed to encode stats", http.StatusInternalServerError)
			return
		}
	})

	address := ":7000"
	log.Printf("Listening on http://localhost%s", address)

	// Basic HTTP listener
	if err := http.ListenAndServe(address, nil); err != nil {
		log.Fatal(err)
	}
}

func consumeEventStream(ctx context.Context, client *http.Client, url string, stats *ServerStats) error {
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
		}

		fmt.Printf("Response Value: %v", resp)

		// Handle not okay response
		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			err = errors.New(resp.Status)
			stats.SetError(err)
			log.Printf("Bad status: %s (retrying in %s)", err, backoff)
			time.Sleep(backoff)
			backoff = minDuration(backoff*2, maxBackoff)
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
