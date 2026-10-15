package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestStatsRecordAndSnapshot(t *testing.T) {
	stats := newServerStats()
	stats.Record(RecentChange{User: "G-Diddy", Bot: false, ServerURL: "https://en.wikipedia.org"})
	stats.Record(RecentChange{User: "Thelonious", Bot: true, ServerURL: "https://en.wikipedia.org"})
	stats.Record(RecentChange{User: "Thelonious", Bot: false, ServerURL: "https://commons.wikipedia.org"})

	snapshot := stats.Snapshot()

	if snapshot.Processed != 3 {
		t.Fatalf("Records Processed = %d, expected 3", snapshot.Processed)
	}
	if snapshot.UniqueUserCount != 2 {
		t.Fatalf("UniqueUserCount = %d, expected 2", snapshot.UniqueUserCount)
	}
	if snapshot.BotCount != 1 {
		t.Fatalf("Bot Count = %d, expected 1", snapshot.BotCount)
	}
	if snapshot.HumanCount != 2 {
		t.Fatalf("Human Count = %d, expected 2", snapshot.HumanCount)
	}
	if snapshot.ByServerURL["https://en.wikipedia.org"] != 2 {
		t.Fatalf("Server count for https://en.wikipedia.org = %d, expected 2", snapshot.ByServerURL["https://en.wikipedia.org"])
	}
	if snapshot.ByServerURL["https://commons.wikipedia.org"] != 1 {
		t.Fatalf("Server count for https://commons.wikipedia.org = %d, expected 1", snapshot.ByServerURL["https://commons.wikipedia.org"])
	}
}

func TestStatusEndpoint(t *testing.T) {
	stats := newServerStats()

	req := httptest.NewRequest(http.MethodGet, "/status", nil)
	rr := httptest.NewRecorder()

	startHTTPServerForTests(stats).ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("Status = %d, want %d", rr.Code, http.StatusOK)
	}
	if body := rr.Body.String(); body != "ok\n" {
		t.Fatalf("Body = %q, want %q", body, "ok\n")
	}
}

// Really kinda played myself with those almost identical endpoint names, should done /health
func TestStatsEndpoint(t *testing.T) {
	stats := newServerStats()
	stats.Record(RecentChange{User: "G-Diddy", Bot: true, ServerURL: "https://en.wikipedia.org"})
	stats.Record(RecentChange{User: "Thelonious", Bot: false, ServerURL: "https://commons.wikipedia.org"})

	req := httptest.NewRequest(http.MethodGet, "/stats", nil)
	rr := httptest.NewRecorder()

	startHTTPServerForTests(stats).ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("Status = %d, want %d", rr.Code, http.StatusOK)
	}
	var snapshot ServerStatsSnapshot
	if err := json.Unmarshal(rr.Body.Bytes(), &snapshot); err != nil {
		t.Fatalf("decode json error: %v", err)
	}
	if snapshot.Processed != 2 {
		t.Fatalf("Records Processed = %d, expected 2", snapshot.Processed)
	}
	if snapshot.UniqueUserCount != 2 {
		t.Fatalf("UniqueUserCount = %d, expected 2", snapshot.UniqueUserCount)
	}

}

// probably a clumsy way to handle it, kinda like mocking a module when testing in JS?
func startHTTPServerForTests(stats *ServerStats) http.Handler {
	http.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})

	http.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(stats.Snapshot())
	})

	return http.DefaultServeMux
}
