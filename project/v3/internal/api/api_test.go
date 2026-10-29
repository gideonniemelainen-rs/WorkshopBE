package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"wikistream_project/internal/config"
	"wikistream_project/internal/metrics"
	"wikistream_project/internal/models"
)

func newTestHandler(authEnabled bool) http.Handler {
	cfg := config.Consumer{
		AuthEnabled: authEnabled,
		JWTSecret:   "test-secret",
		DefaultUser: "admin",
		DefaultPass: "admin",
	}
	return NewServer(cfg, models.NewServerStats(), metrics.NewConsumer()).Handler()
}

func TestMetricsEndpointIsOpenWhenAuthEnabled(t *testing.T) {
	handler := newTestHandler(true)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("/metrics without token = %d, expected 200", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "wikistream_consumer_events_processed_total") {
		t.Fatalf("/metrics body missing consumer counters")
	}
}

func TestStatusReturnsOk(t *testing.T) {
	handler := newTestHandler(true)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/status", nil))

	if rec.Code != http.StatusOK || rec.Body.String() != "Ok" {
		t.Fatalf("/status = %d %q", rec.Code, rec.Body.String())
	}
}

func TestStatsEndpointRequiresAuthWhenEnabled(t *testing.T) {
	handler := newTestHandler(true)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/stats", nil))

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("/stats without token = %d, expected 401", rec.Code)
	}
}

func TestLoginRejectsBadPassword(t *testing.T) {
	handler := newTestHandler(true)

	body, _ := json.Marshal(map[string]string{"username": "admin", "password": "lolololol"})
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/login", bytes.NewReader(body)))

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("login with bad password = %d successful, expected 401", rec.Code)
	}
}

func TestStatsEndpointOpenWhenAuthDisabled(t *testing.T) {
	handler := newTestHandler(false)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/stats", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("/stats with auth disabled = %d", rec.Code)
	}
}

func TestLoginThenStats(t *testing.T) {
	handler := newTestHandler(true)

	body, _ := json.Marshal(map[string]string{"username": "admin", "password": "admin"})
	loginRec := httptest.NewRecorder()
	handler.ServeHTTP(loginRec, httptest.NewRequest(http.MethodPost, "/login", bytes.NewReader(body)))

	if loginRec.Code != http.StatusOK {
		t.Fatalf("/login = %d: %s", loginRec.Code, loginRec.Body.String())
	}

	var output struct {
		Token    string `json:"token"`
		Username string `json:"username"`
	}

	if err := json.Unmarshal(loginRec.Body.Bytes(), &output); err != nil {
		t.Fatalf("decode login: %v", err)
	}
	if output.Token == "" || output.Username != "admin" {
		t.Fatalf("unexpected login response: %+v", output)
	}

	req := httptest.NewRequest(http.MethodGet, "/stats", nil)
	req.Header.Set("Authorization", "Bearer "+output.Token)
	statsRec := httptest.NewRecorder()
	handler.ServeHTTP(statsRec, req)
	// We don't need to check the actual object returned because those methods are tested elsewhere, this is simply for confirming auth behaviour
	if statsRec.Code != http.StatusOK {
		t.Fatalf("/stats with token = %d", statsRec.Code)
	}
}
