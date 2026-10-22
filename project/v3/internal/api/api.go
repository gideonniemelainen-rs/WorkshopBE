package api

import (
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"wikistream_project/internal/config"
	"wikistream_project/internal/models"
	"wikistream_project/internal/services"
)

type Server struct {
	cfg   config.Consumer
	auth  *services.AuthService
	stats *models.ServerStats
}

// Small handler to run on it
func NewServer(cfg config.Consumer, stats *models.ServerStats) *Server {
	users := map[string]string{
		cfg.DefaultUser: cfg.DefaultPass,
	}

	return &Server{
		cfg:   cfg,
		auth:  services.NewAuthService(cfg.JWTSecret, users),
		stats: stats,
	}
}

// build the server
func (s *Server) Handler() http.Handler {
	// This is apparently a much better way to handle serving an app, compared to using the default mux. Should make testing easier too, based on what I've read?
	mux := http.NewServeMux()

	// Status endpoint, lifted from v1
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("Ok"))
	})

	// Stats endpoint, a bit simplified from previous
	mux.HandleFunc("/stats", s.requireAuth(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		snapshot := s.stats.Snapshot()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(snapshot)

	}))

	// new auth endpoint
	mux.HandleFunc("/login", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var required struct {
			Username string `json:"username"`
			Password string `json:"password"`
		}

		if err := json.NewDecoder(r.Body).Decode(&required); err != nil {
			http.Error(w, "invalid request body", http.StatusBadRequest)
			return
		}

		if required.Username == "" || required.Password == "" {
			http.Error(w, "username and password required", http.StatusBadRequest)
			return
		}

		if !s.auth.CheckPassword(required.Username, required.Password) {
			http.Error(w, "invalid username or password", http.StatusUnauthorized)
			return
		}

		token, err := s.auth.IssueToken(required.Username)
		if err != nil {
			http.Error(w, "could not issue token", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"token":    token,
			"username": required.Username,
		})
	})

	return mux
}

// Middleware for auth layer as per spec
func (s *Server) requireAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		log.Println("Authorization headerL ", r.Header.Get("Authorization"))
		if !s.cfg.AuthEnabled {
			next(w, r)
			return
		}

		authHeader := r.Header.Get("Authorization")
		if authHeader == "" {
			w.Header().Set("WWW-Authenticate", `Bearer realm="wikistream"`)
			http.Error(w, "authorization required", http.StatusUnauthorized)
			return
		}
		// check for bearer token
		parts := strings.SplitN(authHeader, " ", 2)
		// EqualFold is kinda neat as a way to normalize strings for comparison
		if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
			w.Header().Set("WWW-Authenticate", `Bearer realm="wikistream"`)
			http.Error(w, "authorization required", http.StatusUnauthorized)
			return
		}

		_, ok := s.auth.ValidateToken(parts[1])
		if !ok {
			w.Header().Set("WWW-Authenticate", `Bearer realm="wikistream"`)
			http.Error(w, "authorization required", http.StatusUnauthorized)
			return
		}
		// TODO cleanup? Might be worth spinning up a tiny rejectAuth helper to DRY this out^^

		next(w, r)
	}
}
