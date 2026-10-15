package main

import (
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

// Struct for supporting Time series data in Scylla db eventually
type StatsBucket struct {
	BucketStart time.Time
	MetricName  string
	InstanceID  string
	Processed   uint64
	UniqueUsers uint64
	BotCount    uint64
	HumanCount  uint64
	ServerURL   string
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
