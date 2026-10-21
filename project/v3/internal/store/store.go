package store

import (
	"fmt"
	"strings"
	"sync"
	"time"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

// Shared Interface between in-memory and scylla stores. Due to the use case of supporting eventual time-series reporting, the actual implementations will look quite different
type Store interface {
	Record(RecentChange)
	SetError(error)
	Snapshot() ServerStatsSnapshot
	Flush() error
	Close() error
}

// In-Memory Store setup
type MemoryStore struct {
	stats *ServerStats
}

func newMemoryStore() *MemoryStore {
	return &MemoryStore{stats: newServerStats()}
}

func (m *MemoryStore) Record(rc RecentChange) {
	m.stats.Record(rc)
}

func (m *MemoryStore) SetError(err error) {
	m.stats.SetError(err)
}

func (m *MemoryStore) Snapshot() ServerStatsSnapshot {
	return m.stats.Snapshot()
}

func (m *MemoryStore) Flush() error {
	return nil
}

func (m *MemoryStore) Close() error {
	return nil
}

// Scylla Store setup
type ScyllaStore struct {
	session    *gocql.Session
	keyspace   string
	instanceID string
	mutex      sync.Mutex
	stats      *ServerStats
}

func newScyllaStore(cfg Config) (*ScyllaStore, error) {
	consistency, err := parseConsistency(cfg.ScyllaConsistency)
	if err != nil {
		return nil, err
	}

	hosts := normalizeHosts(cfg.ScyllaHosts)

	// Set up scylla bootstrap cluster to configure keyspace if necessary
	bootstrap := gocql.NewCluster(hosts...)
	bootstrap.Timeout = cfg.ScyllaTimeout
	bootstrap.ConnectTimeout = cfg.ScyllaTimeout
	bootstrap.Consistency = consistency
	bootstrap.DisableInitialHostLookup = true

	if cfg.ScyllaUsername != "" {
		bootstrap.Authenticator = gocql.PasswordAuthenticator{
			Username: cfg.ScyllaUsername,
			Password: cfg.ScyllaPassword,
		}
	}

	bootstrapSession, err := bootstrap.CreateSession()
	if err != nil {
		return nil, fmt.Errorf("create store session: %w", err)
	}
	defer bootstrapSession.Close()

	// check if keyspace exists and create if not
	keyspace := sanitizeKeyspace(cfg.ScyllaKeyspace)
	createKeyspaceQuery := fmt.Sprintf(
		"CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = {'class': 'NetworkTopologyStrategy', 'datacenter1': 1}",
		keyspace,
	)

	if err := bootstrapSession.Query(createKeyspaceQuery).Exec(); err != nil {
		return nil, fmt.Errorf("create keyspace %s: %w", keyspace, err)
	}

	// Once we've confirmed that keyspace exists, spin up actual cluster
	store := gocql.NewCluster(hosts...)
	store.Keyspace = keyspace
	store.Timeout = cfg.ScyllaTimeout
	store.ConnectTimeout = cfg.ScyllaTimeout
	store.Consistency = consistency
	store.DisableInitialHostLookup = true

	if cfg.ScyllaUsername != "" {
		store.Authenticator = gocql.PasswordAuthenticator{
			Username: cfg.ScyllaUsername,
			Password: cfg.ScyllaPassword,
		}
	}

	storeSession, err := store.CreateSession()
	if err != nil {
		return nil, fmt.Errorf("create session error: %w", err)
	}

	s := &ScyllaStore{
		session:    storeSession,
		keyspace:   keyspace,
		instanceID: "local-dev",
		stats:      newServerStats(),
	}

	if err := s.initSchema(); err != nil {
		storeSession.Close()
		return nil, err
	}

	return s, nil
}

func (s *ScyllaStore) initSchema() error {
	const schema = `
	CREATE TABLE IF NOT EXISTS server_stats_by_minute (
		bucket_start text,
		metric_name text,
		instance_id text,
		ts timestamp,
		processed bigint,
		unique_users bigint,
		bot_count bigint,
		human_count bigint,
		by_server_url map<text, bigint>,
		last_error text,
		PRIMARY KEY ((bucket_start, metric_name, instance_id), ts)
	)`

	if err := s.session.Query(schema).Exec(); err != nil {
		return fmt.Errorf("create server_stats_by_minute table error: %w", err)
	}
	return nil
}

func (s *ScyllaStore) Record(rc RecentChange) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.stats.Record(rc)
}

func (s *ScyllaStore) SetError(err error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.stats.SetError(err)
}

func (s *ScyllaStore) Snapshot() ServerStatsSnapshot {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.stats.Snapshot()
}

func (s *ScyllaStore) Flush() error {
	// This is the main reason the interface needs a flush method, since we'll likely need to aggregate and batch datapoints for time-series workload
	s.mutex.Lock()
	snapshot := s.stats.Snapshot()
	s.stats = newServerStats()
	s.mutex.Unlock()

	// Return nil if there are no stats to be flushed
	if snapshot.Processed == 0 &&
		snapshot.UniqueUserCount == 0 &&
		snapshot.BotCount == 0 &&
		snapshot.HumanCount == 0 &&
		len(snapshot.ByServerURL) == 0 &&
		snapshot.LastError == "" {
		return nil
	}

	now := time.Now().UTC()
	bucket := now.Truncate(time.Minute).Format(time.RFC3339) // Preferring format of RFC3339 specifically, looks to be timezoned which is helpful

	// aggregate statistics to prep for write
	byServerCounts := make(map[string]int64, len(snapshot.ByServerURL))
	for k, v := range snapshot.ByServerURL {
		byServerCounts[k] = int64(v)
	}

	// Build query to insert records
	const insertQuery = `
	INSERT INTO server_stats_by_minute (
		bucket_start, metric_name, instance_id, ts, processed, unique_users, bot_count, human_count, by_server_url, last_error
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	if err := s.session.Query(
		insertQuery,
		bucket,
		"recentchange",
		s.instanceID,
		now,
		int64(snapshot.Processed),
		int64(snapshot.UniqueUserCount),
		int64(snapshot.BotCount),
		int64(snapshot.HumanCount),
		byServerCounts,
		snapshot.LastError,
	).Exec(); err != nil {
		return fmt.Errorf("insert into stats bucket error: %w", err)
	}

	return nil
}

func (s *ScyllaStore) Close() error {
	if s.session != nil {
		s.session.Close()
	}
	return nil
}

// Helpers for Scylla Store
func normalizeHosts(hosts []string) []string {
	normalizedHosts := make([]string, 0, len(hosts))
	for _, h := range hosts {
		h = strings.TrimSpace(h)
		if h != "" {
			normalizedHosts = append(normalizedHosts, h)
		}
	}

	if len(normalizedHosts) == 0 {
		// Return loopback if no hosts provided explicitly
		return []string{"127.0.0.1"}
	}

	return normalizedHosts
}

// Helper to return proper consistency value depending on config
func parseConsistency(raw string) (gocql.Consistency, error) {
	switch strings.ToUpper(strings.TrimSpace(raw)) {
	case "", "LOCAL_QUORUM":
		return gocql.LocalQuorum, nil
	case "ONE":
		return gocql.One, nil
	case "QUORUM":
		return gocql.Quorum, nil
	case "LOCAL_ONE":
		return gocql.LocalOne, nil
	case "ALL":
		return gocql.All, nil
	default:
		return gocql.LocalQuorum, fmt.Errorf("unsupported consistency value: %q", raw)
	}
}

func sanitizeKeyspace(raw string) string {
	key := strings.TrimSpace(raw)
	if key == "" {
		return "wikistream"
	}
	return strings.ReplaceAll(key, "-", "_")
}
