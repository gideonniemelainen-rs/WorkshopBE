package store

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"wikistream_project/internal/aggregate"
	"wikistream_project/internal/config"
	"wikistream_project/internal/models"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

// Scylla Store setup
type ScyllaStore struct {
	session    *gocql.Session
	keyspace   string
	instanceID string
	mutex      sync.Mutex
	stats      *models.ServerStats
}

const createStatsBatchesTable = `
	CREATE TABLE IF NOT EXISTS server_stats_batches (
		metric_name text,
		bucket_start text,
		partition int,
		first_offset bigint,
		last_offset bigint,
		processed bigint,
		unique_users bigint,
		bot_count bigint,
		human_count bigint,
		by_server_url map<text, bigint>,
		PRIMARY KEY ((metric_name, bucket_start), partition, first_offset)
	)`

const insertBatchRecord = `
INSERT INTO server_stats_batches (
	metric_name, bucket_start, partition, first_offset, last_offset,
	processed, unique_users, bot_count, human_count, by_server_url
) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

func NewScyllaStore(cfg config.Consumer) (*ScyllaStore, error) {
	consistency, err := parseConsistency(cfg.ScyllaConsistency)
	if err != nil {
		return nil, err
	}

	hosts := normalizeHosts(cfg.ScyllaHosts)
	keyspace := sanitizeKeyspace(cfg.ScyllaKeyspace)

	bootstrap := configureNewCluster(hosts, "", cfg, consistency)
	bootstrapSession, err := bootstrap.CreateSession()
	if err != nil {
		return nil, fmt.Errorf("create bootstrap session: %w", err)
	}

	createKeyspace := fmt.Sprintf(
		"CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = {'class': 'NetworkTopologyStrategy', 'datacenter1': 1}",
		keyspace,
	)
	err = bootstrapSession.Query(createKeyspace).Exec()
	bootstrapSession.Close()
	if err != nil {
		return nil, fmt.Errorf("create keyspace: %s: %w", keyspace, err)
	}

	session, err := configureNewCluster(hosts, keyspace, cfg, consistency).CreateSession()
	if err != nil {
		return nil, fmt.Errorf("create session %w", err)
	}

	if err := session.Query(createStatsBatchesTable).Exec(); err != nil {
		session.Close()
		return nil, fmt.Errorf("create server_stats_batches table: %w", err)
	}

	return &ScyllaStore{session: session, keyspace: keyspace}, nil
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

func (s *ScyllaStore) Record(rc models.RecentChange) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.stats.Record(rc)
}

func (s *ScyllaStore) SetError(err error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.stats.SetError(err)
}

func (s *ScyllaStore) Snapshot() models.ServerStatsSnapshot {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.stats.Snapshot()
}

func (s *ScyllaStore) Flush() error {
	// This is the main reason the interface needs a flush method, since we'll likely need to aggregate and batch datapoints for time-series workload
	s.mutex.Lock()
	snapshot := s.stats.Snapshot()
	s.stats = models.NewServerStats()
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

func (s *ScyllaStore) WriteBatchRecord(ctx context.Context, rows []aggregate.Row) error {
	for _, r := range rows {
		err := s.session.Query(
			insertBatchRecord,
			r.MetricName,
			r.BucketStart,
			r.Partition,
			r.FirstOffset,
			r.LastOffset,
			r.Processed,
			r.UniqueUsers,
			r.BotCount,
			r.HumanCount,
			r.ByServerURL,
		).ExecContext(ctx)
		if err != nil {
			return fmt.Errorf("insert batch partion=%d first_offset=%d: %w", r.Partition, r.FirstOffset, err)
		}
	}
	return nil
}

func (s *ScyllaStore) Close() error {
	if s.session != nil {
		s.session.Close()
	}
	return nil
}

func configureNewCluster(hosts []string, keyspace string, cfg config.Consumer, consistency gocql.Consistency) *gocql.ClusterConfig {
	cluster := gocql.NewCluster(hosts...)
	cluster.Keyspace = keyspace
	cluster.Timeout = cfg.ScyllaTimeout
	cluster.ConnectTimeout = cfg.ScyllaTimeout
	cluster.Consistency = consistency
	cluster.DisableInitialHostLookup = true
	if cfg.ScyllaUsername != "" {
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: cfg.ScyllaUsername,
			Password: cfg.ScyllaPassword,
		}
	}

	return cluster
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
