package store

import (
	"context"
	"fmt"
	"strings"

	"wikistream_project/internal/aggregate"
	"wikistream_project/internal/config"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

// Scylla Store setup
type ScyllaStore struct {
	session  *gocql.Session
	keyspace string
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
