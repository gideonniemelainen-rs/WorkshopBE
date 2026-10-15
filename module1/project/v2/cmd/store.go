package main

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
	keyspace string
	hosts    []string
}

func newScyllaStore(cfg Config) (*ScyllaStore, error) {
	return &ScyllaStore{
		keyspace: cfg.ScyllaKeyspace,
		hosts:    cfg.ScyllaHosts,
	}, nil
}

func (s *ScyllaStore) Record(rc RecentChange) {
	// No Op currently, just satisfying interface
}

func (s *ScyllaStore) SetError(err error) {
	// same as above
}

func (s *ScyllaStore) Snapshot() ServerStatsSnapshot {
	return ServerStatsSnapshot{}
}

func (s *ScyllaStore) Flush() error {
	// This is the main reason the interface needs a flush method, since we'll likely need to aggregate and batch datapoints for time-series workload
	return nil
}

func (s *ScyllaStore) Close() error {
	return nil
}
