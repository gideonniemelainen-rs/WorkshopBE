package aggregate

import (
	"sort"
	"time"
	"wikistream_project/internal/models"
)

const MetricName = "recentchange"

const UnknownBucket = "unknown"

// Need to define an event struct for what is going to be produced and consumed
type Event struct {
	Partition int32
	Offset    int64
	Change    models.RecentChange
}

type Row struct {
	MetricName  string
	BucketStart string
	Partition   int32
	FirstOffset int64
	LastOffset  int64
	Processed   int64
	UniqueUsers int64
	BotCount    int64
	HumanCount  int64
	ByServerURL map[string]int64
}

func Bucket(ts int64) string {
	if ts <= 0 {
		return UnknownBucket
	}

	return time.Unix(ts, 0).UTC().Truncate(time.Minute).Format(time.RFC3339)
}

type groupKey struct {
	bucket    string
	partition int32
}

type accumulator struct {
	firstOffset int64
	lastOffset  int64
	processed   int64
	bots        int64
	humans      int64
	users       map[string]struct{}
	byServer    map[string]int64
}

// Build grouped events into rows to be inserted into the db. the "aggregation" of the module name, if you will
func Build(metric string, events []Event) []Row {
	groups := make(map[groupKey]*accumulator)

	for _, e := range events {
		key := groupKey{bucket: Bucket(e.Change.Timestamp), partition: e.Partition}

		acc, ok := groups[key]
		if !ok {
			acc = &accumulator{
				firstOffset: e.Offset,
				lastOffset:  e.Offset,
				users:       make(map[string]struct{}),
				byServer:    make(map[string]int64),
			}
			groups[key] = acc
		}

		if e.Offset < acc.firstOffset {
			acc.firstOffset = e.Offset
		}
		if e.Offset > acc.lastOffset {
			acc.lastOffset = e.Offset
		}

		acc.processed++
		if e.Change.Bot {
			acc.bots++
		} else {
			acc.humans++
		}

		if e.Change.User != "" {
			acc.users[e.Change.User] = struct{}{}
		}
		if e.Change.ServerURL != "" {
			acc.byServer[e.Change.ServerURL]++
		}
	}

	// Start assembling the row records from aggregated data
	rows := make([]Row, 0, len(groups))
	for key, acc := range groups {
		rows = append(rows, Row{
			MetricName:  metric,
			BucketStart: key.bucket,
			Partition:   key.partition,
			FirstOffset: acc.firstOffset,
			LastOffset:  acc.lastOffset,
			Processed:   acc.processed,
			UniqueUsers: int64(len(acc.users)),
			BotCount:    acc.bots,
			HumanCount:  acc.humans,
			ByServerURL: acc.byServer,
		})
	}

	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Partition != rows[j].Partition {
			return rows[i].Partition < rows[j].Partition
		}
		if rows[i].FirstOffset != rows[j].FirstOffset {
			return rows[i].FirstOffset < rows[j].FirstOffset
		}
		return rows[i].BucketStart < rows[j].BucketStart
	})

	return rows
}
