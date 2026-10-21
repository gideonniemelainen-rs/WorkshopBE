package models

import "testing"

func TestServerStatsRecordAndSnapshot(t *testing.T) {
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
