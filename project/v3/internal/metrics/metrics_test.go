package metrics

import (
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
)

func scrape(t *testing.T, h http.Handler) string {
	t.Helper()

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("/metrics = %d", rec.Code)
	}
	return rec.Body.String()
}

// Metric names are the contract with the Grafana dashboard, so they are asserted exactly
func TestProducerExposesNamesAtZero(t *testing.T) {
	out := scrape(t, NewProducer().Handler())

	for _, want := range []string{
		"wikistream_producer_stream_events_consumed_total 0",
		"wikistream_producer_events_persisted_total 0",
		`wikistream_producer_events_failed_total{stage="decode"} 0`,
		`wikistream_producer_events_failed_total{stage="encode"} 0`,
		`wikistream_producer_events_failed_total{stage="produce"} 0`,
	} {
		if !contains(out, want) {
			t.Errorf("producer /metrics missing %q", want)
		}
	}
}

func TestProducerCountersReflectIncrements(t *testing.T) {
	p := NewProducer()
	p.StreamConsumed.Add(3)
	p.Persisted.Inc()
	p.Failed.WithLabelValues(StageProduce).Inc()

	out := scrape(t, p.Handler())
	for _, want := range []string{
		"wikistream_producer_stream_events_consumed_total 3",
		"wikistream_producer_events_persisted_total 1",
		`wikistream_producer_events_failed_total{stage="produce"} 1`,
	} {
		if !contains(out, want) {
			t.Errorf("producer /metrics missing %q", want)
		}
	}
}

func TestConsumerExposesNamesAtZero(t *testing.T) {
	out := scrape(t, NewConsumer().Handler())

	for _, want := range []string{
		"wikistream_consumer_redpanda_events_consumed_total 0",
		"wikistream_consumer_events_processed_total 0",
		`wikistream_consumer_events_failed_total{stage="decode"} 0`,
		`wikistream_consumer_events_failed_total{stage="write"} 0`,
		`wikistream_consumer_events_failed_total{stage="commit"} 0`,
	} {
		if !contains(out, want) {
			t.Errorf("consumer /metrics missing %q", want)
		}
	}
}

func TestConsumerCountersReflectIncrements(t *testing.T) {
	c := NewConsumer()
	c.RedpandaConsumed.Add(5)
	c.Processed.Add(4)
	c.Failed.WithLabelValues(StageDecode).Inc()

	out := scrape(t, c.Handler())
	for _, want := range []string{
		"wikistream_consumer_redpanda_events_consumed_total 5",
		"wikistream_consumer_events_processed_total 4",
		`wikistream_consumer_events_failed_total{stage="decode"} 1`,
	} {
		if !contains(out, want) {
			t.Errorf("consumer /metrics missing %q", want)
		}
	}
}

// contains checks for a full exposition line, so HELP text cannot satisfy a match
func contains(body, line string) bool {
	return slices.Contains(strings.Split(body, "\n"), line)
}
