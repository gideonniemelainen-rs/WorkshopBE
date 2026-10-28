package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Module to set up reading of metrics for Grafana/Prometheus dashboards

const (
	StageDecode  = "decode"
	StageEncode  = "encode"
	StageProduce = "produce"
	StageWrite   = "write"
	StageCommit  = "commit"
)

type Producer struct {
	registry       *prometheus.Registry
	StreamConsumed prometheus.Counter
	Persisted      prometheus.Counter
	Failed         *prometheus.CounterVec
}

func NewProducer() *Producer {
	p := &Producer{
		registry: prometheus.NewRegistry(),
		StreamConsumed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "wikistream_producer_stream_events_consumed_total",
			Help: "Events read from the wikimedia recentchange stream",
		}),
		Persisted: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "wikistream_producer_events_persisted_total",
			Help: "Events acknowledged by RedPanda",
		}),
		Failed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wikistream_producer_events_failed_total",
			Help: "Events that failed in the Producer, by stage.",
		}, []string{"stage"}),
	}
	p.registry.MustRegister(p.StreamConsumed, p.Persisted, p.Failed)
	for _, stage := range []string{StageDecode, StageEncode, StageProduce} {
		p.Failed.WithLabelValues(stage)
	}
	return p
}

func (p *Producer) Handler() http.Handler {
	return promhttp.HandlerFor(p.registry, promhttp.HandlerOpts{})
}

type Consumer struct {
	registry         *prometheus.Registry
	RedpandaConsumed prometheus.Counter
	Processed        prometheus.Counter
	Failed           *prometheus.CounterVec
}

func NewConsumer() *Consumer {
	c := &Consumer{
		registry: prometheus.NewRegistry(),
		RedpandaConsumed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "wikistream_consumer_redpanda_events_consumed_total",
			Help: "Records fetched from Redpanda",
		}),
		Processed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "wikistream_consumer_events_processed_total",
			Help: "Events written to Scylla and committed",
		}),
		Failed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wikistream_consumer_events_failed_total",
			Help: "Events that failed in the consumer, by stage.",
		}, []string{"stage"}),
	}
	c.registry.MustRegister(c.RedpandaConsumed, c.Processed, c.Failed)
	for _, stage := range []string{StageDecode, StageWrite, StageCommit} {
		c.Failed.WithLabelValues(stage)
	}
	return c
}

func (c *Consumer) Handler() http.Handler {
	return promhttp.HandlerFor(c.registry, promhttp.HandlerOpts{})
}
