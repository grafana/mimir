// SPDX-License-Identifier: AGPL-3.0-only

package warpstreamclient

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"
)

// produceAPIVersion is the Kafka Produce API version this client emits. v11
// matches franz-go's own negotiated default for ProduceRequest, addresses
// topics by name on the wire (so stale TopicIDs cannot mis-route), and is
// supported by every broker Mimir targets (vanilla Kafka 2.4+, Warpstream).
// Topic UUID is the addressing mode at v13+; bumping past v11 would make
// TopicID load-bearing and require dropping the topic name from requests,
// which we do not need today.
const produceAPIVersion int16 = 11

// WarpstreamClient is a produce-only Kafka client optimised for Warpstream:
// it batches records per primary agent, hedges across secondaries on slow or
// failing primaries, and tracks per-agent stats to make hedge decisions.
//
// Safe for concurrent use.
type WarpstreamClient struct {
	cfg     Config
	logger  log.Logger
	metrics *metrics

	kgoClient *kgo.Client
	pool      *AgentPool
	tracker   *CachedAgentStatsTracker
	hedger    *Hedger
	buffer    *ClusterRecordBuffer

	// refreshCtx is canceled by Close. It both signals the background
	// refresh goroutine to exit and interrupts any in-flight Refresh.
	refreshCtx    context.Context
	refreshCancel context.CancelFunc
	closeOnce     sync.Once
	refreshWG     sync.WaitGroup
}

// NewWarpstreamClient builds and wires every component needed to produce to
// Warpstream: a kgo.Client for connection management and metadata, an
// AgentPool that maintains the current set of agents, an AgentStatsTracker
// that records per-agent latency and error rate, a Hedger that races slow
// primaries against per-partition secondaries, a TrackingProducer that feeds
// the tracker on every leg, and a ClusterRecordBuffer that batches records
// per primary agent and flushes via the Hedger.
//
// On startup the AgentPool is refreshed once; failure aborts construction.
// A background goroutine refreshes it periodically thereafter.
func NewWarpstreamClient(cfg Config, logger log.Logger, reg prometheus.Registerer) (*WarpstreamClient, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid warpstream client config: %w", err)
	}

	kgoClient, err := newKgoClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("creating kgo client: %w", err)
	}

	m := newMetrics(reg)
	pool := NewAgentPool(kgoClient)
	if _, err := pool.Refresh(context.Background()); err != nil {
		kgoClient.Close()
		return nil, fmt.Errorf("initial agent pool refresh: %w", err)
	}

	innerTracker := NewAverageAgentStatsTracker()
	tracker := NewCachedAgentStatsTracker(innerTracker, cfg.ClusterStatsTTL)

	directProducer := NewKafkaDirectProducer(kgoClient)
	trackingProducer := NewTrackingProducer(directProducer, tracker)

	hedgerCfg := HedgerConfig{
		SlowMultiplier:    cfg.HedgeSlowMultiplier,
		MaxSlowFraction:   cfg.HedgeMaxSlowFraction,
		FaultyThreshold:   cfg.HedgeFaultyThreshold,
		MaxFaultyFraction: cfg.HedgeMaxFaultyFraction,
		MinHedgeDelay:     cfg.HedgeMinDelay,
	}

	refreshCtx, refreshCancel := context.WithCancel(context.Background())
	c := &WarpstreamClient{
		cfg:           cfg,
		logger:        logger,
		metrics:       m,
		kgoClient:     kgoClient,
		pool:          pool,
		tracker:       tracker,
		refreshCtx:    refreshCtx,
		refreshCancel: refreshCancel,
	}
	strategy := NewLazyPartitionAssignmentStrategy(pool.Strategy)
	c.hedger = NewHedger(trackingProducer, tracker, strategy, hedgerCfg, m)
	c.buffer = NewClusterRecordBuffer(cfg.Linger, cfg.MaxBatchBytes, strategy.Primary, c.flushBatch, m)
	c.startBackgroundRefresh()
	return c, nil
}

// ProduceSync produces records and blocks until the broker has acknowledged
// the batch (or it has failed). The returned slice has one entry per input
// record, in the same order. Every record receives the same outcome: a
// transport-level error fails the whole batch (Hedger semantics).
//
// ctx governs only the wait on the result. Records are committed to the
// in-memory batch as soon as ProduceSync is called; cancelling ctx detaches
// this caller from the result, but the batch still flushes — possibly
// delivering the records anyway. This is consistent with at-least-once
// semantics: a duplicate may follow a caller-side retry.
func (c *WarpstreamClient) ProduceSync(ctx context.Context, records []*kgo.Record) kgo.ProduceResults {
	if len(records) == 0 {
		return nil
	}
	doneCh := make(chan error, 1)
	c.buffer.Add(ctx, records, func(err error) { doneCh <- err })
	err := <-doneCh

	results := make(kgo.ProduceResults, len(records))
	for i, r := range records {
		results[i] = kgo.ProduceResult{Record: r, Err: err}
	}
	return results
}

// Close stops the background metadata refresh, flushes every pending batch,
// then closes the underlying kgo.Client. Idempotent.
func (c *WarpstreamClient) Close() error {
	c.closeOnce.Do(func() {
		c.refreshCancel()
		c.refreshWG.Wait()
		c.buffer.Close()
		c.kgoClient.Close()
	})
	return nil
}

// flushBatch is the FlushFunc the buffer calls when an agent's batch is
// ready. It builds a (possibly multi-topic) ProduceRequest, sends via the
// Hedger, parses the response for per-partition errors, and reports the
// outcome via done.
//
// Records carrying a topic the AgentPool has not learned about fail the
// whole batch synchronously via buildMultiTopicProduceRequest: the agent
// has no way to address such a topic on v13+ and would in any case reject
// it with UNKNOWN_TOPIC_OR_PARTITION on the wire.
func (c *WarpstreamClient) flushBatch(ctx context.Context, nodeID int32, records []*kgo.Record, done func(error)) {
	req, err := buildMultiTopicProduceRequest(produceAPIVersion, c.pool.TopicID, records)
	if err != nil {
		done(err)
		return
	}

	resp, err := c.hedger.Produce(ctx, nodeID, req)
	if err != nil {
		done(err)
		return
	}

	done(parseProduceResponse(resp))
}

// startBackgroundRefresh runs pool.Refresh on a fixed interval and purges
// the tracker of any agents the refresh reports as removed.
//
// Refresh is called with refreshCtx, which has no deadline of its own; the
// underlying Metadata fetch is bounded by kgo's RequestTimeoutOverhead
// (configured from cfg.WriteTimeout in newKgoClient). Adding our own
// per-call timeout would just duplicate that bound. refreshCtx is canceled
// by Close, which both interrupts an in-flight Refresh and breaks the
// ticker loop on the next iteration.
func (c *WarpstreamClient) startBackgroundRefresh() {
	c.refreshWG.Add(1)
	go func() {
		defer c.refreshWG.Done()
		ticker := time.NewTicker(c.cfg.MetadataRefreshInterval)
		defer ticker.Stop()
		for {
			select {
			case <-c.refreshCtx.Done():
				return
			case <-ticker.C:
				removed, err := c.pool.Refresh(c.refreshCtx)
				if err != nil {
					level.Warn(c.logger).Log("msg", "warpstream client metadata refresh failed", "err", err)
					continue
				}
				if len(removed) > 0 {
					c.tracker.PurgeAgents(removed)
				}
			}
		}
	}()
}

// newKgoClient constructs the kgo.Client used by the WarpstreamClient for
// connection management and metadata.
func newKgoClient(cfg Config) (*kgo.Client, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Address...),
		kgo.ClientID(cfg.ClientID),
		kgo.DialTimeout(cfg.DialTimeout),
		kgo.RequestTimeoutOverhead(cfg.WriteTimeout),
		kgo.MetadataMaxAge(cfg.MetadataRefreshInterval),
	}
	if cfg.TLSEnabled {
		opts = append(opts, kgo.DialTLSConfig(cfg.TLSConfig))
	}
	opts = append(opts, cfg.SASLOptions...)
	return kgo.NewClient(opts...)
}
