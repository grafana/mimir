// SPDX-License-Identifier: AGPL-3.0-only

package warpstreamclient

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// produceAPIVersion is the Kafka Produce API version this client emits. v11
// matches franz-go's own negotiated default for ProduceRequest, addresses
// topics by name on the wire (so stale TopicIDs cannot mis-route), and is
// supported by every broker this client targets (vanilla Kafka 2.4+, Warpstream).
// Topic UUID is the addressing mode at v13+; bumping past v11 would make
// TopicID load-bearing and require dropping the topic name from requests,
// which we do not need today.
const produceAPIVersion int16 = 11

// WarpstreamClient is a produce-only Kafka client tailored to Warpstream's
// stateless-agent architecture. It exists because franz-go's standard produce
// path enforces vanilla Kafka's "Produce request must go to the partition
// leader" rule, which prevents hedging: in vanilla Kafka, sending the same
// batch to two brokers fails (NotLeaderForPartition).
//
// Warpstream is different — its agents are fully stateless, every agent can
// serve every partition at any time — and the produce contract this client
// exposes is at-least-once with no in-partition sequencing requirement, which
// makes duplicates tolerable.
//
// The client trades the vanilla-Kafka guarantee away in exchange for the
// ability to race a slow primary against a different agent and take
// whichever responds first.
//
// The design is the minimum machinery needed to do that hedging on top of
// normal produce traffic:
//
//   - An *AgentPool* watches Metadata and exposes the current set of
//     reachable agents plus the (Kafka-protocol-mandated) leader for each
//     partition. The pool is kept up to date in the background; the produce
//     path never blocks on Metadata refreshes.
//   - A *PartitionAssignmentStrategy* turns the pool into a deterministic
//     primary/secondary mapping. Every client instance with the same pool
//     view picks the same secondary for a given partition, which keeps
//     hedge load predictable and analysable instead of random.
//   - A *ClusterRecordBuffer* + per-agent *AgentRecordBuffer* implement
//     linger-based batching keyed on the primary agent (not on partition,
//     as franz-go does), so a single Produce request to one agent can
//     carry batches for many partitions.
//   - A *Hedger* decides per-flush whether to actually send a hedge based
//     on rolling per-agent latency and error stats. Hedging is opt-in per
//     batch, not unconditional, so a healthy cluster pays no extra cost.
//   - The embedded *kgo.Client is reused for connection management,
//     Metadata, and SASL/TLS. We deliberately do not use franz-go's
//     producer state machine — its routing model would force per-partition
//     leader pinning that defeats the whole point.
//
// Embedding *kgo.Client also means callers can use any non-producer method
// (Ping, MetadataRequest, etc.) on a WarpstreamClient transparently.
//
// Safe for concurrent use.
type WarpstreamClient struct {
	*kgo.Client

	cfg     Config
	logger  log.Logger
	metrics *metrics

	pool    *AgentPool
	tracker *CachedAgentStatsTracker
	hedger  *Hedger
	buffer  *ClusterRecordBuffer

	// refreshCtx is canceled by Close. It both signals the background
	// refresh goroutine to exit and interrupts any in-flight Refresh.
	refreshCtx    context.Context
	refreshCancel context.CancelFunc
	closeOnce     sync.Once
	refreshWG     sync.WaitGroup
}

// NewWarpstreamClient wires every component of the produce path. The initial
// AgentPool refresh is synchronous: if Metadata cannot be reached on startup
// we fail fast rather than serving traffic against an empty pool.
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
		Client:        kgoClient,
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

// Produce buffers record and invokes promise once it has been acknowledged
// or failed. Cancelling ctx detaches the caller from the result; the record
// is still produced (possibly successfully) in the background.
//
// Records whose encoded size exceeds MaxBatchBytes are rejected synchronously
// via promise with kerr.MessageTooLarge.
func (c *WarpstreamClient) Produce(ctx context.Context, record *kgo.Record, promise func(*kgo.Record, error)) {
	if recordBatchEstimateBytes(record) > c.cfg.MaxBatchBytes {
		promise(record, errRecordTooLarge(record))
		return
	}
	c.buffer.Add(ctx, []*kgo.Record{record}, func(err error) { promise(record, err) })
}

// ProduceSync produces records and blocks until they have been acknowledged
// or failed. Results are in input order.
//
// Records whose encoded size exceeds MaxBatchBytes are rejected per-record
// with kerr.MessageTooLarge; remaining records share the same outcome from the buffer
// (transport errors fail the whole batch).
//
// Cancelling ctx detaches the caller, not the in-flight produce.
func (c *WarpstreamClient) ProduceSync(ctx context.Context, records []*kgo.Record) kgo.ProduceResults {
	if len(records) == 0 {
		return nil
	}

	var (
		okRecords []*kgo.Record
		// okIndices preserves input order so the single buffer outcome can
		// be slotted back into results without resorting.
		okIndices []int
		results   = make(kgo.ProduceResults, len(records))
	)
	for i, r := range records {
		if recordBatchEstimateBytes(r) > c.cfg.MaxBatchBytes {
			results[i] = kgo.ProduceResult{Record: r, Err: errRecordTooLarge(r)}
			continue
		}
		okRecords = append(okRecords, r)
		okIndices = append(okIndices, i)
	}
	if len(okRecords) == 0 {
		return results
	}

	doneCh := make(chan error, 1)
	c.buffer.Add(ctx, okRecords, func(err error) { doneCh <- err })
	err := <-doneCh

	for _, i := range okIndices {
		results[i] = kgo.ProduceResult{Record: records[i], Err: err}
	}
	return results
}

// BufferedProduceBytes returns the bytes of all records awaiting ack.
func (c *WarpstreamClient) BufferedProduceBytes() int64 {
	return c.buffer.BufferedBytes()
}

// BufferedProduceRecords returns the count of records awaiting ack.
func (c *WarpstreamClient) BufferedProduceRecords() int64 {
	return c.buffer.BufferedRecords()
}

// Close stops the background metadata refresh, flushes pending batches, and
// closes the underlying kgo.Client. Idempotent.
func (c *WarpstreamClient) Close() {
	c.closeOnce.Do(func() {
		c.refreshCancel()
		c.refreshWG.Wait()
		c.buffer.Close()
		c.Client.Close()
	})
}

// flushBatch is the FlushFunc the buffer calls when an agent's batch is
// ready: build the ProduceRequest, hand it to the Hedger, report the outcome.
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

// startBackgroundRefresh ticks pool.Refresh and purges the tracker of removed
// agents. refreshCtx (cancelled by Close) bounds both the loop and any
// in-flight Refresh; the underlying Metadata fetch is already capped by
// kgo's RequestTimeoutOverhead, so we don't add a per-call deadline.
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

		// Disable franz-go's own buffered-records / buffered-bytes caps. The
		// produce path is enforced one level up.
		kgo.MaxBufferedRecords(math.MaxInt),
		kgo.MaxBufferedBytes(0),

		// Mirror the warpstream-side producer settings so direct Produce calls
		// on the embedded kgo.Client (e.g. benchmarks, tests) honour the same
		// batching configuration. The wrapping ProduceSync path does not use
		// kgo's producer, so these are no-ops in normal operation.
		kgo.ProducerLinger(cfg.Linger),
		kgo.ProducerBatchMaxBytes(cfg.MaxBatchBytes),
	}
	if cfg.TLSEnabled {
		opts = append(opts, kgo.DialTLSConfig(cfg.TLSConfig))
	}
	opts = append(opts, cfg.SASLOptions...)
	return kgo.NewClient(opts...)
}

// errRecordTooLarge wraps kerr.MessageTooLarge so errors.Is matches. The
// reported byte count is the wire-encoded estimate — what we actually
// compared against MaxBatchBytes — to make the value directly comparable
// to the configured cap.
func errRecordTooLarge(r *kgo.Record) error {
	return fmt.Errorf("%w (uncompressed_bytes=%d)", kerr.MessageTooLarge, recordBatchEstimateBytes(r))
}
