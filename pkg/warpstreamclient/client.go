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
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/kversion"
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

	pool           *AgentPool
	tracker        *CachedAgentStatsTracker
	strategy       PartitionAssignmentStrategy
	directProducer *KafkaDirectProducer
	hedger         *Hedger
	buffer         *ClusterRecordBuffer

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

	directProducer := NewKafkaDirectProducer(kgoClient, pool.TopicID, produceAPIVersion, cfg.DirectProducer, m)
	// Tracker observes each per-attempt outcome directly. Cross-agent
	// retry is handled by the Hedger as part of its per-call wave loop,
	// so the agent stats here reflect per-attempt quality and the Hedger
	// reacts to it without a layered retry mechanism dampening the signal.
	trackingProducer := NewTrackingProducer(directProducer, tracker)

	refreshCtx, refreshCancel := context.WithCancel(context.Background())
	c := &WarpstreamClient{
		cfg:            cfg,
		logger:         logger,
		metrics:        m,
		Client:         kgoClient,
		pool:           pool,
		tracker:        tracker,
		directProducer: directProducer,
		refreshCtx:     refreshCtx,
		refreshCancel:  refreshCancel,
	}
	// Demoter sits on top of the lazy pool strategy so refresh-driven
	// agent-pool changes flow through transparently while the Demoter's
	// per-agent probe-timing state persists across refreshes.
	lazy := NewLazyPartitionAssignmentStrategy(pool.Strategy)
	c.strategy = NewDemoter(lazy, tracker, cfg.HealthCheck, cfg.Demoter, logger, reg)
	c.hedger = NewHedger(trackingProducer, tracker, c.strategy, cfg.HealthCheck, cfg.Hedger, cfg.Linger, cfg.MaxBatchBytes, m)
	// The cluster buffer's AgentFlushFunc is the Hedger, wrapped only to
	// bound each flush by WriteTimeout. The Hedger is otherwise shaped
	// like a DirectProducer (same signature as KafkaDirectProducer) so it
	// composes directly with the buffer.
	c.buffer = NewClusterRecordBuffer(cfg.Linger, cfg.MaxBatchBytes, c.flushBatch, m)
	c.startBackgroundRefresh()
	return c, nil
}

// Produce buffers record and invokes promise once it has been
// acknowledged or failed. Cancelling ctx detaches the caller; the record
// is still produced in the background.
func (c *WarpstreamClient) Produce(ctx context.Context, record *kgo.Record, promise func(*kgo.Record, error)) {
	if recordBatchEstimateBytes(record) > c.cfg.MaxBatchBytes {
		promise(record, errRecordTooLarge(record))
		return
	}
	routed, err := c.routeRecord(record, perRecordDone(record, promise))
	if err != nil {
		promise(record, err)
		return
	}
	c.buffer.Add(ctx, []routedTopicPartitionRecords{routed})
}

// ProduceSync produces records and blocks until each has been
// acknowledged or failed. Results are in input order; each record has its
// own outcome. Cancelling ctx detaches the caller, not the in-flight produce.
func (c *WarpstreamClient) ProduceSync(ctx context.Context, records []*kgo.Record) kgo.ProduceResults {
	if len(records) == 0 {
		return nil
	}

	results := make(kgo.ProduceResults, len(records))
	var (
		okRecords []*kgo.Record
		okIndices []int
		wg        sync.WaitGroup
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

	wg.Add(len(okRecords))
	indexOf := make(map[*kgo.Record]int, len(okRecords))
	for _, idx := range okIndices {
		indexOf[records[idx]] = idx
	}

	routed, err := c.routeRecords(okRecords, func(groupRecords []*kgo.Record) func(ProduceResult) {
		return perPartitionDone(groupRecords[0].Topic, groupRecords[0].Partition, func(err error) {
			for _, r := range groupRecords {
				results[indexOf[r]] = kgo.ProduceResult{Record: r, Err: err}
				wg.Done()
			}
		})
	})
	if err != nil {
		// One record had no known candidate. Fail the whole batch
		// uniformly: every ok record gets the same error.
		for _, i := range okIndices {
			results[i] = kgo.ProduceResult{Record: records[i], Err: err}
		}
		return results
	}

	c.buffer.Add(ctx, routed)
	wg.Wait()
	return results
}

// flushBatch is the cluster buffer's AgentFlushFunc: it bounds the
// Hedger call to WriteTimeout and forwards everything else verbatim.
func (c *WarpstreamClient) flushBatch(ctx context.Context, nodeID int32, partitions []routedTopicPartitionRecords) ProduceResult {
	flushCtx, cancel := context.WithTimeout(ctx, c.cfg.WriteTimeout)
	defer cancel()
	return c.hedger.ProduceSync(flushCtx, nodeID, partitions)
}

// BufferedProduceBytes returns the bytes of all records awaiting ack.
func (c *WarpstreamClient) BufferedProduceBytes() int64 {
	return c.buffer.BufferedBytes()
}

// BufferedProduceRecords returns the count of records awaiting ack.
func (c *WarpstreamClient) BufferedProduceRecords() int64 {
	return c.buffer.BufferedRecords()
}

// SetTestProduceResponseHook installs a hook fired after every Broker.Request
// completes (primary or hedge attempt) inside KafkaDirectProducer. The hook
// runs synchronously on the per-attempt goroutine and may block — useful for
// tests that simulate per-agent latency. Production code does not set this.
func (c *WarpstreamClient) SetTestProduceResponseHook(fn func(ctx context.Context, nodeID int32, resp *kmsg.ProduceResponse, err error)) {
	c.directProducer.onProduceResponse = fn
}

// Close stops the background metadata refresh, flushes pending batches, and
// closes the underlying kgo.Client. Idempotent.
func (c *WarpstreamClient) Close() {
	c.closeOnce.Do(func() {
		c.refreshCancel()
		c.refreshWG.Wait()
		c.buffer.Close()
		c.hedger.Close()
		c.Client.Close()
	})
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

// routeRecords groups records by (topic, partition), stamps each group with
// its initial destination NodeID and mints the per-group done callback.
// Returns an error if any record's partition has no known candidate.
func (c *WarpstreamClient) routeRecords(records []*kgo.Record, doneFor func(groupRecords []*kgo.Record) func(ProduceResult)) ([]routedTopicPartitionRecords, error) {
	groups := make(map[topicPartition]*routedTopicPartitionRecords)
	order := make([]topicPartition, 0)
	for _, r := range records {
		key := topicPartition{topic: r.Topic, partition: r.Partition}
		g, ok := groups[key]
		if !ok {
			cands := c.strategy.Candidates(r.Topic, r.Partition, 1)
			if len(cands) == 0 {
				return nil, fmt.Errorf("no agent assigned for topic %q partition %d", r.Topic, r.Partition)
			}
			g = &routedTopicPartitionRecords{
				topicPartitionRecords: topicPartitionRecords{
					topic:     r.Topic,
					partition: r.Partition,
				},
				nodeID:    cands[0].NodeID,
				nodeState: cands[0].State,
			}
			groups[key] = g
			order = append(order, key)
		}
		g.records = append(g.records, r)
	}
	out := make([]routedTopicPartitionRecords, 0, len(order))
	for _, key := range order {
		g := groups[key]
		g.done = doneFor(g.records)
		out = append(out, *g)
	}
	return out, nil
}

// routeRecord is the single-record specialisation of routeRecords: it
// skips the per-partition map and avoids heap-allocating an intermediate
// group. Returns an error if the record's partition has no known candidate.
func (c *WarpstreamClient) routeRecord(record *kgo.Record, done func(ProduceResult)) (routedTopicPartitionRecords, error) {
	cands := c.strategy.Candidates(record.Topic, record.Partition, 1)
	if len(cands) == 0 {
		return routedTopicPartitionRecords{}, fmt.Errorf("no agent assigned for topic %q partition %d", record.Topic, record.Partition)
	}
	return routedTopicPartitionRecords{
		topicPartitionRecords: topicPartitionRecords{
			topic:     record.Topic,
			partition: record.Partition,
			records:   []*kgo.Record{record},
		},
		nodeID:    cands[0].NodeID,
		nodeState: cands[0].State,
		done:      done,
	}, nil
}

// perPartitionDone adapts a batch-wide ProduceResult callback to a
// per-partition outcome for one (topic, partition).
func perPartitionDone(topic string, partition int32, user func(error)) func(ProduceResult) {
	return func(res ProduceResult) {
		// The merged response is authoritative when present: it carries
		// one terminal entry per partition (resolved or synthesized), so
		// a partition that actually succeeded must report success even
		// when res.err says some peer partition failed.
		if res.resp == nil {
			user(res.err)
			return
		}
		err := partitionErrorFromResp(res.resp, topic, partition)
		// A retriable kerr surfaced here means the Hedger exhausted its
		// retry budget; wrap with kgo.ErrRecordTimeout to match franz-go's
		// retry-exhausted contract while keeping the specific kerr in
		// the chain. Direct type assertion — partitionErrorFromResp only ever
		// returns nil or *kerr.Error.
		if ke, _ := err.(*kerr.Error); ke != nil && ke.Retriable {
			err = fmt.Errorf("%w: %w", kgo.ErrRecordTimeout, err)
		}
		user(err)
	}
}

// perRecordDone returns a done callback that resolves the
// outcome for a single (record, promise) pair. Same semantics as
// perPartitionDone but avoids the extra closure that wraps promise.
func perRecordDone(record *kgo.Record, promise func(*kgo.Record, error)) func(ProduceResult) {
	return func(res ProduceResult) {
		if res.resp == nil {
			promise(record, res.err)
			return
		}

		// partitionErrorFromResp only ever returns nil or *kerr.Error
		// (from kerr.ErrorForCode), so a direct type assertion avoids
		// the errors.As reflection.
		err := partitionErrorFromResp(res.resp, record.Topic, record.Partition)
		if ke, _ := err.(*kerr.Error); ke != nil && ke.Retriable {
			err = fmt.Errorf("%w: %w", kgo.ErrRecordTimeout, err)
		}

		promise(record, err)
	}
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

		// Treat first-read EOF as retryable. By default kgo classifies it as
		// likely SASL/TLS misconfig and refuses to retry; we know our config
		// (long-lived produce-only client, validated at startup) so the
		// "busy broker drops the connection" interpretation applies — retry.
		kgo.AlwaysRetryEOF(),

		// Mirror the warpstream-side producer settings so direct Produce calls
		// on the embedded kgo.Client (e.g. benchmarks, tests) honour the same
		// batching configuration. The wrapping ProduceSync path does not use
		// kgo's producer, so these are no-ops in normal operation.
		kgo.ProducerLinger(cfg.Linger),
		kgo.ProducerBatchMaxBytes(cfg.MaxBatchBytes),

		// Pin Produce to produceAPIVersion: our wire builders set
		// req.Version explicitly, but kgo.Broker.Request overrides it
		// to the negotiated max. Without this cap, brokers supporting
		// v13+ would receive UUID-only requests/responses, which our
		// per-partition error/response matching (keyed by topic name)
		// can't decode.
		kgo.MaxVersions(produceMaxVersions()),
	}
	if cfg.TLSEnabled {
		opts = append(opts, kgo.DialTLSConfig(cfg.TLSConfig))
	}
	opts = append(opts, cfg.SASLOptions...)
	return kgo.NewClient(opts...)
}

// produceMaxVersions returns a kversion.Versions snapshot identical to
// kgo's default kversion.Stable() except Produce is capped at
// produceAPIVersion so kgo's per-request version negotiation can't
// promote us past the wire format we generate and parse.
func produceMaxVersions() *kversion.Versions {
	v := kversion.Stable()
	v.SetMaxKeyVersion(kmsg.Produce.Int16(), produceAPIVersion)
	return v
}

// errRecordTooLarge wraps kerr.MessageTooLarge so errors.Is matches. The
// reported byte count is the wire-encoded estimate — what we actually
// compared against MaxBatchBytes — to make the value directly comparable
// to the configured cap.
func errRecordTooLarge(r *kgo.Record) error {
	return fmt.Errorf("%w (uncompressed_bytes=%d)", kerr.MessageTooLarge, recordBatchEstimateBytes(r))
}
