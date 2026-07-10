package wgo

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

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
//   - A *ClusterBuffer* + per-agent *AgentBuffer* implement
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
	logger  kgo.Logger
	metrics *metrics

	// produceHooks are the produce-record lifecycle hooks driven on this
	// client's own produce path.
	produceHooks produceHooks

	pool           *AgentPool
	tracker        *CachedAgentStatsTracker
	demoter        *Demoter
	directProducer *KafkaDirectProducer
	hedger         *Hedger
	buffer         *ClusterBuffer[routedTopicPartitionRecords]

	// refreshCtx is canceled by Close. It both signals the background
	// refresh goroutine to exit and interrupts any in-flight Refresh.
	refreshCtx    context.Context
	refreshCancel context.CancelFunc
	closeOnce     sync.Once
	refreshWG     sync.WaitGroup
}

// NewWarpstreamClient wires every component of the produce path. Config starts
// from DefaultConfig and is overridden by opts. The initial AgentPool refresh
// is synchronous: if Metadata cannot be reached on startup we fail fast rather
// than serving traffic against an empty pool.
//
// logger is used both for the client's own logs and installed on the embedded
// franz-go client, so the two share a single logger. A nil logger disables logging.
func NewWarpstreamClient(logger kgo.Logger, reg prometheus.Registerer, opts ...Opt) (*WarpstreamClient, error) {
	if logger == nil {
		logger = nopLogger{}
	}

	cfg := NewConfig(opts...)
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid warpstream client config: %w", err)
	}

	kgoClient, err := newKgoClient(cfg, logger, reg)
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
		produceHooks:   newProduceHooks(cfg.Hooks),
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
	c.demoter = NewDemoter(lazy, tracker, cfg.HealthCheck, cfg.Demoter, logger, reg)
	c.hedger = NewHedger(trackingProducer, tracker, c.demoter, cfg.HealthCheck, cfg.Hedger, cfg.Linger, cfg.BatchMaxBytes, m)
	// The cluster buffer's AgentFlushFunc is the Hedger, wrapped only to
	// bound each flush by WriteTimeout. The Hedger is otherwise shaped
	// like a DirectProducer (same signature as KafkaDirectProducer) so it
	// composes directly with the buffer.
	c.buffer = NewClusterBuffer[routedTopicPartitionRecords](cfg.Linger, cfg.BatchMaxBytes, c.flushBatch, m, reg)
	c.startBackgroundRefresh()
	return c, nil
}

// Produce buffers record and invokes promise once it has been
// acknowledged or failed. Cancelling ctx detaches the caller; the record
// is still produced in the background.
//
// Record ownership: the caller must not mutate record until promise fires by
// completion (an ack or terminal failure). After that the record is safe to
// reuse, reset, or pool — the client has captured it and never reads it again,
// including on any still-in-flight hedge attempt. Cancelling ctx is not a
// completion: it detaches the caller while the background produce keeps the
// record, so a record whose produce was cancelled must not be reused.
func (c *WarpstreamClient) Produce(ctx context.Context, record *kgo.Record, promise func(*kgo.Record, error)) {
	// Seed the record's parent context (like franz-go).
	ensureRecordContext(record, ctx)

	// Fire the buffered hook (which may inject a trace header) before any rejection,
	// and wrap the promise so the unbuffered hook fires just before the caller sees
	// the outcome on every path.
	if c.produceHooks.enabled() {
		c.produceHooks.fireBuffered(record)
		userPromise := promise
		promise = func(r *kgo.Record, err error) {
			c.produceHooks.fireUnbuffered(r, err)
			userPromise(r, err)
		}
	}

	c.metrics.produceRecordsTotal.Inc()

	if singleRecordBatchEstimateBytes(record) > int64(c.cfg.BatchMaxBytes) {
		c.metrics.produceRecordsRejectedTotal.WithLabelValues(produceRejectedRecordTooLarge).Inc()
		promise(record, errRecordTooLarge(record))
		return
	}

	routed, err := c.routeRecord(record, perRecordDone(record, func(r *kgo.Record, err error) {
		// The buffered path counts a post-dispatch failure on any non-nil error;
		// the pre-dispatch rejections are counted by produceRecordsRejectedTotal.
		if err != nil {
			c.metrics.produceRecordsFailedTotal.Inc()
		}
		promise(r, err)
	}))
	if err != nil {
		c.metrics.produceRecordsRejectedTotal.WithLabelValues(produceRejectedNoAgentAssigned).Inc()
		promise(record, err)
		return
	}

	// Stamp the produce time only after routing succeeds, so a failed produce
	// leaves the caller's record unchanged. Mirrors franz-go's bufferRecord.
	ensureRecordTimestamp(record, time.Now())
	c.buffer.Add(ctx, routed)
}

// ProduceSync produces records and blocks until each has been
// acknowledged or failed. Results are in input order; each record has its
// own outcome. Cancelling ctx detaches the caller, not the in-flight produce.
//
// Record ownership: when ProduceSync returns by completion (every record acked
// or terminally failed), the records are safe to reuse, reset, or pool — the
// client has captured them and never reads them again, including on any
// still-in-flight hedge attempt. A ctx cancellation is not a completion: it
// detaches the caller while the background produce keeps the records, so records
// whose produce was cancelled must not be reused.
func (c *WarpstreamClient) ProduceSync(ctx context.Context, records []*kgo.Record) kgo.ProduceResults {
	if len(records) == 0 {
		return nil
	}
	c.metrics.produceRecordsTotal.Add(float64(len(records)))

	// Seed each record's parent context (like franz-go) and fire the buffered
	// hook for every record up front, before any rejection.
	hasProduceHooks := c.produceHooks.enabled()
	for _, r := range records {
		ensureRecordContext(r, ctx)
		if hasProduceHooks {
			c.produceHooks.fireBuffered(r)
		}
	}

	results := make(kgo.ProduceResults, len(records))

	// Fire the unbuffered hook for each record from this goroutine, in input order,
	// just before returning. Every return path below fully populates results first
	// (wg.Wait blocks until the async completions have run), so the terminal error
	// is final here. Mirrors franz-go's "unbuffered hook, then the record's outcome".
	if hasProduceHooks {
		defer func() {
			for i, r := range records {
				c.produceHooks.fireUnbuffered(r, results[i].Err)
			}
		}()
	}

	var (
		okRecords []*kgo.Record
		okIndices []int
		wg        sync.WaitGroup
	)
	for i, r := range records {
		if singleRecordBatchEstimateBytes(r) > int64(c.cfg.BatchMaxBytes) {
			c.metrics.produceRecordsRejectedTotal.WithLabelValues(produceRejectedRecordTooLarge).Inc()
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
			if err != nil {
				// Post-dispatch failure, resolved uniformly for the whole
				// partition group; pre-dispatch rejections never reach here.
				c.metrics.produceRecordsFailedTotal.Add(float64(len(groupRecords)))
			}
			for _, r := range groupRecords {
				results[indexOf[r]] = kgo.ProduceResult{Record: r, Err: err}
				wg.Done()
			}
		})
	})
	if err != nil {
		// One record had no known candidate. Fail the whole batch
		// uniformly: every ok record gets the same error.
		c.metrics.produceRecordsRejectedTotal.WithLabelValues(produceRejectedNoAgentAssigned).Add(float64(len(okIndices)))
		for _, i := range okIndices {
			results[i] = kgo.ProduceResult{Record: records[i], Err: err}
		}
		return results
	}

	// Stamp each record's produce time only after routing succeeds, so a failed
	// produce leaves the caller's records unchanged. A single now keeps records
	// buffered together on one produce timestamp. Mirrors franz-go's bufferRecord.
	now := time.Now()
	for _, r := range okRecords {
		ensureRecordTimestamp(r, now)
	}

	c.buffer.MultiAdd(ctx, routed)
	wg.Wait()
	return results
}

// flushBatch is the cluster buffer's AgentFlushFunc: it encodes each partition's
// RecordBatch once, up front, then bounds the Hedger call to WriteTimeout.
func (c *WarpstreamClient) flushBatch(ctx context.Context, nodeID int32, partitions []routedTopicPartitionRecords) ProduceResult {
	// Encode here — synchronously on the flush goroutine, before the Hedger runs
	// and any completion promise can fire — so the primary and every hedge leg
	// transmit the same immutable bytes and no leg reads a *kgo.Record afterward.
	// This is the only records->encoded conversion point.
	encoded := make([]routedEncodedTopicPartitionRecords, 0, len(partitions))
	for _, p := range partitions {
		if len(p.records) == 0 {
			continue
		}
		encoded = append(encoded, routedEncodedTopicPartitionRecords{
			encodedTopicPartitionRecords: newEncodedTopicPartitionRecords(p.topic, p.partition, p.records),
			nodeID:                       p.nodeID,
			nodeState:                    p.nodeState,
		})
	}

	flushCtx, cancel := context.WithTimeout(ctx, c.cfg.WriteTimeout)
	defer cancel()
	return c.hedger.ProduceSync(flushCtx, nodeID, encoded)
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
					log(c.logger, kgo.LogLevelWarn, "warpstream client metadata refresh failed", "err", err)
					continue
				}
				if len(removed) > 0 {
					c.tracker.PurgeAgents(removed)
				}
				c.demoter.Refresh(c.pool.Agents())
			}
		}
	}()
}

// routeRecords groups records by (topic, partition), stamps each group with
// its initial destination NodeID and mints the per-group done callback.
// Returns an error if any record's partition has no known candidate.
func (c *WarpstreamClient) routeRecords(records []*kgo.Record, doneFor func(groupRecords []*kgo.Record) func(ProduceResult)) ([]promised[routedTopicPartitionRecords], error) {
	groups := make(map[topicPartition]*promised[routedTopicPartitionRecords])
	order := make([]topicPartition, 0)
	for _, r := range records {
		key := topicPartition{topic: r.Topic, partition: r.Partition}
		g, ok := groups[key]
		if !ok {
			cands := c.demoter.Candidates(r.Topic, r.Partition, 1)
			if len(cands) == 0 {
				return nil, fmt.Errorf("no agent assigned for topic %q partition %d", r.Topic, r.Partition)
			}
			g = &promised[routedTopicPartitionRecords]{
				item: routedTopicPartitionRecords{
					topicPartitionRecords: topicPartitionRecords{
						topic:     r.Topic,
						partition: r.Partition,
					},
					nodeID:    cands[0].NodeID,
					nodeState: cands[0].State,
				},
			}
			groups[key] = g
			order = append(order, key)
		}
		g.item.records = append(g.item.records, r)
	}
	out := make([]promised[routedTopicPartitionRecords], 0, len(order))
	for _, key := range order {
		g := groups[key]
		g.done = doneFor(g.item.records)
		out = append(out, *g)
	}
	return out, nil
}

// routeRecord is the single-record specialisation of routeRecords: it
// skips the per-partition map and avoids heap-allocating an intermediate
// group. Returns an error if the record's partition has no known candidate.
func (c *WarpstreamClient) routeRecord(record *kgo.Record, done func(ProduceResult)) (promised[routedTopicPartitionRecords], error) {
	cands := c.demoter.Candidates(record.Topic, record.Partition, 1)
	if len(cands) == 0 {
		return promised[routedTopicPartitionRecords]{}, fmt.Errorf("no agent assigned for topic %q partition %d", record.Topic, record.Partition)
	}
	return promised[routedTopicPartitionRecords]{
		item: routedTopicPartitionRecords{
			topicPartitionRecords: topicPartitionRecords{
				topic:     record.Topic,
				partition: record.Partition,
				records:   []*kgo.Record{record},
			},
			nodeID:    cands[0].NodeID,
			nodeState: cands[0].State,
		},
		done: done,
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
// connection management and metadata. It wires the kprom transport-metrics
// hook (registered on reg) alongside any caller hooks from cfg.
func newKgoClient(cfg Config, logger kgo.Logger, reg prometheus.Registerer) (*kgo.Client, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Address...),
		kgo.ClientID(cfg.ClientID),
		kgo.WithLogger(logger),
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
		kgo.ProducerBatchMaxBytes(cfg.BatchMaxBytes),

		// Pin Produce to produceAPIVersion: our wire builders set
		// req.Version explicitly, but kgo.Broker.Request overrides it
		// to the negotiated max. Without this cap, brokers supporting
		// v13+ would receive UUID-only requests/responses, which our
		// per-partition error/response matching (keyed by topic name)
		// can't decode.
		kgo.MaxVersions(produceMaxVersions()),
	}
	if cfg.Dialer != nil {
		opts = append(opts, kgo.Dialer(cfg.Dialer))
	}
	if cfg.TLSEnabled {
		opts = append(opts, kgo.DialTLSConfig(cfg.TLSConfig))
	}
	opts = append(opts, cfg.SASLOptions...)

	// kprom tracks the transport-level metrics. The filtering registerer drops
	// the producer-state metrics this client tracks itself. Caller hooks are appended on top.
	kpromMetrics := newKpromMetrics(newFilteringRegisterer(reg, kpromProducerStateMetricNames...))
	hooks := append([]kgo.Hook{kpromMetrics}, cfg.Hooks...)
	opts = append(opts, kgo.WithHooks(hooks...))

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
// compared against BatchMaxBytes — to make the value directly comparable
// to the configured cap.
func errRecordTooLarge(r *kgo.Record) error {
	return fmt.Errorf("%w (uncompressed_bytes=%d)", kerr.MessageTooLarge, singleRecordBatchEstimateBytes(r))
}

// nopLogger is a kgo.Logger that discards all messages.
type nopLogger struct{}

func (nopLogger) Level() kgo.LogLevel              { return kgo.LogLevelNone }
func (nopLogger) Log(kgo.LogLevel, string, ...any) {}

// log emits msg through logger only when level is enabled, matching how the
// embedded franz-go client gates its own logs by Logger.Level().
func log(logger kgo.Logger, level kgo.LogLevel, msg string, keyvals ...any) {
	if level <= logger.Level() {
		logger.Log(level, msg, keyvals...)
	}
}
