// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"context"
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/grafana/mimir/pkg/nautilus/assignment"
	"github.com/grafana/mimir/pkg/storage/ingest"
)

// Config holds the configuration for the nautilus ingestion rebalancer.
type Config struct {
	// MinRebalanceInterval is a lower bound on the gap between
	// rebalance rounds. The rebalancer normally schedules itself
	// dynamically — each round is timed to fire LeaseLookahead
	// before the soonest active or pre-issued lease expires. This
	// value caps how aggressively it runs immediately after a
	// reassignment (which truncates a lease's To and would otherwise
	// drag the next scheduled time arbitrarily close to "now") and
	// covers the cold-start case where the log is empty.
	MinRebalanceInterval time.Duration `yaml:"min_rebalance_interval"`

	// MaxRebalanceInterval caps the gap between rounds even when
	// the lease horizon would otherwise allow a longer wait. Acts
	// as a heartbeat for stats collection and reassignment
	// reactivity (the rebalancer can't react to a load imbalance
	// while it's idle).
	MaxRebalanceInterval time.Duration `yaml:"max_rebalance_interval"`

	MovementBudget float64 `yaml:"movement_budget"`

	// IngesterRPCTimeout bounds each individual HashRangeStats /
	// SetHashRanges call to a single ingester. RPCs are issued in
	// parallel, but a stuck ingester (e.g. mid-rollout, with a stale
	// pool connection) would otherwise tie up the whole round behind
	// TCP-level timeouts. Set to 0 to disable per-call timeouts (not
	// recommended in production).
	IngesterRPCTimeout time.Duration `yaml:"ingester_rpc_timeout"`

	// IngesterRPCConcurrency caps the number of in-flight ingester
	// RPCs per round. A value <= 0 means "one job per ingester"
	// (effectively unbounded). Tune down if the rebalancer pod has
	// limited file descriptors or you want gentler load on the pool.
	IngesterRPCConcurrency int `yaml:"ingester_rpc_concurrency"`

	// MoveCooldown is the minimum time a hash range must sit on its new
	// partition after a move before it (or any range overlapping its
	// former boundaries) is eligible to be moved again. Acts as a
	// per-range anti-flap guard; aggregate per-round churn is bounded
	// by MovementBudget.
	MoveCooldown time.Duration `yaml:"move_cooldown"`

	// LeaseDuration is how long each freshly-issued lease is valid.
	// Each rebalance round, for every (Range, PartitionID) in the
	// desired tiling whose latest lease will expire within
	// LeaseLookahead, the rebalancer pre-issues a successor lease
	// covering [prev.To, prev.To + LeaseDuration). Consumers
	// (distributors, queriers) treat leases whose To has passed as
	// expired and fall back to the partition ring's default
	// routing. The rebalancer schedules itself to run roughly once
	// per lease — see LeaseLookahead.
	LeaseDuration time.Duration `yaml:"lease_duration"`

	// LeaseLookahead is how far before an active lease's expiry the
	// rebalancer wakes up to pre-issue its successor. The next
	// round is scheduled for (lease_horizon - LeaseLookahead),
	// where lease_horizon is the soonest-expiring active or
	// pre-issued lease. This window must be wide enough to cover
	// the rebalance round's runtime plus RPC propagation to all
	// distributors, so by the time the active lease expires every
	// consumer has its successor in hand.
	LeaseLookahead time.Duration `yaml:"lease_lookahead"`

	// EntryRetention bounds how long expired log entries are kept
	// in the log after their lease ended (To). Active leases are
	// never pruned. Must exceed the querier's QueryIngestersWithin
	// (default 13h) plus a drain buffer once the querier consumes
	// the log; for now (distributor-only consumer) the value
	// primarily caps the rebalancer's memory footprint and the size
	// of the streaming snapshots.
	EntryRetention time.Duration `yaml:"entry_retention"`

	// ReadcacheSlicer configures the second slicer round that
	// balances partition->readcache-instance assignments. The first
	// round (above) balances hash-range->partition; the second round
	// balances partition->readcache-instance.
	ReadcacheSlicer ReadcacheSlicerConfig `yaml:"readcache_slicer"`

	// ReadcacheClient configures the gRPC client the rebalancer uses
	// to dial readcache pods (HashRangeStats / SetHashRanges /
	// GetHashRanges). Read only when a readcache instance ring is
	// wired; without the ring the rebalancer falls back to the
	// legacy ingester pool and ignores these settings.
	ReadcacheClient ReadcacheClientConfig `yaml:"readcache_client"`

	// DataDir is a local-disk directory where the rebalancer
	// persists both the (hash-range -> partition) and the (partition
	// -> readcache instance) logs. On restart the rebalancer seeds
	// the in-memory logs from disk so the very first round after a
	// crash doesn't reset routing to FineEvenSplit, which would
	// shuffle every partition's owner and stall every readcache for
	// the duration of a warm-up.
	//
	// Empty string disables persistence (logs are seeded from
	// FineEvenSplit / GetHashRanges as before).
	DataDir string `yaml:"data_dir"`

	// KafkaTopic is the Kafka topic the rebalancer asks Kafka to
	// auto-create on startup (subject to the global
	// -ingest-storage.kafka.auto-create-topic-enabled gate). It
	// matches the topic distributors forward writes to for tenants
	// in nautilus_ingest_routing=nautilus-only and the topic
	// readcache pods consume from. Default "nautilus_ingest".
	KafkaTopic string `yaml:"kafka_topic"`

	// PartitionCount is the number of partitions on KafkaTopic.
	// Used both for the auto-create call and to size the slicer's
	// partition-set when seeding the in-memory log. A higher
	// partition count widens the rebalancer's redistribution budget
	// (more, smaller chunks); a lower one reduces gRPC fan-out per
	// round.
	PartitionCount int32 `yaml:"partition_count"`

	// ActivePartitionCount caps the rebalancer's logical view of
	// "active partitions" to the first K partition IDs [0, K).
	// When zero, the rebalancer slices over [0, PartitionCount).
	// Nautilus is decoupled from the ingester partition ring; both
	// values refer to Kafka partitions on KafkaTopic.
	//
	// Constraints:
	//   - 0 < ActivePartitionCount <= PartitionCount (validated at
	//     config parse). Setting it greater than the topic's
	//     provisioned partition count would route writes to
	//     non-existent Kafka partitions.
	//   - Raising the value at runtime is safe (new partitions come
	//     online cold and the slicer assigns them). Lowering it
	//     orphans data on partitions [K_new, K_old) until those
	//     partitions are re-included or their data ages out.
	ActivePartitionCount int32 `yaml:"active_partition_count"`

	// Kafka is the shared ingest-storage Kafka config, injected by
	// pkg/mimir so this package doesn't depend on the global Mimir
	// configuration tree. It is consulted for connection/auth
	// settings when auto-creating KafkaTopic; the Topic and
	// AutoCreateTopicDefaultPartitions fields are overridden with
	// KafkaTopic/PartitionCount before the create call.
	Kafka ingest.KafkaConfig `yaml:"-"`
}

func (cfg *Config) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.DurationVar(&cfg.MinRebalanceInterval, prefix+"min-rebalance-interval", 30*time.Second, "Lower bound on the gap between rebalance rounds. The rebalancer schedules itself dynamically (next round at lease_horizon - LeaseLookahead), but this floor protects against degenerate cases such as just-truncated leases or a fully-expired log.")
	f.DurationVar(&cfg.MaxRebalanceInterval, prefix+"max-rebalance-interval", 5*time.Minute, "Upper bound on the gap between rebalance rounds. Acts as a heartbeat for stats collection and reassignment reactivity even when the lease horizon would otherwise allow a longer wait.")
	f.Float64Var(&cfg.MovementBudget, prefix+"movement-budget", 0.09, "Maximum fraction of the hash space that can be moved per round.")
	f.DurationVar(&cfg.IngesterRPCTimeout, prefix+"ingester-rpc-timeout", 4*time.Second, "Per-call timeout for HashRangeStats and SetHashRanges RPCs to each ingester. Prevents one stuck pod (e.g. mid-rollout) from stalling the whole rebalance round. 0 disables.")
	f.IntVar(&cfg.IngesterRPCConcurrency, prefix+"ingester-rpc-concurrency", 10, "Maximum concurrent ingester RPCs per round. 0 means one per ingester (unbounded).")
	f.DurationVar(&cfg.MoveCooldown, prefix+"move-cooldown", 90*time.Second, "Minimum time between consecutive moves of the same hash range (or any range overlapping it). Per-range anti-flap guard; aggregate per-round churn is bounded by MovementBudget. 0 disables.")
	f.DurationVar(&cfg.LeaseDuration, prefix+"lease-duration", 5*time.Minute, "Duration of each freshly-issued or pre-issued successor assignment-log lease. Consumers fall back to the partition ring once a lease expires, so this caps how long stale routing can persist after a rebalancer outage.")
	f.DurationVar(&cfg.LeaseLookahead, prefix+"lease-lookahead", 90*time.Second, "How far before an active lease's expiry the rebalancer pre-issues its successor. Steady-state rebalance interval is approximately LeaseDuration; LeaseLookahead is the safety buffer to disseminate the successor to all consumers before the active lease ends.")
	f.DurationVar(&cfg.EntryRetention, prefix+"entry-retention", 24*time.Hour, "How long expired assignment-log entries are retained after their lease ended. Active and pre-issued leases are never pruned. Must exceed querier QueryIngestersWithin plus a drain buffer once queriers consume the log; today the value chiefly caps the rebalancer's snapshot size sent to distributor stream subscribers.")
	f.StringVar(&cfg.DataDir, prefix+"data-dir", "", "Directory where the rebalancer persists its assignment logs. Empty disables persistence; on restart the log is seeded from FineEvenSplit / ingester reports as before. Set this to a persistent volume in production so rebalancer restarts don't shuffle routing.")
	f.StringVar(&cfg.KafkaTopic, prefix+"kafka-topic", "nautilus_ingest", "Name of the Kafka topic the nautilus pipeline runs on. The rebalancer auto-creates this topic on startup (gated by -ingest-storage.kafka.auto-create-topic-enabled); distributors forward nautilus-only tenant writes here; readcache pods consume from it.")
	f.Var(asInt32Var{&cfg.PartitionCount}, prefix+"partition-count", "Number of partitions on -nautilus.rebalancer.kafka-topic. Used both for auto-creation and to size the slicer's initial partition set when seeding the in-memory log. Must be > 0 when persistence is empty; otherwise the seeded value from disk wins.")
	f.Var(asInt32Var{&cfg.ActivePartitionCount}, prefix+"active-partition-count", "Cap on the rebalancer's logical active-partition set: when > 0 the rebalancer slices over partition IDs [0, K); when 0 it uses -nautilus-rebalancer.partition-count. Must be <= -nautilus-rebalancer.partition-count when both are set.")
	cfg.ReadcacheSlicer.RegisterFlagsWithPrefix(prefix+"readcache-slicer.", f)
	cfg.ReadcacheClient.RegisterFlagsWithPrefix(prefix+"readcache-client.", f)
}

// Validate returns an error if the config is internally inconsistent.
// Called by pkg/mimir during configuration parsing.
func (cfg *Config) Validate() error {
	if cfg.ActivePartitionCount < 0 {
		return fmt.Errorf("nautilus-rebalancer.active-partition-count must be >= 0, got %d", cfg.ActivePartitionCount)
	}
	if cfg.ActivePartitionCount > 0 && cfg.PartitionCount > 0 && cfg.ActivePartitionCount > cfg.PartitionCount {
		return fmt.Errorf("nautilus-rebalancer.active-partition-count (%d) must be <= nautilus-rebalancer.partition-count (%d); the cap can't exceed the topic's provisioned partitions", cfg.ActivePartitionCount, cfg.PartitionCount)
	}
	if cfg.ActivePartitionCount <= 0 && cfg.PartitionCount <= 0 {
		return fmt.Errorf("nautilus-rebalancer requires either -nautilus-rebalancer.partition-count or -nautilus-rebalancer.active-partition-count > 0")
	}
	return nil
}

// asInt32Var adapts an *int32 to flag.Value, which only ships with
// IntVar by default.
type asInt32Var struct{ v *int32 }

func (a asInt32Var) String() string {
	if a.v == nil {
		return "0"
	}
	return fmt.Sprintf("%d", *a.v)
}

func (a asInt32Var) Set(s string) error {
	n, err := strconv.ParseInt(s, 10, 32)
	if err != nil {
		return err
	}
	*a.v = int32(n)
	return nil
}

// Rebalancer is a Mimir module that periodically queries readcache
// pods for per-hash-range ingestion rates, rebalances
// hash-range-to-partition assignments, pushes hash ranges to readcache
// owners, and serves assignment logs to distributors and readcache
// pods.
type Rebalancer struct {
	services.Service

	cfg    Config
	logger log.Logger

	// readcachePool dials readcache pods for HashRangeStats,
	// SetHashRanges, and GetHashRanges. Required in production;
	// unit tests may construct a Rebalancer without a pool and
	// inject synthetic stats directly into runSlicer.
	readcachePool *ReadcachePool

	store          *logStore
	readcacheStore *readcacheLogStore
	admin          adminState
	metrics        *metrics

	// moveCooldowns records, for each hash range that was recently
	// moved, the wall-clock time at which it (and any range overlapping
	// its boundaries) becomes eligible to be moved again. Mutated only
	// by rebalance(), which runs single-threaded from running().
	moveCooldowns map[assignment.HashRange]time.Time

	// readcacheCooldowns tracks per-partition cooldowns for the
	// second slicer round (partition -> readcache instance). Mutated
	// only by rebalance() / planReadcacheAssignment.
	readcacheCooldowns readcacheMoveCooldowns

	// readcacheRing is the ring client the slicer consults to learn
	// which readcache instances are currently healthy. Nil when
	// running without a ring (tests, or operators who pin the
	// instance set via ReadcacheSlicer.Instances). Resolved on each
	// slicer round so scale-up/scale-down is picked up at most one
	// round (lease_lookahead by default) after the ring event.
	readcacheRing readcacheRingReader
}

// readcacheRingReader is the subset of *ring.Ring the slicer needs to
// enumerate healthy readcache instances. Modeled as an interface so
// tests can substitute a fake without spinning up a real KV.
type readcacheRingReader interface {
	GetAllHealthy(op ring.Operation) (ring.ReplicationSet, error)
}

// New creates and returns a new Rebalancer. readcachePool is required
// for production rebalance rounds; readcacheRing may be nil when the
// operator pins instances via ReadcacheSlicer.Instances.
func New(cfg Config, readcacheRing readcacheRingReader, readcachePool *ReadcachePool, registerer prometheus.Registerer, logger log.Logger) (*Rebalancer, error) {
	if readcachePool == nil {
		return nil, fmt.Errorf("readcache pool is required")
	}
	r := &Rebalancer{
		cfg:                cfg,
		logger:             logger,
		readcacheRing:      readcacheRing,
		readcachePool:      readcachePool,
		store:              newLogStore(),
		readcacheStore:     newReadcacheLogStore(),
		moveCooldowns:      make(map[assignment.HashRange]time.Time),
		readcacheCooldowns: make(readcacheMoveCooldowns),
		metrics:            newMetrics(registerer),
	}

	r.Service = services.NewBasicService(r.starting, r.running, nil)
	return r, nil
}

func (r *Rebalancer) starting(_ context.Context) error {
	level.Info(r.logger).Log("msg", "nautilus rebalancer starting",
		"lease_duration", r.cfg.LeaseDuration,
		"lease_lookahead", r.cfg.LeaseLookahead,
		"min_rebalance_interval", r.cfg.MinRebalanceInterval,
		"max_rebalance_interval", r.cfg.MaxRebalanceInterval,
		"data_dir", r.cfg.DataDir)

	if r.cfg.DataDir != "" {
		if err := os.MkdirAll(r.cfg.DataDir, 0o755); err != nil {
			return fmt.Errorf("creating rebalancer data dir %q: %w", r.cfg.DataDir, err)
		}

		assignmentFile := newLogFile(filepath.Join(r.cfg.DataDir, assignmentLogFilename), log.With(r.logger, "component", "assignment_log_file"))
		if entries, ok := assignmentFile.readAssignmentLog(); ok {
			level.Info(r.logger).Log("msg", "seeded assignment log from disk", "entries", len(entries))
			r.store.seedFromEntries(entries)
		}
		r.store.setPersistFn(assignmentFile.writeAssignmentLog, r.logger)

		readcacheFile := newLogFile(filepath.Join(r.cfg.DataDir, readcacheLogFilename), log.With(r.logger, "component", "readcache_log_file"))
		if entries, ok := readcacheFile.readReadcacheLog(); ok {
			level.Info(r.logger).Log("msg", "seeded readcache assignment log from disk", "entries", len(entries))
			r.readcacheStore.seedFromEntries(entries)
		}
		r.readcacheStore.setPersistFn(readcacheFile.writeReadcacheLog, r.logger)
	}

	// Auto-create the nautilus_ingest topic on startup so the dev-cell
	// boot order doesn't require a manual kafka-topics --create step.
	// Gated on the global -ingest-storage.kafka.auto-create-topic-enabled
	// so the symmetry with the production topic is exact: if you
	// opt into auto-creation there, you also get it here.
	//
	// CreateTopic is idempotent (TopicAlreadyExists is swallowed),
	// so it's safe to call on every restart. The override below
	// touches only the fields specific to the nautilus topic; all
	// connection/auth/TLS/SASL/etc. comes from the production
	// ingest-storage Kafka config.
	if r.cfg.Kafka.AutoCreateTopicEnabled && r.cfg.KafkaTopic != "" && r.cfg.PartitionCount > 0 {
		topicCfg := r.cfg.Kafka
		topicCfg.Topic = r.cfg.KafkaTopic
		topicCfg.AutoCreateTopicDefaultPartitions = int(r.cfg.PartitionCount)
		if err := ingest.CreateTopic(topicCfg, log.With(r.logger, "component", "rebalancer_topic_bootstrap")); err != nil {
			return fmt.Errorf("auto-creating nautilus kafka topic %q: %w", r.cfg.KafkaTopic, err)
		}
	}
	return nil
}

// running drives rebalance rounds with dynamic scheduling: each
// round computes the lease horizon (the soonest-expiring active or
// pre-issued lease) and schedules the next round to fire
// LeaseLookahead before that, clamped to [MinRebalanceInterval,
// MaxRebalanceInterval]. This puts the steady-state cadence at one
// round per LeaseDuration, with each round emitting the next
// successor lease LeaseLookahead in advance of the current one
// expiring.
func (r *Rebalancer) running(ctx context.Context) error {
	// First round runs immediately so the cluster gets an initial
	// assignment as soon as the rebalancer is up.
	timer := time.NewTimer(0)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
			if err := r.rebalance(ctx); err != nil {
				level.Warn(r.logger).Log("msg", "rebalance round failed", "err", err)
			}
			timer.Reset(r.nextRoundDelay(time.Now()))
		}
	}
}

// nextRoundDelay computes the time until the next rebalance round.
// In steady state the result equals LeaseDuration (one round per
// lease, scheduled LeaseLookahead before the current lease expires).
// On cold start (no leases yet) or after a full lease expiry the
// MinRebalanceInterval floor applies; the MaxRebalanceInterval
// ceiling guards against degenerate "horizon very far in the
// future" cases.
func (r *Rebalancer) nextRoundDelay(now time.Time) time.Duration {
	horizon := r.store.leaseHorizon(now)
	var delay time.Duration
	if horizon.IsZero() {
		// No active or pre-issued leases. Likely either a cold start
		// where the round above couldn't write to the log (e.g.
		// no active partitions yet) or a long outage that left every
		// lease expired. Retry at the floor.
		delay = r.cfg.MinRebalanceInterval
	} else {
		delay = horizon.Sub(now) - r.cfg.LeaseLookahead
	}
	if delay < r.cfg.MinRebalanceInterval {
		delay = r.cfg.MinRebalanceInterval
	}
	if r.cfg.MaxRebalanceInterval > 0 && delay > r.cfg.MaxRebalanceInterval {
		delay = r.cfg.MaxRebalanceInterval
	}
	return delay
}

// WatchAssignments implements NautilusRebalancerServer. It sends
// the live entries (active leases plus pre-issued successors) of
// the assignment log immediately on connect and then a fresh
// live-entry snapshot on every subsequent rebalance round that
// mutates the log (i.e. every round that pre-issues a successor
// lease, preempts an active lease, or creates a new lease for a
// reassignment). Expired entries are not transmitted: a fresh
// subscriber can never use them, and including them would make the
// gRPC message proportional to the full retention window
// (currently 24h) rather than the active+successor working set.
// The store conflates updates so a slow subscriber sees only the
// most recent snapshot.
//
// If the rebalancer has not yet completed its first apply() the
// initial Send is skipped; the subscriber waits on the updates
// channel for the first broadcast triggered by the cold-start or
// first slicer round. This prevents a freshly-restarted rebalancer
// from broadcasting an empty snapshot derived from stale persisted
// state (whose leases have all expired during the restart window).
func (r *Rebalancer) WatchAssignments(_ *WatchAssignmentsRequest, stream NautilusRebalancer_WatchAssignmentsServer) error {
	initial, updates, unsubscribe := r.store.subscribe(time.Now())
	defer unsubscribe()

	// subscribe returns initial=nil when the store has not yet run
	// its first apply(); skip the initial Send in that case and let
	// the first apply's broadcast (delivered on updates) prime the
	// subscriber.
	if initial != nil {
		if err := stream.Send(&WatchAssignmentsResponse{Entries: EntriesToProto(initial)}); err != nil {
			return err
		}
	}
	ctx := stream.Context()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case snap, ok := <-updates:
			if !ok {
				return nil
			}
			if err := stream.Send(&WatchAssignmentsResponse{Entries: EntriesToProto(snap)}); err != nil {
				return err
			}
		}
	}
}

// WatchReadcacheAssignments is the readcache-side analogue of
// WatchAssignments: instead of (hash range -> ingester partition) it
// streams (Kafka partition -> readcache instance) leases. The wire
// contract is identical (snapshot on connect, conflated updates,
// only live entries transmitted). The same first-apply gate applies:
// the initial Send is skipped until the readcache log has been
// touched by apply() at least once (cold start, regular slicer
// round, or admin reset), so a rebalancer restart never broadcasts
// an empty/expired view that would tell every readcache to drop all
// partitions.
func (r *Rebalancer) WatchReadcacheAssignments(_ *WatchReadcacheAssignmentsRequest, stream NautilusRebalancer_WatchReadcacheAssignmentsServer) error {
	initial, updates, unsubscribe := r.readcacheStore.subscribe(time.Now())
	defer unsubscribe()

	// See WatchAssignments for why we may skip the initial Send.
	if initial != nil {
		if err := stream.Send(&WatchReadcacheAssignmentsResponse{Entries: ReadcacheEntriesToProto(initial)}); err != nil {
			return err
		}
	}
	ctx := stream.Context()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case snap, ok := <-updates:
			if !ok {
				return nil
			}
			if err := stream.Send(&WatchReadcacheAssignmentsResponse{Entries: ReadcacheEntriesToProto(snap)}); err != nil {
				return err
			}
		}
	}
}

// activePartitionsForRound returns the partition ID set the
// rebalancer slices over for this round: [0, K) where K is
// ActivePartitionCount when set, otherwise PartitionCount.
func (r *Rebalancer) activePartitionsForRound() []int32 {
	n := int(r.cfg.ActivePartitionCount)
	if n <= 0 {
		n = int(r.cfg.PartitionCount)
	}
	if n <= 0 {
		return nil
	}
	out := make([]int32, n)
	for i := 0; i < n; i++ {
		out[i] = int32(i)
	}
	return out
}

func (r *Rebalancer) rebalance(ctx context.Context) error {
	// The ingester client pool requires an org ID in the context
	// (ClientUserHeaderInterceptor). Inject a synthetic one since
	// rebalancer RPCs are not tenant-scoped.
	ctx = user.InjectOrgID(ctx, "nautilus-rebalancer")

	activePartitions := r.activePartitionsForRound()
	if len(activePartitions) == 0 {
		level.Warn(r.logger).Log("msg", "no active partitions configured, skipping rebalance")
		return nil
	}

	current := r.store.latestActiveAssignment(time.Now())
	if current == nil {
		// Cold start: try to reconstruct the assignment from whatever
		// each owning pod (readcache when wired, ingester otherwise)
		// locally remembers (via GetHashRanges). On a rolling
		// rebalancer restart this preserves rebalanced state; on a
		// truly-cold cluster it falls back to FineEvenSplit.
		current = r.reconstructRound(ctx, activePartitions)
		if current == nil {
			current = assignment.FineEvenSplit(activePartitions, initialSlicesPerPartition)
			level.Info(r.logger).Log("msg", "initialized assignment with fine even split",
				"partitions", len(activePartitions),
				"slices_per_partition", initialSlicesPerPartition,
				"total_slices", len(current.Entries))
		} else {
			level.Info(r.logger).Log("msg", "initialized assignment from readcache reports",
				"partitions", len(activePartitions),
				"total_entries", len(current.Entries))
		}
		r.store.apply(time.Now(), current, r.cfg.LeaseDuration, r.cfg.LeaseLookahead, r.cfg.EntryRetention)
		level.Info(r.logger).Log(
			"msg", "cold start hash assignment log seeded",
			"entries", len(current.Entries),
			"subscribers", r.store.numSubscribers(),
		)
		r.pushRanges(ctx, current, time.Now())
		// Seed the readcache slicer too: without this the readcache
		// log stays empty until the next round, which is up to
		// LeaseDuration - LeaseLookahead away (3.5min by default).
		// During that gap distributors find no readcache owner for
		// any partition and readcache pods (which subscribe to
		// WatchReadcacheAssignments and trust the log as
		// authoritative) own nothing — so newly-written nautilus
		// data is silently unqueryable from readcache. Running the
		// slicer with empty load signals just produces an even
		// spread, which is exactly what we want at cold start.
		if r.cfg.ReadcacheSlicer.Enabled {
			if instances := r.activeReadcacheInstances(); len(instances) > 0 {
				if r.runReadcacheSlicer(time.Now(), activePartitions, nil, nil, instances) {
					level.Info(r.logger).Log("msg", "cold start readcache assignment log seeded")
				}
			}
		}
		return nil
	}

	now := time.Now()
	level.Info(r.logger).Log(
		"msg", "rebalance round starting",
		"active_partitions", len(activePartitions),
		"current_slices", len(current.Entries),
		"lease_horizon", r.store.leaseHorizon(now).Format(time.RFC3339),
		"hash_log_subscribers", r.store.numSubscribers(),
		"readcache_log_subscribers", r.readcacheStore.numSubscribers(),
	)

	rates, _, partitionTotals, partitionQuerySamples, unnamedPerInstance, err := r.collectRoundStats(ctx, current)
	if err != nil {
		level.Warn(r.logger).Log("msg", "failed to collect rates", "err", err)
		return nil
	}

	r.metrics.updateRound(partitionQuerySamples, unnamedPerInstance)
	r.pruneExpiredCooldowns(now)

	// Compute per-partition L (head-series). With the readcache pool
	// wired, L comes from per-partition HashRangeStats; otherwise fall
	// back to the ingester partition ring (max across the replica set).
	// L is observability-only since the slicer balances on sample rate.
	partitionLByPID := r.partitionLByPID(partitionTotals, activePartitions)

	lm := buildLoadMap(rates)
	partitionRateByPID := partitionLoadFromRates(rates, activePartitions)
	r.admin.setLastStats(lm, partitionLByPID, partitionRateByPID, activePartitions)

	// Snapshot pre-slicer state for the trace. Done BEFORE runSlicer
	// mutates anything: it returns a fresh assignment but inspects the
	// cooldown map and rates slice as-of `now`, and we want the snapshot
	// to reflect what the slicer actually saw.
	startEntries := append([]assignment.Entry(nil), current.Entries...)
	cooldownsSnapshot := cooldownsToWire(r.moveCooldowns)

	newAssignment, actions := r.runSlicer(current, rates, partitionRateByPID, activePartitions, now)
	if err := newAssignment.Validate(); err != nil {
		level.Error(r.logger).Log("msg", "generated invalid assignment", "err", err)
		return nil
	}

	r.recordMoveCooldowns(now, actions)

	actionSummary := countActions(actions)
	if n := actionSummary.moves + actionSummary.reassigns + actionSummary.splits + actionSummary.merges; n > 0 {
		level.Info(r.logger).Log(
			"msg", "slicer produced actions",
			"moves", actionSummary.moves,
			"reassigns", actionSummary.reassigns,
			"splits", actionSummary.splits,
			"merges", actionSummary.merges,
		)
	}

	hashLogChanged := r.store.apply(now, newAssignment, r.cfg.LeaseDuration, r.cfg.LeaseLookahead, r.cfg.EntryRetention)
	if hashLogChanged {
		level.Info(r.logger).Log(
			"msg", "hash assignment log updated",
			"live_entries", len(r.store.snapshot()),
			"lease_horizon", r.store.leaseHorizon(now).Format(time.RFC3339),
			"subscribers", r.store.numSubscribers(),
		)
	}
	r.pushRanges(ctx, newAssignment, now)

	readcacheLogChanged := false
	// Second slicer round: balance partition -> readcache instance
	// using the per-partition load signal we just collected. Only
	// runs when explicitly enabled and an instance set is available
	// (from the ring when wired, otherwise from the static config);
	// either gate disabled leaves the readcache log untouched, which
	// is the production default during the Phase 2A bring-up.
	if r.cfg.ReadcacheSlicer.Enabled {
		if instances := r.activeReadcacheInstances(); len(instances) > 0 {
			readcacheLogChanged = r.runReadcacheSlicer(now, activePartitions, partitionRateByPID, partitionQuerySamples, instances)
		}
	}

	// Compute round summary stats using L (memory series) so the admin
	// view tracks the same quantity the slicer balances.
	var totalL, maxL, minL int64
	minL = math.MaxInt64
	for _, pid := range activePartitions {
		l := partitionLByPID[pid]
		totalL += l
		if l > maxL {
			maxL = l
		}
		if l < minL {
			minL = l
		}
	}
	if minL == math.MaxInt64 {
		minL = 0
	}
	var meanL int64
	if len(activePartitions) > 0 {
		meanL = totalL / int64(len(activePartitions))
	}
	// Per the Slicer paper, load imbalance is defined as max / mean.
	// 1.0 means perfectly balanced; values above 1.0 indicate the
	// hottest partition exceeds the average by that factor.
	imbalance := 0.0
	if meanL > 0 {
		imbalance = float64(maxL) / float64(meanL)
	}
	movedFraction := 0.0
	hashSpaceSize := float64(uint64(math.MaxUint32) + 1)
	for _, a := range actions {
		if a.Kind == ActionMove || a.Kind == ActionReassign {
			movedFraction += float64(a.Range.Size()) / hashSpaceSize
		}
	}

	round := RoundLog{
		Time:           now,
		TotalL:         totalL,
		MeanL:          meanL,
		MaxL:           maxL,
		MinL:           minL,
		ImbalanceRatio: imbalance,
		NumEntries:     len(newAssignment.Entries),
		NumPartitions:  len(activePartitions),
		MovedFraction:  movedFraction,
		Actions:        actions,
	}

	r.admin.addTrace(Trace{
		SlicerVersion:         SlicerVersion,
		Round:                 round,
		Now:                   now,
		Start:                 startEntries,
		Rates:                 ratesToWire(rates),
		PartitionL:            partitionLByPID,
		PartitionQuerySamples: partitionQuerySamples,
		UnnamedQuerySamples:   unnamedPerInstance,
		ActivePartitions:      append([]int32(nil), activePartitions...),
		Cooldowns:             cooldownsSnapshot,
		Config: ConfigSnapshot{
			MovementBudget: r.cfg.MovementBudget,
			MoveCooldown:   r.cfg.MoveCooldown,
		},
		End: append([]assignment.Entry(nil), newAssignment.Entries...),
	})

	level.Info(r.logger).Log(
		"msg", "rebalance complete",
		"entries", len(newAssignment.Entries),
		"log_entries", len(r.store.snapshot()),
		"hash_log_changed", hashLogChanged,
		"readcache_log_changed", readcacheLogChanged,
		"moves", actionSummary.moves,
		"reassigns", actionSummary.reassigns,
		"splits", actionSummary.splits,
		"merges", actionSummary.merges,
		"moved_fraction", movedFraction,
		"imbalance_ratio", imbalance,
		"total_l", totalL,
		"max_l", maxL,
		"mean_l", meanL,
	)
	return nil
}

// withRPCTimeout wraps ctx with the configured per-call timeout. When
// the timeout is disabled (<=0), returns the parent context and a
// no-op cancel.
func (r *Rebalancer) withRPCTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if r.cfg.IngesterRPCTimeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, r.cfg.IngesterRPCTimeout)
}

// reconstructionQuorumNum / reconstructionQuorumDen together express the
// minimum fraction of expected readcache pods that must successfully return
// their hash ranges for reconstruction to be trusted. Below this, we
// fall back to FineEvenSplit rather than take a destructive action
// (e.g. blowing up a range's ownership) on a minority view.
const (
	reconstructionQuorumNum = 1
	reconstructionQuorumDen = 2
)

// reportedEntry is a (partition, range) pair collected from a
// GetHashRanges response during assignment reconstruction.
type reportedEntry struct {
	partitionID int32
	hr          assignment.HashRange
}

// sortReportedEntries sorts reported entries ascending by (Lo,
// partitionID). Used in tests and by reconstructAssignmentFromReadcache
// to establish stitchReportedEntries' precondition.
func sortReportedEntries(entries []reportedEntry) {
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].hr.Lo != entries[j].hr.Lo {
			return entries[i].hr.Lo < entries[j].hr.Lo
		}
		return entries[i].partitionID < entries[j].partitionID
	})
}

// stitchReportedEntries walks reported (partition, range) entries in
// sorted (Lo, partitionID) order and produces a sequence of entries
// covering [0, math.MaxUint32] with no gaps and no overlaps. Overlaps
// between different partitions are resolved first-replica-wins: the
// earlier entry keeps its coverage and any later entry is truncated
// to [cursor, Hi] or dropped entirely if fully covered. Gaps (including
// a leading gap before the first reported Lo and a trailing gap after
// the last reported Hi) are filled with single entries round-robin
// across activePartitions.
//
// Precondition: sorted is sorted ascending by (hr.Lo, partitionID).
// Returns entries that are sorted and non-overlapping, with the first
// Lo == 0 and the last Hi == math.MaxUint32, i.e. ready for
// Assignment.Validate.
func stitchReportedEntries(sorted []reportedEntry, activePartitions []int32, logger log.Logger) []assignment.Entry {
	out := make([]assignment.Entry, 0, len(sorted)+1)
	// gapRR is the round-robin cursor for assigning gap-filler entries
	// to active partitions. Advances only when a filler is emitted.
	gapRR := 0
	emitGap := func(lo, hi uint32) {
		if len(activePartitions) == 0 {
			return
		}
		pid := activePartitions[gapRR%len(activePartitions)]
		gapRR++
		out = append(out, assignment.Entry{
			Range:       assignment.HashRange{Lo: lo, Hi: hi},
			PartitionID: pid,
		})
	}

	// cursor tracks the next uncovered hash. Tracked as uint64 so we
	// can represent "past MaxUint32" (i.e. fully covered) without
	// overflow.
	var cursor uint64
	for _, e := range sorted {
		lo := uint64(e.hr.Lo)
		hi := uint64(e.hr.Hi)

		if lo > cursor {
			// Gap before this entry.
			emitGap(uint32(cursor), uint32(lo-1))
			cursor = lo
		}

		if hi < cursor {
			// Entry is entirely behind the cursor; first-replica
			// already won this span.
			level.Warn(logger).Log(
				"msg", "reconstructAssignment: dropping overlapped range",
				"partition", e.partitionID,
				"lo", e.hr.Lo,
				"hi", e.hr.Hi,
				"cursor", cursor,
			)
			continue
		}

		if lo < cursor {
			// Partial overlap: truncate to [cursor, hi].
			level.Warn(logger).Log(
				"msg", "reconstructAssignment: truncating overlapped range",
				"partition", e.partitionID,
				"orig_lo", e.hr.Lo,
				"orig_hi", e.hr.Hi,
				"truncated_lo", cursor,
			)
			lo = cursor
		}

		out = append(out, assignment.Entry{
			Range:       assignment.HashRange{Lo: uint32(lo), Hi: uint32(hi)},
			PartitionID: e.partitionID,
		})
		cursor = hi + 1
	}

	// Trailing gap up to MaxUint32, if any.
	if cursor <= uint64(math.MaxUint32) {
		emitGap(uint32(cursor), math.MaxUint32)
	}

	return out
}

const (
	// initialSlicesPerPartition is the number of sub-ranges each
	// partition gets in the first assignment. Starting fine gives
	// the move step enough granularity on the first round.
	initialSlicesPerPartition = 64

	// minSlicesPerPartition is the floor below which merging stops.
	// Matches Slicer paper's "50 slices per task" guideline.
	minSlicesPerPartition = 50

	// maxSlicesPerPartition is the ceiling above which splitting
	// stops. Matches Slicer paper's "150 slices per task" guideline.
	maxSlicesPerPartition = 150

	// mergeChurnBudget is the max fraction of keyspace that merging
	// may move (Slicer paper: 1%).
	mergeChurnBudget = 0.01
)

// rangeRate is one entry from a reporter's (ingester or readcache)
// HashRangeStats response. Per the reframed load model only the
// per-range in-memory TSDB head series count (R_r) feeds the slicer.
//
// partitionID identifies which Kafka partition this entry's series
// belong to. For readcache reporters, the same (Lo, Hi) range can
// appear with different partitionIDs: the current owner (growth)
// plus any partition still holding residue series after a recent
// move. Keying load by (partitionID, range) lets the slicer
// distinguish growth from residue. For ingester reporters,
// partitionID is filled in from the current assignment at load-map
// build time.
type rangeRate struct {
	hr          assignment.HashRange
	series      int64
	sampleRate  float64
	partitionID int32
}

// rangeLoad carries a single assignment entry alongside its cost
// metrics. `load` is the float the slicer's Phase 2 merge score and
// Phase 4 split-threshold arithmetic balance on; today it is the
// per-range samples-per-second EWMA (rangeRate.sampleRate). `series`
// is kept as observability metadata so the trace tooling can still
// surface head cardinality alongside the throughput signal, but no
// slicer phase reads it.
//
// The series → sample_rate swap is intentional: head-series counts
// lag new tenants by minutes (Head growth is gated by churn) and
// over-weight residue from recently-moved ranges, while sample rate
// reacts at the EWMA cadence and matches "what work does this
// readcache do?". A future revision will blend both signals; for
// now load == sampleRate.
type rangeLoad struct {
	entry  assignment.Entry
	load   float64 // per-second samples EWMA, used by merge/split scoring
	series int64   // raw in-memory TSDB head series count (observability only)
}

// runSlicer implements the Slicer weighted-move algorithm (Adya et al.,
// OSDI'16, Section 4.4.1). Phases:
//
//  1. Reassign slices from inactive partitions.
//  2. Merge adjacent cold slices to defragment (cap: 1% churn, floor:
//     minSlicesPerPartition).
//  3. Weighted-move: greedily move slices from the hottest partition
//     (highest sample rate, minus moves already booked this round)
//     to the coldest (lowest sample rate plus moves already booked
//     this round), gated by a per-source movable budget of
//     max(0, load - meanLoad). Aggregate per-round churn is bounded
//     by cfg.MovementBudget (default 9% of hash space).
//  4. Split hot slices (>2× mean slice load) without changing
//     assignments. Cap: maxSlicesPerPartition.
//
// The `now` parameter is used to evaluate per-range move cooldowns
// (see Config.MoveCooldown). Pass time.Now() in production; tests can
// pass a deterministic value or the zero time to disable cooldown
// filtering.
//
// partitionRateByPID provides the per-partition samples-per-second
// EWMA — the slicer's primary balancing signal. When nil/empty
// (legacy callers / unit tests) Phase 3 falls back to summing
// per-range sampleRate from rates so the phase still exercises.
func (r *Rebalancer) runSlicer(
	current *assignment.Assignment,
	rates []rangeRate,
	partitionRateByPID map[int32]float64,
	activePartitions []int32,
	now time.Time,
) (*assignment.Assignment, []Action) {
	lm := buildLoadMap(rates)
	numPartitions := len(activePartitions)
	var actions []Action

	activeSet := make(map[int32]bool, numPartitions)
	for _, pid := range activePartitions {
		activeSet[pid] = true
	}

	// Validate the input. The slicer's phases assume current tiles
	// the full hash space without gaps or overlaps; if it doesn't, the
	// downstream Validate on the round's output will fail and we won't
	// know whether the bug is in current or in a phase. Logging here
	// removes that ambiguity.
	if err := current.Validate(); err != nil {
		level.Error(r.logger).Log("msg", "slicer received invalid input assignment", "err", err, "num_entries", len(current.Entries))
	}

	// --- Phase 1: build entries, reassign inactive partitions ----------
	entries := make([]rangeLoad, len(current.Entries))
	rrIdx := 0
	for i, e := range current.Entries {
		series := lm.seriesAt(e.PartitionID, e.Range)
		rate := lm.sampleRateAt(e.PartitionID, e.Range)
		entries[i] = rangeLoad{
			entry:  e,
			load:   rate,
			series: series,
		}
		if !activeSet[e.PartitionID] {
			newPID := activePartitions[rrIdx%numPartitions]
			actions = append(actions, Action{
				Kind:     ActionReassign,
				Range:    e.Range,
				FromPart: e.PartitionID,
				ToPart:   newPID,
				Detail:   fmt.Sprintf("inactive partition %d → %d", e.PartitionID, newPID),
			})
			entries[i].entry.PartitionID = newPID
			rrIdx++
		}
	}
	if err := validateSlicerPhaseEntries(entries); err != nil {
		// Phase 1 only mutates PartitionID, so reaching this branch
		// means current itself was malformed in a way Validate() didn't
		// catch above (or after the input check). Log; there's no
		// useful pre-Phase-1 state to revert to.
		logSlicerPhaseError(r.logger, entries, "phase1-reassign", err)
	}

	totalLoad := 0.0
	for _, rl := range entries {
		totalLoad += rl.load
	}
	targetLoad := totalLoad / float64(numPartitions)

	// --- Phase 2: merge adjacent cold slices (defragment) -------------
	pre2Entries := snapshotRangeLoads(entries)
	pre2ActionsLen := len(actions)
	if len(entries) > minSlicesPerPartition*numPartitions {
		meanSliceLoad := totalLoad / float64(len(entries))
		mergeMoveBudget := mergeChurnBudget * float64(uint64(math.MaxUint32)+1)
		var mergeActions []Action
		entries, mergeActions = mergeAdjacentCold(entries, meanSliceLoad, mergeMoveBudget, targetLoad, minSlicesPerPartition*numPartitions, minSlicesPerPartition)
		actions = append(actions, mergeActions...)
	}
	if err := validateSlicerPhaseEntries(entries); err != nil {
		logSlicerPhaseError(r.logger, entries, "phase2-merge", err)
		entries = pre2Entries
		actions = actions[:pre2ActionsLen]
	}

	// --- Phase 3: weighted-move using per-partition sample rate ---------
	pre3Entries := snapshotRangeLoads(entries)
	pre3ActionsLen := len(actions)
	// The slicer's float-precision load signal is the sum of
	// per-(partition, range) sample rates. The caller usually computes
	// this once and passes it in; if it didn't (older test fixtures),
	// reconstruct it from rates here.
	partitionLoadByPID := partitionRateByPID
	if partitionLoadByPID == nil {
		partitionLoadByPID = partitionLoadFromRates(rates, activePartitions)
	}
	phase3Actions := r.runPhase3(entries, partitionLoadByPID, activePartitions, now)
	actions = append(actions, phase3Actions...)
	if err := validateSlicerPhaseEntries(entries); err != nil {
		logSlicerPhaseError(r.logger, entries, "phase3-move", err)
		entries = pre3Entries
		actions = actions[:pre3ActionsLen]
	}

	// --- Phase 4: split hot slices ----------------------------------------
	// Only split ranges on OVERLOADED partitions (>= target load).
	// Splitting ranges on cold partitions just adds fragmentation
	// without helping rebalancing.
	//
	// The split threshold is computed from ranges with non-zero load
	// to avoid the feedback loop where zero-load fragments from prior
	// splits drag down the mean and cause everything to look "hot".
	pre4Entries := snapshotRangeLoads(entries)
	pre4ActionsLen := len(actions)
	maxTotal := maxSlicesPerPartition * numPartitions
	if len(entries) < maxTotal {
		partitionLoads := computePartitionLoads(entries)

		var nonZeroCount int
		var nonZeroLoad float64
		for _, rl := range entries {
			if rl.load > 0 {
				nonZeroCount++
				nonZeroLoad += rl.load
			}
		}
		meanSliceLoad := nonZeroLoad / math.Max(float64(nonZeroCount), 1)
		splitThreshold := 2.0 * meanSliceLoad

		overloaded := make(map[int32]bool, numPartitions)
		for _, pid := range activePartitions {
			if partitionLoads[pid] >= targetLoad {
				overloaded[pid] = true
			}
		}

		var newEntries []rangeLoad
		for _, rl := range entries {
			if len(newEntries) >= maxTotal {
				newEntries = append(newEntries, rl)
				continue
			}
			if rl.load > splitThreshold && rl.entry.Range.Size() > 1 && overloaded[rl.entry.PartitionID] {
				mid := rl.entry.Range.Lo + uint32((uint64(rl.entry.Range.Hi)-uint64(rl.entry.Range.Lo))/2)
				left := assignment.HashRange{Lo: rl.entry.Range.Lo, Hi: mid}
				right := assignment.HashRange{Lo: mid + 1, Hi: rl.entry.Range.Hi}
				// Split halves haven't been observed per-(partition,
				// range) yet (the readcache only sees them after the
				// next SetHashRanges). The fallback below distributes
				// the parent's load proportionally to keep Phase 2
				// from immediately re-merging.
				leftSeries := lm.seriesAt(rl.entry.PartitionID, left)
				rightSeries := lm.seriesAt(rl.entry.PartitionID, right)
				leftLoad := lm.sampleRateAt(rl.entry.PartitionID, left)
				rightLoad := lm.sampleRateAt(rl.entry.PartitionID, right)
				if leftLoad == 0 && rightLoad == 0 && rl.load > 0 {
					// Newly split sub-ranges have no per-range data yet.
					// Distribute the parent's load proportionally so the
					// next phase doesn't immediately re-merge them.
					leftFraction := float64(left.Size()) / float64(rl.entry.Range.Size())
					leftLoad = rl.load * leftFraction
					rightLoad = rl.load * (1 - leftFraction)
					leftSeries = int64(float64(rl.series) * leftFraction)
					rightSeries = rl.series - leftSeries
				}
				newEntries = append(newEntries,
					rangeLoad{entry: assignment.Entry{Range: left, PartitionID: rl.entry.PartitionID}, load: leftLoad, series: leftSeries},
					rangeLoad{entry: assignment.Entry{Range: right, PartitionID: rl.entry.PartitionID}, load: rightLoad, series: rightSeries},
				)
				actions = append(actions, Action{
					Kind:   ActionSplit,
					Range:  rl.entry.Range,
					ToPart: rl.entry.PartitionID,
					Detail: fmt.Sprintf("series=%d > threshold=%.0f, split on P%d", rl.series, splitThreshold, rl.entry.PartitionID),
				})
			} else {
				newEntries = append(newEntries, rl)
			}
		}
		entries = newEntries
	}
	if err := validateSlicerPhaseEntries(entries); err != nil {
		logSlicerPhaseError(r.logger, entries, "phase4-split", err)
		entries = pre4Entries
		actions = actions[:pre4ActionsLen]
	}

	// --- Build result --------------------------------------------------
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].entry.Range.Lo < entries[j].entry.Range.Lo
	})

	result := &assignment.Assignment{
		Entries: make([]assignment.Entry, len(entries)),
	}
	for i, rl := range entries {
		result.Entries[i] = rl.entry
	}
	return result, actions
}

// snapshotRangeLoads returns a defensive copy of entries. Used by
// runSlicer to enable per-phase rollback when a phase corrupts the
// invariants Assignment.Validate checks. rangeLoad is a value type
// (no pointers) so a single slice copy suffices.
func snapshotRangeLoads(entries []rangeLoad) []rangeLoad {
	out := make([]rangeLoad, len(entries))
	copy(out, entries)
	return out
}

// validateSlicerPhaseEntries reports whether the snapshot of entries
// (as a sorted Assignment) tiles [0, MaxUint32] without gaps or
// overlaps. Returns nil on success; on failure returns the same error
// shape Assignment.Validate produces, so callers can include it in
// the round's error log to identify the offending boundary.
func validateSlicerPhaseEntries(entries []rangeLoad) error {
	if len(entries) == 0 {
		return fmt.Errorf("entries is empty")
	}
	snapshot := make([]assignment.Entry, len(entries))
	for i, rl := range entries {
		snapshot[i] = rl.entry
	}
	sort.Slice(snapshot, func(i, j int) bool {
		return snapshot[i].Range.Lo < snapshot[j].Range.Lo
	})
	a := assignment.Assignment{Entries: snapshot}
	return a.Validate()
}

// logSlicerPhaseError logs a phase corruption with a small window of
// context around the offending boundary. The actions list (kept in
// the round trace) plus the boundary indices usually pin the bug down
// to a specific phase plus a specific input shape, which is what we
// need to fix the underlying cause.
func logSlicerPhaseError(logger log.Logger, entries []rangeLoad, phase string, err error) {
	snapshot := make([]assignment.Entry, len(entries))
	for i, rl := range entries {
		snapshot[i] = rl.entry
	}
	sort.Slice(snapshot, func(i, j int) bool {
		return snapshot[i].Range.Lo < snapshot[j].Range.Lo
	})
	ctxLo, ctxHi := slicerInvalidBoundaryWindow(snapshot)
	level.Error(logger).Log(
		"msg", "slicer phase produced invalid intermediate state, reverting",
		"phase", phase,
		"err", err,
		"num_entries", len(snapshot),
		"window_first_idx", ctxLo,
		"window_last_idx", ctxHi,
		"window", formatSlicerWindow(snapshot, ctxLo, ctxHi),
	)
}

// slicerInvalidBoundaryWindow returns the [first, last] index range
// of entries to log as context around the first invalid boundary in
// snapshot. Returns (-1, -1) if no boundary is invalid (caller should
// have early-returned in that case). The window is small (up to 5
// entries) to keep log lines bounded.
func slicerInvalidBoundaryWindow(snapshot []assignment.Entry) (int, int) {
	if len(snapshot) == 0 {
		return -1, -1
	}
	if snapshot[0].Range.Lo != 0 {
		// Missing prefix. Show the first few entries to confirm the
		// gap location.
		end := 4
		if end >= len(snapshot) {
			end = len(snapshot) - 1
		}
		return 0, end
	}
	for i := 1; i < len(snapshot); i++ {
		if snapshot[i].Range.Lo != snapshot[i-1].Range.Hi+1 {
			start := i - 2
			if start < 0 {
				start = 0
			}
			end := i + 2
			if end >= len(snapshot) {
				end = len(snapshot) - 1
			}
			return start, end
		}
	}
	if snapshot[len(snapshot)-1].Range.Hi != math.MaxUint32 {
		start := len(snapshot) - 5
		if start < 0 {
			start = 0
		}
		return start, len(snapshot) - 1
	}
	return -1, -1
}

// formatSlicerWindow renders [first, last] entries from snapshot as
// a compact string for logging.
func formatSlicerWindow(snapshot []assignment.Entry, first, last int) string {
	if first < 0 || last < 0 || first > last || first >= len(snapshot) {
		return ""
	}
	if last >= len(snapshot) {
		last = len(snapshot) - 1
	}
	var b []byte
	for i := first; i <= last; i++ {
		if i > first {
			b = append(b, ',', ' ')
		}
		b = append(b, []byte(fmt.Sprintf("[%d]={Lo=%d,Hi=%d,P=%d}", i, snapshot[i].Range.Lo, snapshot[i].Range.Hi, snapshot[i].PartitionID))...)
	}
	return string(b)
}

// runPhase3 runs the weighted-move phase and appends any resulting
// moves to actions. It mutates `entries` in place (updating PartitionID
// for moved ranges). The source/destination asymmetry is handled as
// follows:
//
//   - Source side: thisRoundMoves[pid] accumulates load already booked
//     off this partition during this iteration. The effective source
//     load is `load - thisRoundMoves[pid]`. A partition with exhausted
//     budget has effective load at or below meanLoad and is not
//     selected as hot. There is no cross-round bookkeeping — sample
//     rate is a near-instantaneous signal: as soon as a range is
//     reassigned, the writes flow to the new owner and the source's
//     reported rate for that range decays via the readcache's EWMA
//     tick well within one rebalance interval. The TSDB-head
//     compaction-interval discount that the old series-based slicer
//     needed does not apply here.
//
//   - Destination side: a within-round plannedAdded[pid] inflates the
//     effective cold load so the loop spreads moves across multiple
//     cold partitions rather than stacking on one. No cross-round
//     destination state: by the next round, the destination's
//     reported sample rate already reflects writes routed to it.
//
// partitionLoadByPID is the float-precision per-partition load
// (sum of per-range samples-per-second) used by Phase 3's hot/cold
// selection and movable-budget computation. Empty / zero entries
// behave the same as "no load": the partition is never selected as
// hot (movable is zero) and is always the coldest candidate.
func (r *Rebalancer) runPhase3(
	entries []rangeLoad,
	partitionLoadByPID map[int32]float64,
	activePartitions []int32,
	now time.Time,
) []Action {
	numPartitions := len(activePartitions)
	if numPartitions == 0 {
		return nil
	}

	// Build effective load snapshot. If no per-partition load map was
	// provided (legacy callers / unit tests), fall back to the
	// per-range-sum approximation so the phase still exercises.
	useRealL := len(partitionLoadByPID) > 0
	effL := make(map[int32]float64, numPartitions)
	if useRealL {
		for _, pid := range activePartitions {
			effL[pid] = partitionLoadByPID[pid]
		}
	} else {
		for _, rl := range entries {
			effL[rl.entry.PartitionID] += rl.load
		}
	}

	var totalL float64
	for _, pid := range activePartitions {
		totalL += effL[pid]
	}
	var meanL float64
	if numPartitions > 0 {
		meanL = totalL / float64(numPartitions)
	}

	// plannedAdded accumulates within-round additions per destination,
	// and thisRoundMoves accumulates within-round removals per source.
	// Both are local to this call so they reset between rounds.
	plannedAdded := make(map[int32]float64, numPartitions)
	thisRoundMoves := make(map[int32]float64, numPartitions)

	effectiveSource := func(pid int32) float64 {
		return effL[pid] - thisRoundMoves[pid]
	}

	effectiveDest := func(pid int32) float64 {
		return effL[pid] + plannedAdded[pid]
	}

	movable := func(pid int32) float64 {
		s := effectiveSource(pid)
		if s <= meanL {
			return 0
		}
		return s - meanL
	}

	movementBudget := r.cfg.MovementBudget * float64(uint64(math.MaxUint32)+1)
	var moved float64

	// Partitions excluded from "hottest" consideration: those that had
	// no profitable range to move when examined. Without this guard a
	// single budget-exhausted-but-still-nominally-hot partition could
	// stall the whole round. Separate from movable-based filtering so
	// that moving a range off P1 doesn't unintentionally unblock a
	// previously-exhausted P0 — once excluded, stays excluded.
	excludedHot := make(map[int32]bool)

	var actions []Action

	for iter := 0; iter < len(entries); iter++ {
		// Hot: argmax effectiveSource among partitions with movable > 0
		// and not excluded.
		var hotPID, coldPID int32
		hotL := math.Inf(-1)
		coldL := math.Inf(1)
		hotFound := false
		coldFound := false
		for _, pid := range activePartitions {
			if !excludedHot[pid] && movable(pid) > 0 {
				s := effectiveSource(pid)
				if s > hotL {
					hotL = s
					hotPID = pid
					hotFound = true
				}
			}
		}
		for _, pid := range activePartitions {
			d := effectiveDest(pid)
			if d < coldL {
				coldL = d
				coldPID = pid
				coldFound = true
			}
		}
		if !hotFound || !coldFound || hotPID == coldPID {
			break
		}

		mov := movable(hotPID)

		bestIdx := -1
		var bestScore float64
		for j, rl := range entries {
			if rl.entry.PartitionID != hotPID {
				continue
			}
			if r.isInMoveCooldown(now, rl.entry.Range) {
				continue
			}
			moveCost := float64(rl.entry.Range.Size())
			if moved+moveCost > movementBudget {
				continue
			}
			// Per-source movable budget: can't move more load off
			// than the "above mean" surplus (net of recent moves).
			if rl.load > mov {
				continue
			}
			// Imbalance improvement: the spread between hot and cold
			// should narrow after the move. Use absolute distance
			// from meanL on both sides.
			newHot := hotL - rl.load
			newCold := coldL + rl.load
			imbalanceBefore := math.Abs(hotL-meanL) + math.Abs(coldL-meanL)
			imbalanceAfter := math.Abs(newHot-meanL) + math.Abs(newCold-meanL)
			improvement := imbalanceBefore - imbalanceAfter
			if improvement <= 0 {
				continue
			}
			score := improvement / moveCost
			if score > bestScore {
				bestScore = score
				bestIdx = j
			}
		}

		if bestIdx < 0 {
			// Either every range is in cooldown, over-budget, too
			// large for the movable budget, or every potential move
			// yields no imbalance improvement. Exclude this hot and
			// try the next-hottest candidate; don't terminate Phase 3
			// because of one stuck source.
			excludedHot[hotPID] = true
			continue
		}

		fromPID := entries[bestIdx].entry.PartitionID
		moved += float64(entries[bestIdx].entry.Range.Size())
		loadMoved := entries[bestIdx].load
		seriesMoved := entries[bestIdx].series
		entries[bestIdx].entry.PartitionID = coldPID

		// Update phase-3 bookkeeping. plannedAdded delays re-picking
		// the same cold in the next iteration; thisRoundMoves feeds
		// movable() so the hot's budget shrinks as we move off it.
		plannedAdded[coldPID] += loadMoved
		thisRoundMoves[fromPID] += loadMoved

		actions = append(actions, Action{
			Kind:     ActionMove,
			Range:    entries[bestIdx].entry.Range,
			FromPart: fromPID,
			ToPart:   coldPID,
			Series:   seriesMoved,
			Detail:   fmt.Sprintf("L=%.2f meanL=%.2f, P%d→P%d, load=%.2f series=%d, movable=%.2f", hotL, meanL, fromPID, coldPID, loadMoved, seriesMoved, mov),
		})
	}

	return actions
}

// computePartitionLoads sums the per-range combined load for each
// partition. Used by the defragmentation (merge) and split phases.
// Phase 3 no longer uses this; it ranks by partitionLByPID (from
// ingester TotalActiveSeries) instead.
func computePartitionLoads(entries []rangeLoad) map[int32]float64 {
	m := make(map[int32]float64)
	for _, rl := range entries {
		m[rl.entry.PartitionID] += rl.load
	}
	return m
}

// partitionRangeKey is the (partition, range) key for loadMap.
// Two reporters reporting load for the same hash range with different
// partition IDs (e.g. current owner + a residue holder) end up as
// distinct entries.
type partitionRangeKey struct {
	partitionID int32
	hr          assignment.HashRange
}

// partitionLoadFromRates returns the per-partition sum of
// sample-rate EWMAs, restricted to the active partition set. The
// caller is responsible for ensuring rates were aggregated by
// (partition, range) — passing raw per-reporter rangeRate slices
// over-counts ranges that appear on multiple reporters; in practice
// the readcache path emits at most one entry per (partition, range)
// so this is a non-issue today. activePartitions seeds zero
// entries so the slicer's iteration order is deterministic even
// for partitions with no reported load yet.
func partitionLoadFromRates(rates []rangeRate, activePartitions []int32) map[int32]float64 {
	out := make(map[int32]float64, len(activePartitions))
	for _, pid := range activePartitions {
		out[pid] = 0
	}
	for _, rr := range rates {
		out[rr.partitionID] += rr.sampleRate
	}
	return out
}

// loadMap holds per-(partition, range) load signals: raw series
// counts (for observability) and samples-per-second EWMA (the
// rebalancer's primary balancing signal). Construct via buildLoadMap
// and query via seriesAt() / sampleRateAt().
type loadMap struct {
	series     map[partitionRangeKey]int64
	sampleRate map[partitionRangeKey]float64
}

// buildLoadMap aggregates per-(partition, range) signals across all
// reporters by taking the max over reports of the same (partition,
// range). Max (not sum) is intentional: where multiple healthy
// owners report counts for the same partition (mirrored ingester
// replicas), they are mirrors and summing would scale the per-range
// signal by the replication factor while `partitionL` is on a 1×
// (max-over-owners) scale. In the readcache path each (partition,
// range) is reported by at most one instance so max reduces to
// passthrough.
//
// Note that this is keyed by (partition, range), NOT just range:
// residue on a previous owner's partition is reported with that
// partition's id, separately from growth on the new owner.
//
// sampleRate is the slicer's primary load signal; series is retained
// only for observability on the admin page. Reporters that fail to
// populate sampleRate (e.g. legacy ingester instances) will see
// their ranges treated as zero-load, which is the correct behavior
// once the readcache is the authoritative source — a ranges with
// no rate signal cannot be balanced against ones with one.
func buildLoadMap(rates []rangeRate) *loadMap {
	lm := &loadMap{
		series:     make(map[partitionRangeKey]int64, len(rates)),
		sampleRate: make(map[partitionRangeKey]float64, len(rates)),
	}
	for _, rr := range rates {
		k := partitionRangeKey{partitionID: rr.partitionID, hr: rr.hr}
		if rr.series > lm.series[k] {
			lm.series[k] = rr.series
		}
		if rr.sampleRate > lm.sampleRate[k] {
			lm.sampleRate[k] = rr.sampleRate
		}
	}
	return lm
}

// seriesAt returns the raw in-memory TSDB head series count for the
// given (partition, range), or 0 if the pair is unknown.
//
// Callers in the slicer pass the partition currently assigned to the
// range. Any residue on a previous owner is recorded under that
// previous owner's partition id, so it does NOT contribute to the
// load returned here for the current owner.
func (lm *loadMap) seriesAt(partitionID int32, hr assignment.HashRange) int64 {
	return lm.series[partitionRangeKey{partitionID: partitionID, hr: hr}]
}

// sampleRateAt returns the samples-per-second EWMA for the given
// (partition, range), or 0 if the pair is unknown. This is the
// primary load signal used by the slicer's Phase 2/3/4 scoring;
// seriesAt is retained only for observability and trace continuity.
func (lm *loadMap) sampleRateAt(partitionID int32, hr assignment.HashRange) float64 {
	return lm.sampleRate[partitionRangeKey{partitionID: partitionID, hr: hr}]
}

// isInMoveCooldown reports whether the given range overlaps any range
// that was moved within the configured cooldown window. Splits and
// merges that happen between the original move and now are handled
// implicitly: any overlap with a cooled-down ancestor's boundaries
// disqualifies the candidate. Always returns false when the cooldown
// is disabled (cfg.MoveCooldown <= 0) or no cooldowns are tracked.
func (r *Rebalancer) isInMoveCooldown(now time.Time, hr assignment.HashRange) bool {
	if r.cfg.MoveCooldown <= 0 || len(r.moveCooldowns) == 0 {
		return false
	}
	for cooled, deadline := range r.moveCooldowns {
		if !now.Before(deadline) {
			continue
		}
		if hashRangesOverlap(hr, cooled) {
			return true
		}
	}
	return false
}

// recordMoveCooldowns scans the slicer's actions and starts a cooldown
// timer for every ActionMove. ActionReassign is intentionally excluded:
// it's a recovery action triggered by an inactive partition, not a
// load-balancing decision, so cooldowning it would needlessly delay
// recovery.
func (r *Rebalancer) recordMoveCooldowns(now time.Time, actions []Action) {
	if r.cfg.MoveCooldown <= 0 {
		return
	}
	deadline := now.Add(r.cfg.MoveCooldown)
	for _, a := range actions {
		if a.Kind != ActionMove {
			continue
		}
		if r.moveCooldowns == nil {
			r.moveCooldowns = make(map[assignment.HashRange]time.Time)
		}
		// Use the post-move (current) range as the cooldown key. If a
		// later round splits or merges this range, the overlap test
		// in isInMoveCooldown will still match.
		r.moveCooldowns[a.Range] = deadline
	}
}

// pruneExpiredCooldowns drops cooldown entries whose deadline has passed.
func (r *Rebalancer) pruneExpiredCooldowns(now time.Time) {
	for hr, deadline := range r.moveCooldowns {
		if !now.Before(deadline) {
			delete(r.moveCooldowns, hr)
		}
	}
}

// hashRangesOverlap returns true if the two ranges share at least one
// hash value (closed intervals on both sides).
func hashRangesOverlap(a, b assignment.HashRange) bool {
	return a.Lo <= b.Hi && b.Lo <= a.Hi
}

// mergeAdjacentCold merges adjacent cold slices to defragment the
// assignment, following the Slicer paper (Section 4.4.1, Phase 3).
//
// Same-partition adjacent slices are merged directly.
// Cross-partition adjacent slices are merged by moving the smaller
// slice onto the other's partition, then combining into one range.
//
// Constraints:
//   - merged load < meanSliceLoad
//   - receiving partition load stays below maxPartitionLoad (target * 1.5)
//   - total churn stays within churnBudget
//   - total entries don't drop below minEntries
//   - cross-partition merges never push the donor partition below
//     perPartitionFloor entries. Without this floor the merge phase
//     can drain a lightly-loaded partition completely (every range is
//     "cold" relative to meanSliceLoad and gets absorbed by neighbours
//     over a few rounds), at which point traffic to that partition's
//     keyspace flips to other ingesters until Phase 3 floods it back.
func mergeAdjacentCold(entries []rangeLoad, meanSliceLoad, churnBudget, targetLoad float64, minEntries, perPartitionFloor int) ([]rangeLoad, []Action) {
	if len(entries) <= 1 || len(entries) <= minEntries {
		return entries, nil
	}

	maxPartitionLoad := targetLoad * 1.5
	partitionLoads := computePartitionLoads(entries)
	partitionEntries := make(map[int32]int, len(partitionLoads))
	for _, rl := range entries {
		partitionEntries[rl.entry.PartitionID]++
	}
	var churned float64
	var actions []Action

	result := []rangeLoad{entries[0]}
	for i := 1; i < len(entries); i++ {
		if len(result)+len(entries)-i <= minEntries {
			result = append(result, entries[i:]...)
			break
		}
		prev := &result[len(result)-1]
		curr := entries[i]

		if prev.entry.Range.Hi+1 != curr.entry.Range.Lo {
			result = append(result, curr)
			continue
		}

		mergedLoad := prev.load + curr.load
		if mergedLoad >= meanSliceLoad {
			result = append(result, curr)
			continue
		}

		if prev.entry.PartitionID == curr.entry.PartitionID {
			if partitionLoads[prev.entry.PartitionID] <= maxPartitionLoad {
				mergeCost := float64(curr.entry.Range.Size())
				if churned+mergeCost <= churnBudget {
					merged := assignment.HashRange{Lo: prev.entry.Range.Lo, Hi: curr.entry.Range.Hi}
					actions = append(actions, Action{
						Kind:   ActionMerge,
						Range:  merged,
						ToPart: prev.entry.PartitionID,
						Detail: fmt.Sprintf("same-partition merge on P%d, combined load=%.4f", prev.entry.PartitionID, mergedLoad),
					})
					prev.entry.Range = merged
					prev.load = mergedLoad
					prev.series += curr.series
					churned += mergeCost
					partitionEntries[prev.entry.PartitionID]--
					continue
				}
			}
		} else {
			var receiverPID, donorPID int32
			var movedSize float64
			var donorLoad float64
			if prev.load >= curr.load {
				receiverPID = prev.entry.PartitionID
				donorPID = curr.entry.PartitionID
				movedSize = float64(curr.entry.Range.Size())
				donorLoad = curr.load
			} else {
				receiverPID = curr.entry.PartitionID
				donorPID = prev.entry.PartitionID
				movedSize = float64(prev.entry.Range.Size())
				donorLoad = prev.load
			}

			// Refuse cross-partition merges that would push the donor
			// below the per-partition floor. The donor loses one
			// entry on each cross-merge (its range is transferred to
			// the receiver), so without this guard a partition whose
			// ranges all happen to be cold-adjacent to neighbours can
			// be drained to zero entries over a small number of
			// rounds.
			if partitionEntries[donorPID]-1 < perPartitionFloor {
				result = append(result, curr)
				continue
			}

			if partitionLoads[receiverPID]+donorLoad <= maxPartitionLoad && churned+movedSize <= churnBudget {
				partitionLoads[receiverPID] += donorLoad
				partitionLoads[donorPID] -= donorLoad
				partitionEntries[donorPID]--

				merged := assignment.HashRange{Lo: prev.entry.Range.Lo, Hi: curr.entry.Range.Hi}
				actions = append(actions, Action{
					Kind:     ActionMerge,
					Range:    merged,
					FromPart: donorPID,
					ToPart:   receiverPID,
					Detail:   fmt.Sprintf("cross-partition merge P%d+P%d→P%d, combined load=%.4f", donorPID, receiverPID, receiverPID, mergedLoad),
				})
				prev.entry.Range = merged
				prev.entry.PartitionID = receiverPID
				prev.load = mergedLoad
				prev.series += curr.series
				churned += movedSize
				continue
			}
		}

		result = append(result, curr)
	}
	return result, actions
}
