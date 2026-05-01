# Warpstream Producer: Implementation Plan

> Based on `warpstream-producer-research.md`. Read that document first for full design rationale.

---

## Overview

Build a custom Warpstream-optimised Kafka producer for Grafana Mimir that implements dynamic
hedging on top of franz-go's existing connection infrastructure
(`kgo.Client.Broker(nodeID).RetriableRequest()`).

**New package:** `pkg/warpstreamclient/` — zero Mimir internal imports in production code. Test files may import `pkg/util/testkafka` for kfake-based integration tests.  
**Integration:** `pkg/storage/ingest/` — selects the backend via `-ingest-storage.kafka.backend`.

---

## Execution model for each step

Each step follows this sequence:

1. Implement the component.
2. Write tests. Use maps for table-driven tests and `t.Run()` for subtests (see patterns below).
3. Write benchmarks for every function on the hot path. Run with `-bench=. -benchmem` and fix
   any unexpected allocation or latency found before moving on.
4. Run `make format && go build ./... && go test -race ./...` scoped to the relevant package.
5. Run the `code-reviewer` agent on the new/changed files. Fix every issue found, then re-run
   from step 4 until the review is clean.

### Comment style (apply throughout)

- Function and struct doc comments: one sentence saying what it does at the caller's level of
  abstraction. Never expose implementation details (algorithms, data structures, field names).
- Inline comments on blocks of code: explain *why* a non-obvious decision was made, not *what*
  the code does. Omit comments when the code is self-explanatory.
- Bad: `// CRC-32C over bytes from attributes field to end of batch using Castagnoli polynomial`
- Good: `// The CRC range is defined by the Kafka protocol; it excludes the leading fixed fields.`
- Bad: `// Group records by record.Partition`
- Good: *(no comment; the code is self-explanatory)*

### Test patterns

**Table test with map (required for multi-case tests):**
```go
func TestFoo(t *testing.T) {
    tests := map[string]struct {
        input    string
        expected int
        wantErr  bool
    }{
        "happy path":  {input: "ok", expected: 1},
        "empty input": {input: "", wantErr: true},
    }
    for name, tc := range tests {
        t.Run(name, func(t *testing.T) {
            got, err := foo(tc.input)
            if tc.wantErr {
                require.Error(t, err)
                return
            }
            require.NoError(t, err)
            assert.Equal(t, tc.expected, got)
        })
    }
}
```

**Benchmark pattern (required for hot-path functions):**
```go
func BenchmarkFoo(b *testing.B) {
    input := makeInput()
    b.ResetTimer()
    b.ReportAllocs()
    for range b.N {
        _ = foo(input)
    }
}
```

---

## Step 1 — `produce.go`: ProduceRequest construction and response parsing

### Purpose

Pure functions with no state. Starting here establishes the wire format before any networking
code is written and gives an accurate benchmark of the serialisation cost before other layers
are added.

### Files

- `pkg/warpstreamclient/produce.go` *(new)*
- `pkg/warpstreamclient/produce_test.go` *(new)*

### What to implement

```go
// buildProduceRequest builds a ProduceRequest from a set of records belonging
// to the same topic. Records are grouped by partition.
// Uses magic=2, Snappy compression, and no idempotence fields.
func buildProduceRequest(topic string, version int16, records []*kgo.Record) *kmsg.ProduceRequest

// parseProduceResponse returns the first per-partition error in the response,
// or nil if all partitions succeeded.
func parseProduceResponse(resp *kmsg.ProduceResponse) error
```

**Implementation notes:**

`buildProduceRequest`:
- Group records by `record.Partition`; one `kmsg.ProduceRequestTopicPartition` per partition.
- `RecordBatch`: `magic=2`, `producerId=-1`, `producerEpoch=-1`, `baseSequence=-1`.
- Snappy-compress the encoded records payload using `github.com/klauspost/compress/s2` (already
  vendored). Set `attributes` bits 0-2 to `2` (CodecSnappy). Consider a `sync.Pool` of encode
  buffers to avoid per-call heap allocation — benchmark first to confirm the allocation exists.
- CRC: `crc32.New(crc32.MakeTable(crc32.Castagnoli))` over the correct byte range as specified
  by the Kafka protocol.
- Timestamps: `firstTimestamp` and `maxTimestamp` in milliseconds from `time.Now()`.

`parseProduceResponse`:
- Iterate `resp.Topics[].Partitions[]`; convert non-zero `ErrorCode` via `kerr.ErrorForCode`.
- Return the first non-nil error found.

### Test cases

```go
func TestBuildProduceRequest(t *testing.T) {
    tests := map[string]struct {
        topic              string
        records            []*kgo.Record
        wantPartitionCount int
        wantRecordCounts   []int // per partition, ascending partition order
    }{
        "single record, single partition": {
            topic: "t", records: makeRecords(0, "v1"),
            wantPartitionCount: 1, wantRecordCounts: []int{1},
        },
        "multiple records, same partition": {
            topic: "t", records: makeRecords(0, "v1", "v2", "v3"),
            wantPartitionCount: 1, wantRecordCounts: []int{3},
        },
        "records across two partitions": {
            topic: "t",
            records: append(makeRecords(0, "a"), makeRecords(1, "b", "c")...),
            wantPartitionCount: 2, wantRecordCounts: []int{1, 2},
        },
    }
    for name, tc := range tests {
        t.Run(name, func(t *testing.T) {
            req := buildProduceRequest(tc.topic, 9, tc.records)
            require.Len(t, req.Topics, 1)
            assert.Equal(t, tc.topic, req.Topics[0].Topic)
            // verify partition count and per-partition record counts ...
        })
    }
}

func TestParseProduceResponse(t *testing.T) {
    tests := map[string]struct {
        resp    *kmsg.ProduceResponse
        wantErr bool
    }{
        "all partitions success":         {resp: successResponse(), wantErr: false},
        "one partition error":            {resp: partialErrResponse(kerr.UnknownTopicOrPartition), wantErr: true},
        "multiple errors returns first":  {resp: multiErrResponse(), wantErr: true},
    }
    for name, tc := range tests {
        t.Run(name, func(t *testing.T) {
            err := parseProduceResponse(tc.resp)
            if tc.wantErr {
                require.Error(t, err)
            } else {
                require.NoError(t, err)
            }
        })
    }
}
```

### Benchmarks (hot path)

```go
func BenchmarkBuildProduceRequest(b *testing.B) {
    records := makeBenchRecords(100, 1000) // 100 records, ~1 KB each
    b.ResetTimer()
    b.ReportAllocs()
    for range b.N {
        _ = buildProduceRequest("mimir-ingest", 9, records)
    }
}

func BenchmarkParseProduceResponse(b *testing.B) {
    resp := largeSuccessResponse(100) // 100 partitions
    b.ResetTimer()
    b.ReportAllocs()
    for range b.N {
        _ = parseProduceResponse(resp)
    }
}
```

**If the benchmark shows per-call allocations in `buildProduceRequest`:** introduce a
`sync.Pool` for the encode scratch buffer and re-run to confirm zero allocations on the hot
path. Do not leave the pool in place speculatively if no allocation is found.

### Review focus

- CRC byte range is correct per the Kafka protocol spec.
- Snappy uses raw format (not framing format), matching what Kafka brokers expect.
- Varint encoding of per-record fields is correct.
- No heap allocations on the benchmark hot path after pooling is applied.

---

## Step 2 — `config.go` and `metrics.go`

### Purpose

`Config` holds all parameters `WarpstreamClient` needs, set entirely by the ingest package
— no CLI flag registration in this package. `metrics` exposes only metrics that franz-go's
built-in kprom hooks do not already cover.

### Files

- `pkg/warpstreamclient/config.go` *(new)*
- `pkg/warpstreamclient/config_test.go` *(new)*
- `pkg/warpstreamclient/metrics.go` *(new)*

### Config: no flags in this package

```go
// Config holds all parameters for WarpstreamClient.
// It is constructed by the caller (pkg/storage/ingest) from KafkaConfig.
// No field is registered as a CLI flag in this package.
type Config struct {
    // Connection
    Address      []string
    DialTimeout  time.Duration
    WriteTimeout time.Duration
    TLSEnabled   bool
    TLSConfig    *tls.Config // stdlib; no Mimir dependency
    ClientID     string
    Topic        string

    // SASL — mechanism name + pre-configured kgo option (constructed by ingest package)
    SASLOptions []kgo.Opt

    // Producer
    Linger        time.Duration
    MaxBatchBytes int32
    MaxBufferedBytes int64

    // Warpstream-specific
    HedgeMinSamples      int
    HedgeSlowMultiplier  float64
    HedgeMaxSlowFraction float64
    MaxAgents            int // 0 = all agents from Metadata
}

func (c *Config) Validate() error
```

**Warpstream-specific flags live in `pkg/storage/ingest/config.go`** under `KafkaConfig`:

```go
// In KafkaConfig (existing struct — add these fields):
WarpstreamHedgeMinSamples      int     `yaml:"warpstream_hedge_min_samples"`
WarpstreamHedgeSlowMultiplier  float64 `yaml:"warpstream_hedge_slow_multiplier"`
WarpstreamHedgeMaxSlowFraction float64 `yaml:"warpstream_hedge_max_slow_fraction"`
WarpstreamMaxAgents            int     `yaml:"warpstream_max_agents"`

// In RegisterFlagsWithPrefix — flag help text must mention warpstream backend:
f.IntVar(&cfg.WarpstreamHedgeMinSamples, prefix+"warpstream-hedge-min-samples", 10,
    "Minimum number of produce samples per agent before dynamic hedging activates. "+
    "Only applies when -"+prefix+"backend=warpstream.")
// ... similarly for the other fields
```

**Mapping function** (in `pkg/storage/ingest/writer_client.go`):

```go
func warpstreamClientConfig(cfg KafkaConfig) warpstreamclient.Config {
    return warpstreamclient.Config{
        Address:              cfg.Address.ToSlice(),
        DialTimeout:          cfg.DialTimeout,
        WriteTimeout:         cfg.WriteTimeout,
        TLSEnabled:           cfg.TLSEnabled,
        TLSConfig:            buildTLSConfig(cfg),  // existing helper
        ClientID:             cfg.ClientID,
        Topic:                cfg.Topic,
        SASLOptions:          buildSASLOptions(cfg), // existing helper; returns []kgo.Opt
        Linger:               lingerDuration(cfg),   // respects DisableLinger flag
        MaxBatchBytes:        producerBatchMaxBytes,  // existing constant
        MaxBufferedBytes:     cfg.ProducerMaxBufferedBytes,
        HedgeMinSamples:      cfg.WarpstreamHedgeMinSamples,
        HedgeSlowMultiplier:  cfg.WarpstreamHedgeSlowMultiplier,
        HedgeMaxSlowFraction: cfg.WarpstreamHedgeMaxSlowFraction,
        MaxAgents:            cfg.WarpstreamMaxAgents,
    }
}
```

### Metrics: only what franz-go doesn't already provide

Franz-go automatically emits the following via `kprom.NewMetrics` and
`KafkaClientExtendedMetrics` hooks (already wired in `commonKafkaClientOptions`):
- Produce record and byte counts (compressed + uncompressed) via `kprom.FetchAndProduceDetail`
- Per-broker E2E write latency via `kgo.HookBrokerE2E`
- Throttle metrics via `kgo.HookBrokerThrottle`

The warpstream client reuses the same hook pattern. **Do not duplicate** the above. Add only:

```go
type metrics struct {
    // Hedging
    hedgeAttemptsTotal prometheus.Counter  // secondary goroutine was started
    hedgeWinsTotal     prometheus.Counter  // secondary response arrived first

    // Custom linger buffer (not covered by franz-go; our buffer is outside kgo's produce path)
    lingerBufferBytes prometheus.Gauge    // bytes currently accumulated in the buffer
    lingerFlushTotal  prometheus.Counter  // number of partition batch flushes

    // Per-agent request outcomes (franz-go tracks connection-level; we track app-level)
    agentRequestsTotal *prometheus.CounterVec // labels: agent, outcome (success|error)
    agentLatency       *prometheus.HistogramVec // label: agent

    // Failure categorisation (distinct from franz-go's connection-level errors)
    produceFailuresTotal *prometheus.CounterVec // label: reason
}

func newMetrics(reg prometheus.Registerer) *metrics
```

All metrics are registered under the same prefix as the writer client metrics so dashboards
need no changes for the shared metrics (see `writerMetricsPrefix` in `writer.go`).

### Test cases

```go
func TestConfigValidate(t *testing.T) {
    base := validConfig()
    tests := map[string]struct {
        mutate  func(*Config)
        wantErr bool
    }{
        "valid config":                    {mutate: func(_ *Config) {}},
        "empty topic":                    {mutate: func(c *Config) { c.Topic = "" }, wantErr: true},
        "zero write timeout":             {mutate: func(c *Config) { c.WriteTimeout = 0 }, wantErr: true},
        "zero max batch bytes":           {mutate: func(c *Config) { c.MaxBatchBytes = 0 }, wantErr: true},
        "negative hedge slow multiplier": {mutate: func(c *Config) { c.HedgeSlowMultiplier = -1 }, wantErr: true},
        "slow fraction > 1":              {mutate: func(c *Config) { c.HedgeMaxSlowFraction = 1.5 }, wantErr: true},
        "zero linger is valid":           {mutate: func(c *Config) { c.Linger = 0 }},
        "zero max agents means all":      {mutate: func(c *Config) { c.MaxAgents = 0 }},
    }
    for name, tc := range tests {
        t.Run(name, func(t *testing.T) {
            cfg := base
            tc.mutate(&cfg)
            err := cfg.Validate()
            if tc.wantErr {
                require.Error(t, err)
            } else {
                require.NoError(t, err)
            }
        })
    }
}
```

### Review focus

- `Config` has no `flag.FlagSet` dependency anywhere in the file.
- All metrics use `promauto.With(reg)`.
- Metrics that franz-go already provides are absent from the struct.

---

## Step 3 — `direct_producer.go`: DirectProducer interface and KafkaDirectProducer

### Purpose

Define the single seam between custom logic and the franz-go network stack. The more important
output of this step is `mockDirectProducer`, which all subsequent components use in their tests.

### Files

- `pkg/warpstreamclient/direct_producer.go` *(new)*
- `pkg/warpstreamclient/direct_producer_test.go` *(new — includes `mockDirectProducer`)*

### What to implement

```go
// DirectProducer produces to a specific Warpstream agent by its Kafka NodeID.
// Implementations must be safe for concurrent use.
type DirectProducer interface {
    Produce(ctx context.Context, nodeID int32, req *kmsg.ProduceRequest) (*kmsg.ProduceResponse, error)
}

// KafkaDirectProducer implements DirectProducer using kgo.Client.Broker().RetriableRequest().
// This is the only type in this package that depends on kgo.Client directly.
type KafkaDirectProducer struct {
    client *kgo.Client
}

func NewKafkaDirectProducer(client *kgo.Client) *KafkaDirectProducer
func (s *KafkaDirectProducer) Produce(ctx context.Context, nodeID int32, req *kmsg.ProduceRequest) (*kmsg.ProduceResponse, error)
```

`mockDirectProducer` (test-only):
```go
type mockDirectProducerCall struct {
    nodeID int32
    req    *kmsg.ProduceRequest
    err    error     // nil on success; ctx.Err() if cancelled
    sentAt time.Time // for ordering assertions
}

type mockDirectProducer struct {
    mu      sync.Mutex
    calls   []mockDirectProducerCall
    delays  map[int32]time.Duration
    errs    map[int32]error
    blockCh map[int32]chan struct{}  // per-nodeID gate for deterministic ordering
}

func newMockDirectProducer() *mockDirectProducer
func (m *mockDirectProducer) Produce(ctx, nodeID, req) (*kmsg.ProduceResponse, error)
func (m *mockDirectProducer) recordedCalls() []mockDirectProducerCall
```

### Tests

- `TestKafkaDirectProducerImplementsDirectProducer` — compile-time interface check.
- `TestMockDirectProducer` — table-driven coverage of delay, error, context cancellation, recording.
- `TestMockDirectProducerMultipleCalls` — multi-call ordering, concurrent races under `-race`,
  block/release determinism.
- `TestKafkaDirectProducerProduce` — kfake-based round-trip: produce a record and consume it back.

### Review focus

- `mockDirectProducer` is race-safe under `-race`.
- Context cancellation is respected inside the mock delay and during block-release waits.

---

## Step 4 — `agentpool.go` + `partition_assignment.go`: agent discovery and selection

### Purpose

Two files with a single responsibility each:

- **`agentpool.go`** — `AgentPool`: discovers agents via Kafka Metadata and produces a new
  `DefaultPartitionAssignmentStrategy` snapshot on every `Refresh()`.
- **`partition_assignment.go`** — `PartitionAssignmentStrategy` interface +
  `DefaultPartitionAssignmentStrategy`: immutable selection logic on a fixed agent snapshot.

Decoupling the two responsibilities means `DefaultPartitionAssignmentStrategy` has no locks on
the hot path (uses `sync.Map` for write-once-read-many secondary caching) and can be tested
without any network or Metadata involvement.

### Files

- `pkg/warpstreamclient/agentpool.go` *(new)*
- `pkg/warpstreamclient/partition_assignment.go` *(new)*
- `pkg/warpstreamclient/agentpool_test.go` *(new)*

### What to implement

```go
// partition_assignment.go

// PartitionAssignmentStrategy selects the primary and secondary agent for a given partition.
type PartitionAssignmentStrategy interface {
    Primary(topic string, partition int32) int32
    Secondary(topic string, partition int32) (nodeID int32, ok bool)
}

// DefaultPartitionAssignmentStrategy implements PartitionAssignmentStrategy from an
// immutable snapshot of the agent pool. Safe for concurrent use without explicit locking;
// secondary results are cached in a sync.Map (write-once, read-many).
// A new instance is created by AgentPool.Refresh() on every call.
type DefaultPartitionAssignmentStrategy struct {
    agents    []int32                  // sorted ascending, immutable
    leaders   map[topicPartition]int32 // immutable
    secondary sync.Map                 // topicPartition → int32 (noSecondary=-1 if unavailable)
}

func newDefaultPartitionAssignmentStrategy(agents []int32, leaders map[topicPartition]int32) *DefaultPartitionAssignmentStrategy
func (s *DefaultPartitionAssignmentStrategy) Primary(topic string, partition int32) int32
func (s *DefaultPartitionAssignmentStrategy) Secondary(topic string, partition int32) (int32, bool)

// selectSecondary picks a secondary NodeID deterministically using xxhash(topic:partition)
// over the sorted candidate list (all agents minus primary). Alloc-free.
// Returns (0, false) if no secondary is available.
func selectSecondary(topic string, partition int32, primary int32, all []int32) (int32, bool)
```

```go
// agentpool.go

// AgentPool discovers and maintains the current set of Warpstream agents.
// Each Refresh produces a new DefaultPartitionAssignmentStrategy from the updated snapshot.
type AgentPool struct {
    client   *kgo.Client
    topic    string
    mu       sync.RWMutex
    topicID  [16]byte
    agents   []int32 // sorted ascending; used to detect removed agents
    strategy atomic.Pointer[DefaultPartitionAssignmentStrategy]
}

func NewAgentPool(client *kgo.Client) *AgentPool

// Refresh updates the agent pool via RequestCachedMetadata (avoids duplicate
// network requests when kgo's cache is fresh). Returns removed NodeIDs.
// Always creates a new DefaultPartitionAssignmentStrategy from the current snapshot.
func (p *AgentPool) Refresh(ctx context.Context) (removed []int32, err error)

// Strategy returns the PartitionAssignmentStrategy built from the last Refresh.
func (p *AgentPool) Strategy() PartitionAssignmentStrategy

// TopicID returns the UUID of the topic (required for Produce API v13+).
func (p *AgentPool) TopicID(topic string) [16]byte
```

**Secondary caching (now in `DefaultPartitionAssignmentStrategy`):**
`sync.Map` is used instead of `map+RWMutex` because each strategy is immutable: once a
secondary is computed for a `(topic, partition)` key it never changes for that instance.
`sync.Map` is optimal for this write-once, read-many access pattern and eliminates the
double-check locking complexity that was previously in `AgentPool.Secondary()`.

**Caching is required at production scale.**
Warpstream clusters range from tens to thousands of agents (up to ~1 000 in the largest
deployments). `selectSecondary` at 1 000 agents costs ~300 ns. Without caching this runs on
every produce flush for every partition. The cache amortises this to one call per
`(topic, partition)` per strategy instance (~10 s lifetime).

**`mockPartitionAssignmentStrategy`** (test-only, in `agentpool_test.go`):
```go
type mockPartitionAssignmentStrategy struct {
    agents    []int32
    leaders   map[topicPartition]int32
    secondary map[topicPartition]int32 // optional override; falls back to selectSecondary
}
func (m *mockPartitionAssignmentStrategy) Primary(topic string, partition int32) int32
func (m *mockPartitionAssignmentStrategy) Secondary(topic string, partition int32) (int32, bool)
```

### Test cases

```go
func TestSelectSecondary(t *testing.T) {
    tests := map[string]struct {
        topic     string
        partition int32
        primary   int32
        all       []int32
        wantOk    bool
        wantNotEq int32 // secondary must differ from this value when ok=true
    }{
        "single agent: no secondary available": {
            topic: "t", partition: 0, primary: 1, all: []int32{1},
            wantOk: false,
        },
        "two agents: returns the non-primary": {
            topic: "t", partition: 0, primary: 1, all: []int32{1, 2},
            wantOk: true, wantNotEq: 1,
        },
        "two agents: primary=2 also returns the non-primary": {
            topic: "t", partition: 0, primary: 2, all: []int32{1, 2},
            wantOk: true, wantNotEq: 2,
        },
        "three agents: result is deterministic across calls": {
            topic: "t", partition: 0, primary: 1, all: []int32{1, 2, 3},
            wantOk: true, wantNotEq: 1,
        },
    }
    for name, tc := range tests {
        t.Run(name, func(t *testing.T) {
            nodeID, ok := selectSecondary(tc.topic, tc.partition, tc.primary, tc.all)
            assert.Equal(t, tc.wantOk, ok)
            if ok {
                assert.NotEqual(t, tc.wantNotEq, nodeID)
                // Verify determinism.
                nodeID2, _ := selectSecondary(tc.topic, tc.partition, tc.primary, tc.all)
                assert.Equal(t, nodeID, nodeID2)
            }
        })
    }
}

func TestSelectSecondaryNeverReturnsPrimary(t *testing.T) {
    // Property: with ≥2 agents, secondary ≠ primary for all (topic, partition) pairs.
    agents := []int32{1, 2, 3, 4, 5}
    for _, primary := range agents {
        for part := int32(0); part < 50; part++ {
            t.Run(fmt.Sprintf("primary=%d partition=%d", primary, part), func(t *testing.T) {
                nodeID, ok := selectSecondary("t", part, primary, agents)
                require.True(t, ok)
                assert.NotEqual(t, primary, nodeID)
            })
        }
    }
}

func TestAgentPoolSecondaryCaching(t *testing.T) {
    t.Run("returns same value on repeated calls", func(t *testing.T) { ... })
    t.Run("cache is invalidated when agent set changes after Refresh", func(t *testing.T) { ... })
    t.Run("cache is not invalidated when agent set is unchanged after Refresh", func(t *testing.T) { ... })
}
```

### Benchmarks

```go
func BenchmarkSelectSecondary(b *testing.B) {
    // Benchmark at realistic production scales.
    for _, n := range []int{10, 100, 1000} {
        all := makeNodeIDs(n)
        b.Run(fmt.Sprintf("agents=%d", n), func(b *testing.B) {
            b.ReportAllocs()
            for range b.N {
                _, _ = selectSecondary("mimir-ingest", 7, all[0], all)
            }
        })
    }
}

// Verifies the cache eliminates selectSecondary calls on the hot path.
func BenchmarkAgentPoolSecondary_Cached(b *testing.B) {
    pool := newTestPool(8) // 8 agents
    pool.Refresh(context.Background())
    _, _ = pool.Secondary("mimir-ingest", 0) // warm the cache
    b.ResetTimer()
    b.ReportAllocs()
    for range b.N {
        _, _ = pool.Secondary("mimir-ingest", 0)
    }
}
```

### Review focus

- `Secondary()` double-check locking pattern is correct (read→write upgrade).
- Cache cleared on agent set change, not on any Refresh.
- `Refresh()` does not hold the write lock while waiting for the network response.

---

## Step 5 — Stats tracker, tracking producer, and hedger

### Purpose

Implement the latency- and error-aware speculative hedging layer. The work is split across
four small, composable units rather than the originally-planned single `hedger.go`:

- **`AgentStatsTracker`** — a sliding-window per-agent / cluster-wide stats tracker. Records
  the outcome of every Produce request (latency + error) and answers the questions
  "is this agent unhealthy?" and "is the cluster as a whole unhealthy?".
- **`CachedAgentStatsTracker`** — wraps an `AgentStatsTracker` and caches `ClusterStats` for a
  short TTL. The cluster gather is O(agents) on every call; the cache reduces that to one gather
  per TTL window. `AgentStats` and `TrackAgentRequest` forward live.
- **`TrackingProducer`** — a `DirectProducer` decorator that records every call's outcome into
  the tracker. By living between the `Hedger` and the network leaf, every leg the hedger
  dispatches (primary, fanout sub-leg, abandoned race loser) is observed exactly once, without
  the hedger needing to know about the tracker for write purposes.
- **`Hedger`** — the orchestrator. Implements `DirectProducer`, decides per-call whether to
  speculatively hedge, splits failed/slow requests across per-partition secondaries, and races
  the legs.

Hedging is suppressed in three cases: when the primary's stats are not yet representative
(bucket-spread quorum), when the cluster as a whole has a widespread slow- or fault-rate issue
(amplifying load would make it worse), or when the primary is healthy on both axes.

### Files

- `pkg/warpstreamclient/agent_stats_tracker.go` *(new)*
- `pkg/warpstreamclient/agent_stats_tracker_test.go` *(new)*
- `pkg/warpstreamclient/cached_agent_stats_tracker.go` *(new)*
- `pkg/warpstreamclient/cached_agent_stats_tracker_test.go` *(new)*
- `pkg/warpstreamclient/tracking_producer.go` *(new)*
- `pkg/warpstreamclient/tracking_producer_test.go` *(new)*
- `pkg/warpstreamclient/produce_split_merge.go` *(new — split/merge helpers consumed by the Hedger)*
- `pkg/warpstreamclient/produce_split_merge_test.go` *(new)*
- `pkg/warpstreamclient/hedger.go` *(new)*
- `pkg/warpstreamclient/hedger_test.go` *(new)*

### `AgentStatsTracker` interface

```go
// AgentStatsTracker is implemented by components that track per-agent and
// cluster-wide request stats (latency and error rate).
type AgentStatsTracker interface {
    // TrackAgentRequest records the outcome of one request for nodeID. Errored
    // requests contribute only to the error rate; the latency stat is computed
    // from successful requests only. context.Canceled is ignored (caller
    // intent, not agent health). context.DeadlineExceeded is treated as a
    // genuine error (agent or network was too slow).
    TrackAgentRequest(now time.Time, nodeID int32, latency time.Duration, err error)

    // PurgeAgents removes all tracked state for the given NodeIDs. Called by
    // AgentPool.Refresh when agents leave the cluster.
    PurgeAgents(nodeIDs []int32)

    // AgentStats returns the agent's stats over the current observation
    // window. Returns false when the agent has not yet collected requests
    // spread across enough time buckets to be representative.
    AgentStats(now time.Time, nodeID int32) (AgentStats, bool)

    // ClusterStats returns the cluster-wide stats view. slowMultiplier scales
    // the baseline latency into the slow threshold; faultyThreshold is the
    // absolute error-rate threshold above which an agent is considered
    // faulty. Returns false when fewer than a quorum of qualifying agents
    // have reported data.
    ClusterStats(now time.Time, slowMultiplier, faultyThreshold float64) (ClusterStats, bool)
}

// AgentStats summarises one agent's observations over the current window.
type AgentStats struct {
    Latency      time.Duration // average over successful requests
    ErrorRate    float64       // raw faulty/total ratio
    RequestCount int64         // total requests in window — caller can gate small-sample noise
}

// ClusterStats summarises cluster-wide stats. SlowContributorsCount and
// FaultyContributorsCount are exposed so callers can scale fraction-based
// decisions to cluster size.
type ClusterStats struct {
    BaselineLatency         time.Duration
    SlowThreshold           time.Duration
    SlowFraction            float64
    SlowContributorsCount   int64
    BaselineErrorRate       float64
    FaultyThreshold         float64
    FaultyFraction          float64
    FaultyContributorsCount int64
}
```

### `AverageAgentStatsTracker` (default implementation)

A bucketed sliding window: 6 × 10s buckets per agent (60s total observation), wall-clock-aligned
ring rotation. Each bucket stores `successfulLatencySumMs`, `successfulLatencyCount`, and
`faultyCount`. The two reliability gates owned by the tracker are:

- **Bucket-spread quorum** (`minFilledBuckets = 3`): an agent's stats are only released once
  its requests cover at least 3 distinct 10s buckets. Prevents a single burst from being
  treated as "representative".
- **Cluster quorum**: `ClusterStats` returns `(_, false)` if fewer than half of all agents
  with data pass the bucket-spread quorum.

For the cluster *error-rate* signal a per-call request-count floor is derived from
`faultyThreshold` (`errorRateMinRequests = ceil(1/threshold)`): low-volume agents are excluded
from both numerator and denominator of `FaultyFraction` so their 1/N quantisation noise can't
pretend to be a fleet-wide event. `AgentStats` itself reports raw `ErrorRate` plus
`RequestCount`; callers gate noise themselves.

### `CachedAgentStatsTracker`

Caches `ClusterStats` keyed by `(slowMultiplier, faultyThreshold)` for a configurable TTL.
`AgentStats` and `TrackAgentRequest` forward live. `PurgeAgents` invalidates the cache so
purged agents don't linger for up to a TTL. A "no-quorum" sentinel is also cached so the
gather isn't re-run on every miss when the cluster is below quorum.

### `TrackingProducer`

```go
// TrackingProducer is a DirectProducer decorator that records the outcome of
// every Produce call into an AgentStatsTracker. Sits between the Hedger and
// the network-leaf DirectProducer so every leg the hedger dispatches —
// primary, hedge, fanout sub-leg, abandoned race loser — is observed exactly
// once.
type TrackingProducer struct {
    inner   DirectProducer
    tracker AgentStatsTracker
}

func (p *TrackingProducer) Produce(ctx context.Context, nodeID int32, req *kmsg.ProduceRequest) (*kmsg.ProduceResponse, error) {
    start := time.Now()
    resp, err := p.inner.Produce(ctx, nodeID, req)
    p.tracker.TrackAgentRequest(time.Now(), nodeID, time.Since(start), err)
    return resp, err
}
```

The Hedger does **not** call `TrackAgentRequest` itself — it only consumes the read side
(`AgentStats` / `ClusterStats`) for the hedge decision. Production wiring is
`Hedger(TrackingProducer(KafkaDirectProducer))`.

### `produce_split_merge.go`

Two pure functions consumed by the Hedger's fanout path:

```go
// splitProduceRequestToSecondaryAgents groups the partitions of req by their
// per-partition secondary. All-or-nothing: if any partition lacks a designated
// secondary the function returns an error and no map (the caller surfaces the
// upstream primary error instead).
func splitProduceRequestToSecondaryAgents(req *kmsg.ProduceRequest, strategy PartitionAssignmentStrategy) (map[int32]*kmsg.ProduceRequest, error)

// mergeProduceResponses concatenates per-partition entries from disjoint
// sub-responses into a single response. Version is taken from the first
// non-nil sub-response; ThrottleMillis is the max across sub-responses (the
// agent that asked the client to back off the most wins).
func mergeProduceResponses(resps []*kmsg.ProduceResponse) *kmsg.ProduceResponse
```

### `Hedger`

```go
type HedgerConfig struct {
    SlowMultiplier    float64       // primary is "slow" when latency > baseline * SlowMultiplier
    MaxSlowFraction   float64       // suppress hedging when more than this fraction of agents are slow
    FaultyThreshold   float64       // primary is "faulty" when ErrorRate exceeds this
    MaxFaultyFraction float64       // suppress hedging when more than this fraction of agents are faulty
    MinHedgeDelay     time.Duration // floor on the dynamically-computed hedge delay (otherwise = baseline)
}

// Hedger implements DirectProducer; composes with other DirectProducer
// decorators. Per-leg observations must be recorded by a tracking decorator
// inside `inner` (Hedger does not write to the tracker directly).
type Hedger struct {
    inner    DirectProducer
    tracker  AgentStatsTracker
    strategy PartitionAssignmentStrategy
    cfg      HedgerConfig
    metrics  *metrics
}

func NewHedger(inner DirectProducer, tracker AgentStatsTracker, strategy PartitionAssignmentStrategy, cfg HedgerConfig, m *metrics) *Hedger

// Implements DirectProducer.
func (h *Hedger) Produce(ctx context.Context, primaryID int32, req *kmsg.ProduceRequest) (*kmsg.ProduceResponse, error)
```

**Triggers for the secondary leg:**

| Primary behaviour              | Hedge decision | Outcome |
|--------------------------------|----------------|---------|
| Returns success before timer   | hedge          | Return primary; fanout never starts. |
| Returns success                | no-hedge       | Return primary. |
| Times out at hedge delay       | hedge          | Race primary vs. fanout. First success wins. |
| Returns error                  | hedge or no    | Fanout retry (unconditional). |

Hedging is therefore both an *anticipation* (timer-driven race) and an *unconditional retry on
primary failure*. The fanout is all-or-nothing: any sub-request error fails the whole fanout.

**`shouldHedge` decision** (read-only against tracker):
1. `AgentStats(now, primaryID)` — false → no hedge.
2. `ClusterStats(now, SlowMultiplier, FaultyThreshold)` — false → no hedge.
3. `primarySlow = primary.Latency > cluster.SlowThreshold`.
4. `primaryFaulty = RequestCount >= ceil(1/FaultyThreshold) && ErrorRate > cluster.FaultyThreshold`.
5. If `!primarySlow && !primaryFaulty` → no hedge.
6. **Scale-aware fraction floor**: each fraction gate is compared against
   `max(configured, 1/contributors)` so a single bad agent never trips suppression regardless
   of cluster size. At large N the configured fraction dominates; at small N the floor opens
   the gate. Implemented in `maxFractionFloor(configured, contributors)`.
7. `SlowFraction > maxFractionFloor(MaxSlowFraction, SlowContributorsCount)` → no hedge.
8. `FaultyFraction > maxFractionFloor(MaxFaultyFraction, FaultyContributorsCount)` → no hedge.
9. `delay = max(BaselineLatency, MinHedgeDelay)`; return `(delay, true)`.

**`fanoutToSecondaryAgents`** (used for both timer-fired races and primary-failure retries):
1. `splitProduceRequestToSecondaryAgents` — on error, wait for primary and return its
   outcome alone.
2. Increment `hedgeAttemptsTotal`. Spawn one goroutine per secondary ID, all writing into a
   single buffered `secondaryCh`.
3. Loop selecting on `primaryCh` and `secondaryCh` until both have errored:
   - Any leg success → return that response (increment `hedgeWinsTotal` on a fanout success).
   - First sub-leg error fails the fanout (all-or-nothing).
4. If both legs fail, return only the primary's error (the fanout error is discarded —
   primary is the source of truth; the secondary's error is observable via
   `TrackingProducer` per-agent metrics).

**Internal type:**

```go
// directProduceResult carries the outcome of a single inner.Produce call
// through the channels the orchestration uses internally. Used for both
// "primary leg" and "fanout sub-leg" outcomes.
type directProduceResult struct {
    resp *kmsg.ProduceResponse
    err  error
}
```

The "primary already failed" path uses a small helper `bufferedPrimary(primary)` that
preloads a 1-buffered channel so `fanoutToSecondaryAgents` can take a uniform `primaryCh`
parameter regardless of whether the primary is still in flight or already errored.

### Test cases

`AverageAgentStatsTracker`:
- `TrackAgentRequest` — successful and faulty requests update the right buckets;
  `context.Canceled` is ignored; `context.DeadlineExceeded` counts as faulty; concurrent
  rotation across bucket boundaries is race-safe.
- `AgentStats` — returns false before bucket-spread quorum; reports raw `ErrorRate` plus
  `RequestCount`; computes per-agent average latency over the window.
- `ClusterStats` — bucket-spread quorum; cluster quorum (≥ ½ observed agents must qualify);
  `BaselineLatency` is per-agent mean; `BaselineErrorRate` is request-weighted; low-volume
  agents excluded from `FaultyFraction` numerator and denominator.
- `PurgeAgents` — removes per-agent entries; subsequent `AgentStats` returns false.

`CachedAgentStatsTracker`:
- `AgentStats` is not cached (forwards live).
- `ClusterStats` caches by `(slowMultiplier, faultyThreshold)` within TTL; refreshes after TTL.
- No-quorum sentinel: keeps returning `(_, false)` until TTL elapses, even after the inner
  tracker would now satisfy quorum.
- `PurgeAgents` invalidates the cache.

`TrackingProducer`:
- Records latency and nil error on success; records error on failure; forwards inner
  response unchanged.

`splitProduceRequestToSecondaryAgents`:
- Groups partitions by per-partition secondary; returns one sub-request per unique secondary.
- A partition without a designated secondary fails the whole split with an error.
- An empty input (no topics / no partitions) fails the split — guards against a deadlock in
  the fanout select-loop where no sub-leg goroutines would be spawned.
- Preserves top-level fields (`Version`, `Acks`, `TimeoutMillis`, `TopicID`).
- Multi-topic split correctly groups across topics.

`mergeProduceResponses`:
- Concatenates per-topic partitions across sub-responses.
- Spans multiple topics in the merged result.
- `ThrottleMillis` is the max across sub-responses.
- Nil sub-responses are skipped.
- Empty input returns an empty response (Version=0).

`Hedger.Produce`:
- No hedge decision (healthy cluster): only primary is called.
- Primary slow + healthy secondaries: hedge fires, secondary wins the race.
- Primary fails before hedge timer: per-partition fanout retry succeeds.
- Hedge timer fires and secondary wins: `hedgeAttemptsTotal` and `hedgeWinsTotal` both
  incremented.
- Hedge decided but primary wins before timer: no `hedgeAttemptsTotal` increment.
- Multi-partition primary error: fanout splits across per-partition secondaries; merged
  response contains all partitions.
- Single-agent cluster (no secondaries available): primary error propagates unchanged.
- Both legs fail: returns only the primary error; the fanout error is discarded.

`maxFractionFloor`:
- Single contributor → floor at 1.0; configured value is irrelevant.
- Two contributors → floor at 0.5 (a single bad agent never trips suppression).
- Large cluster → configured fraction dominates (1/N ≪ configured).
- Configured higher than 1/N → configured wins.
- Zero contributors → returns configured (no signal to scale against).

### Benchmarks

```go
func BenchmarkAverageAgentStatsTracker_TrackAgentRequest(b *testing.B) { ... }
func BenchmarkCachedAgentStatsTracker_ClusterStats(b *testing.B) {
    // Scenarios: agents=10, 100, 1000.
    // Cache warm — measures the steady-state hit path.
}
```

### Review focus

- Bucket-spread quorum and cluster quorum gates produce stable behaviour as agents come and
  go (`PurgeAgents` invalidates promptly; ring rotation is wall-clock-aligned).
- `Hedger` does not call `TrackAgentRequest` itself — the `TrackingProducer` decorator is the
  single source of per-leg observations, including legs whose results the Hedger races and
  discards.
- The fanout select loop exits cleanly: secondary sub-leg goroutines write to a buffered
  channel sized to the number of sub-legs, so abandoned sub-legs don't block (and their
  observations are still recorded by `TrackingProducer`).
- Scale-aware fraction floor (`maxFractionFloor`) prevents pathological 2-agent / 1-faulty
  suppression while preserving production semantics in large clusters.
- `hedgeAttemptsTotal` / `hedgeWinsTotal` semantics cover both hedge-timer races and
  primary-failure retries (documented in `metrics.go`).

---

## Step 6 — `linger.go`: per-partition record buffering

### Purpose

The most complex component. Accumulates records per `(topic, partition)` and flushes on timer
expiry, batch-full, or `Close()`. The linger is always honoured — `ProduceSync` does not bypass
it. This maximises batching at the cost of a bounded latency floor.

### Files

- `pkg/warpstreamclient/linger.go` *(new)*
- `pkg/warpstreamclient/linger_test.go` *(new)*

### What to implement

```go
// FlushFunc is called with a ready batch. It must call done(err) exactly once.
type FlushFunc func(ctx context.Context, nodeID int32, records []*kgo.Record, done func(error))

// RecordBuffer accumulates records and flushes them in batches.
// The linger period is always honoured; there is no immediate-flush path.
type RecordBuffer struct {
    linger        time.Duration
    maxBatchBytes int32
    selector      PartitionAssignmentStrategy
    flush         FlushFunc
    metrics       *metrics

    // Each partition has its own lock to reduce contention on the hot path.
    mu   sync.Mutex             // protects the bufs map itself
    bufs map[topicPartition]*partitionBuf
}

type partitionBuf struct {
    mu        sync.Mutex    // protects records, wireBytes, callbacks, timer
    records   []*kgo.Record
    wireBytes int32
    callbacks []func(error) // one per Add call that contributed to this batch
    timer     *time.Timer
}

func NewRecordBuffer(linger time.Duration, maxBatchBytes int32, selector PartitionAssignmentStrategy, flush FlushFunc, m *metrics) *RecordBuffer

// Add buffers records into the appropriate per-partition batch.
// done is called exactly once when the batch containing these records is
// acknowledged (or fails). The linger timer is always honoured; callers block
// until the batch is flushed and the broker responds.
func (b *RecordBuffer) Add(ctx context.Context, records []*kgo.Record, done func(error))

// Close flushes all pending batches and waits until every done callback has fired.
func (b *RecordBuffer) Close()
```

**Flush triggers:**
1. Linger timer fires for a partition.
2. Adding records would cause the partition batch to exceed `maxBatchBytes`: flush the
   current batch first, then start a new batch for the records that did not fit.
3. `Close()`: flush all partitions and drain in-flight callbacks.

**Note on context propagation:**  
`RecordBuffer.Add` accepts a context, but once records are buffered they are committed to
the batch and will be sent when the timer fires regardless of whether the original context
is later cancelled. This is intentional: cancelling a context after buffering should not
silently discard records that have already been accepted. If the write deadline is the
concern, set `WriteTimeout` on the Config instead.

**Performance: per-partition locking**  
Each `partitionBuf` has its own mutex. The global `RecordBuffer.mu` is held only for the
`bufs` map lookup and insertion of new partitions (rare after startup). This means concurrent
adds to different partitions do not contend.

### Test cases

```go
func TestRecordBufferFlushTriggers(t *testing.T) {
    tests := map[string]struct {
        linger         time.Duration
        maxBatchBytes  int32
        addGroups      []addGroup // each group is one Add call with its records
        waitAfterAdd   time.Duration
        wantFlushCount int
    }{
        "linger timer triggers flush": {
            linger: 20 * time.Millisecond, maxBatchBytes: 1<<20,
            addGroups: []addGroup{{partition: 0, values: []string{"a"}}},
            waitAfterAdd: 50 * time.Millisecond,
            wantFlushCount: 1,
        },
        "batch full triggers immediate flush before linger": {
            linger: time.Hour, maxBatchBytes: 100,
            addGroups: []addGroup{{partition: 0, values: manyValues(200)}},
            wantFlushCount: 2, // at least 2 flushes for the overflow
        },
        "close flushes all pending": {
            linger: time.Hour, maxBatchBytes: 1<<20,
            addGroups: []addGroup{{partition: 0, values: []string{"x"}}},
            wantFlushCount: 1,
        },
        "two partitions flush independently": {
            linger: 20 * time.Millisecond, maxBatchBytes: 1<<20,
            addGroups: []addGroup{
                {partition: 0, values: []string{"a"}},
                {partition: 1, values: []string{"b"}},
            },
            waitAfterAdd: 50 * time.Millisecond,
            wantFlushCount: 2,
        },
    }
    for name, tc := range tests {
        t.Run(name, func(t *testing.T) { ... })
    }
}

func TestRecordBufferDoneCallbacks(t *testing.T) {
    t.Run("done called exactly once per Add after flush ack", func(t *testing.T) { ... })
    t.Run("done called with error when FlushFunc signals failure", func(t *testing.T) { ... })
    t.Run("concurrent adds to same partition coalesce into one flush", func(t *testing.T) { ... })
    t.Run("done called for all callbacks after Close", func(t *testing.T) { ... })
}
```

### Benchmarks

```go
func BenchmarkRecordBufferAdd(b *testing.B) {
    buf := newTestRecordBuffer(50*time.Millisecond, 16<<20)
    records := makeRecords(0, "value")
    b.ResetTimer()
    b.ReportAllocs()
    for range b.N {
        done := make(chan struct{})
        buf.Add(context.Background(), records, func(error) { close(done) })
        // Don't wait for done in the benchmark; we measure Add throughput only.
    }
}
```

**If Add shows allocations per call:** profile to identify whether they come from the callback
closure, the slice append, or the map lookup; address accordingly.

### Review focus

- `partitionBuf.mu` is not held while calling `flush` (prevents deadlock if `flush` callbacks
  back into the buffer).
- `Close()` waits for all in-flight `FlushFunc` goroutines to complete before returning.
- Timer is stopped before the `partitionBuf` is discarded after `Close`.
- The per-partition mutex eliminates global contention for concurrent adds to different partitions.
- `wireBytes` tracks the serialised size accurately enough to prevent `maxBatchBytes` violations.

---

## Step 7 — `client.go`: WarpstreamClient

### Purpose

Wire all components into a single type and implement `ProduceSync`. `WarpstreamClient`
directly satisfies the `kafkaProducer` interface defined in `pkg/storage/ingest` — no adapter
struct is required.

### Files

- `pkg/warpstreamclient/client.go` *(new)*
- `pkg/warpstreamclient/client_test.go` *(new)*

### What to implement

```go
// WarpstreamClient is a produce-only Kafka client optimised for Warpstream.
// Safe for concurrent use.
type WarpstreamClient struct {
    pool    *AgentPool
    buffer  *RecordBuffer
    hedger  *Hedger
    tracker *LatencyTracker
    metrics *metrics
    logger  log.Logger
    done    chan struct{} // closed by Close to stop the background refresh goroutine
}

func NewWarpstreamClient(cfg Config, logger log.Logger, reg prometheus.Registerer) (*WarpstreamClient, error)

// ProduceSync produces records and blocks until the linger timer fires, the records are
// sent to Warpstream, and the broker acknowledges. Returns one ProduceResult per record.
func (c *WarpstreamClient) ProduceSync(ctx context.Context, records []*kgo.Record) kgo.ProduceResults

// Close flushes buffered records, stops background goroutines, and closes the
// underlying kgo.Client. Safe to call only once.
func (c *WarpstreamClient) Close()
```

Note: `Close()` returns nothing to match the `kafkaProducer` interface in
`pkg/storage/ingest/writer_client.go`. Errors during the final flush are logged and
reflected in metrics.

**`NewWarpstreamClient` steps:**
1. Build `kgo.Client` with `commonKafkaClientOptions` style options (TLS, SASL, seed
   brokers, metadata ages, `RequiredAcks(AllISRAcks())`, `ProduceRequestTimeout`). Wire
   the same `kprom.NewMetrics` and `KafkaClientExtendedMetrics` hooks used by the generic
   producer.
2. Create `KafkaSender`, `LatencyTracker`, `AgentPool` (call `Refresh` — fail startup on
   error).
3. Create `Hedger` and `RecordBuffer` with the `FlushFunc` closure below.
4. Start background goroutine that calls `pool.Refresh` every `MetadataMaxAge` and purges
   the `LatencyTracker` of any NodeIDs the refresh reports as removed:
   ```go
   removed, err := c.pool.Refresh(ctx)
   if err != nil {
       // log and continue; next tick will retry
   } else if len(removed) > 0 {
       c.tracker.Purge(removed)
   }
   ```

**`FlushFunc` closure (defined inside `NewWarpstreamClient`):**
```go
flush := func(ctx context.Context, nodeID int32, records []*kgo.Record, done func(error)) {
    req := buildProduceRequest(c.pool.topic, produceAPIVersion, records)
    secondaryID, hasSecondary := c.pool.Secondary(c.pool.topic, records[0].Partition)
    go func() {
        var err error
        if hasSecondary {
            err = c.hedger.Produce(ctx, nodeID, secondaryID, req)
        } else {
            err = sendWithTracking(ctx, c.producer, c.tracker, nodeID, req)
        }
        done(err)
    }()
}
```

**`ProduceSync` implementation:**
Group records by partition, add each group to the buffer with a per-group callback, wait for
all callbacks.

```go
func (c *WarpstreamClient) ProduceSync(ctx context.Context, records []*kgo.Record) kgo.ProduceResults {
    results := make(kgo.ProduceResults, len(records))
    groups, indexMap := groupByPartition(records) // returns map[int32][]*kgo.Record and index map

    var wg sync.WaitGroup
    wg.Add(len(groups))
    for partition, group := range groups {
        partition, group := partition, group
        indices := indexMap[partition]
        c.buffer.Add(ctx, group, func(err error) {
            for i, idx := range indices {
                results[idx] = kgo.ProduceResult{Record: group[i], Err: err}
            }
            wg.Done()
        })
    }
    wg.Wait()
    return results
}
```

**`Close` shutdown order:**
1. Close the `done` channel to stop the background refresh goroutine.
2. Call `c.buffer.Close()` — flushes all pending batches and drains callbacks.
3. Call `c.pool.client.Close()` — closes the kgo.Client last (all other components stop first).

### Test cases

```go
// newTestableClient creates a WarpstreamClient with injected mock components.
// Use this instead of NewWarpstreamClient in all unit tests.
func newTestableClient(t *testing.T, sender Sender, selector PartitionAssignmentStrategy) *WarpstreamClient

func TestWarpstreamClientProduceSync(t *testing.T) {
    tests := map[string]struct {
        records      []*kgo.Record
        senderSetup  func(*mockSender)
        wantErrCount int
    }{
        "all records succeed": {
            records: makeRecords(0, "a", "b"), senderSetup: func(_ *mockSender) {},
        },
        "produce failure propagates to all records in the batch": {
            records: makeRecords(0, "a", "b"),
            senderSetup: func(m *mockSender) { m.errors[1] = errors.New("fail") },
            wantErrCount: 2,
        },
        "records across two partitions: failures are partition-scoped": {
            records: append(makeRecords(0, "a"), makeRecords(1, "b")...),
            senderSetup: func(m *mockSender) { m.errors[2] = errors.New("fail") },
            wantErrCount: 1, // only partition 1 fails
        },
    }
    for name, tc := range tests {
        t.Run(name, func(t *testing.T) { ... })
    }
}

func TestWarpstreamClientClose(t *testing.T) {
    t.Run("pending records are flushed before Close returns", func(t *testing.T) { ... })
    t.Run("background refresh goroutine stops on Close", func(t *testing.T) { ... })
}
```

### Review focus

- `Close()` shutdown order: background goroutine → buffer drain → kgo.Client close.
- `ProduceSync` result slice preserves input record order.
- The `FlushFunc` goroutine does not retain the `records` slice after `done` is called.
- `newTestableClient` does not start the background refresh goroutine (to avoid goroutine
  leaks in tests).

---

## Step 8 — Integration in `pkg/storage/ingest/`

### Purpose

Introduce the `backend` config option and wire `pkg/warpstreamclient` into the existing
ingest writer. `WarpstreamClient` satisfies `kafkaProducer` directly; no adapter struct is
needed.

### Files

- `pkg/storage/ingest/config.go` *(modified)*
- `pkg/storage/ingest/config_test.go` *(modified)*
- `pkg/storage/ingest/writer_client.go` *(modified)*
- `pkg/storage/ingest/writer.go` *(modified)*
- `pkg/storage/ingest/writer_test.go` *(modified)*

### `config.go` changes

```go
const (
    KafkaBackendKafka      = "kafka"
    KafkaBackendWarpstream = "warpstream"
)

var kafkaBackendOptions = []string{KafkaBackendKafka, KafkaBackendWarpstream}

var ErrInvalidKafkaBackend = fmt.Errorf(
    "the configured kafka backend is invalid, must be one of: %s",
    strings.Join(kafkaBackendOptions, ", "))

// New fields in KafkaConfig:
Backend string `yaml:"backend"`

// Warpstream-specific fields (ignored when backend=kafka):
WarpstreamHedgeMinSamples      int     `yaml:"warpstream_hedge_min_samples"`
WarpstreamHedgeSlowMultiplier  float64 `yaml:"warpstream_hedge_slow_multiplier"`
WarpstreamHedgeMaxSlowFraction float64 `yaml:"warpstream_hedge_max_slow_fraction"`
WarpstreamMaxAgents            int     `yaml:"warpstream_max_agents"`
```

Flags in `RegisterFlagsWithPrefix`:
```go
f.StringVar(&cfg.Backend, prefix+"backend", KafkaBackendKafka,
    "Kafka producer backend implementation. Supported values: kafka, warpstream.")
f.IntVar(&cfg.WarpstreamHedgeMinSamples, prefix+"warpstream-hedge-min-samples", 10,
    "Minimum produce samples per agent before dynamic hedging activates. "+
    "Only applies when -"+prefix+"backend=warpstream.")
f.Float64Var(&cfg.WarpstreamHedgeSlowMultiplier, prefix+"warpstream-hedge-slow-multiplier", 2.0,
    "Hedge when primary latency exceeds this multiple of the cluster baseline. "+
    "Only applies when -"+prefix+"backend=warpstream.")
f.Float64Var(&cfg.WarpstreamHedgeMaxSlowFraction, prefix+"warpstream-hedge-max-slow-fraction", 0.3,
    "Suppress hedging when more than this fraction of agents are slow (widespread issue). "+
    "Only applies when -"+prefix+"backend=warpstream.")
f.IntVar(&cfg.WarpstreamMaxAgents, prefix+"warpstream-max-agents", 0,
    "Maximum agents to maintain connections to; 0 means all. "+
    "Only applies when -"+prefix+"backend=warpstream.")
```

Validation in `KafkaConfig.Validate()`:
```go
if !slices.Contains(kafkaBackendOptions, cfg.Backend) {
    return ErrInvalidKafkaBackend
}
```

### `writer_client.go` changes

```go
// kafkaProducer is the internal interface for both backend implementations.
type kafkaProducer interface {
    ProduceSync(ctx context.Context, records []*kgo.Record) kgo.ProduceResults
    Close()
}

// newGenericProducer creates the franz-go-backed producer (backend=kafka).
func newGenericProducer(cfg KafkaConfig, logger log.Logger, reg prometheus.Registerer) (kafkaProducer, error)

// newWarpstreamProducer creates the Warpstream-optimised producer (backend=warpstream).
// warpstreamClientConfig maps KafkaConfig fields to warpstreamclient.Config.
func newWarpstreamProducer(cfg KafkaConfig, logger log.Logger, reg prometheus.Registerer) (kafkaProducer, error) {
    wsCfg := warpstreamClientConfig(cfg)
    return warpstreamclient.NewWarpstreamClient(wsCfg, logger, reg)
    // *warpstreamclient.WarpstreamClient satisfies kafkaProducer directly;
    // no adapter struct is required.
}

// warpstreamClientConfig maps the relevant KafkaConfig fields to warpstreamclient.Config.
func warpstreamClientConfig(cfg KafkaConfig) warpstreamclient.Config { ... }
```

**No `warpstreamAdapter` struct is needed.** `*warpstreamclient.WarpstreamClient` has
`ProduceSync(ctx, []*kgo.Record) kgo.ProduceResults` and `Close()` (no error return),
satisfying `kafkaProducer` directly.

### `writer.go` changes

`atomic.Pointer[KafkaProducer]` must become `atomic.Pointer[clientHolder]` to avoid the
double-indirection of storing a pointer to an interface:

```go
// clientHolder wraps kafkaProducer so it can be stored in an atomic.Pointer
// without the ambiguity of a pointer-to-interface.
type clientHolder struct {
    p kafkaProducer
}

type Writer struct {
    // client was atomic.Pointer[KafkaProducer]; the new type avoids pointer-to-interface.
    client atomic.Pointer[clientHolder]
    ...
}
```

Usage in `starting()`:
```go
switch w.kafkaCfg.Backend {
case KafkaBackendWarpstream:
    producer, err = newWarpstreamProducer(w.kafkaCfg, w.logger, clientReg)
default:
    producer, err = newGenericProducer(w.kafkaCfg, w.logger, clientReg)
}
if err != nil { return err }
w.client.Store(&clientHolder{p: producer})
```

Usage in `MultiWriteSync` and `stopping()` via `w.client.Load().p`.

### Test cases

```go
func TestKafkaConfigValidate_Backend(t *testing.T) {
    tests := map[string]struct {
        backend string
        wantErr bool
        wantIs  error
    }{
        "kafka is valid":       {backend: "kafka"},
        "warpstream is valid":  {backend: "warpstream"},
        "empty is invalid":     {backend: "", wantErr: true, wantIs: ErrInvalidKafkaBackend},
        "unknown is invalid":   {backend: "confluent", wantErr: true, wantIs: ErrInvalidKafkaBackend},
        "case sensitive":       {backend: "Kafka", wantErr: true, wantIs: ErrInvalidKafkaBackend},
    }
    for name, tc := range tests {
        t.Run(name, func(t *testing.T) {
            cfg := validKafkaConfig()
            cfg.Backend = tc.backend
            err := cfg.Validate()
            if tc.wantErr {
                require.ErrorIs(t, err, tc.wantIs)
            } else {
                require.NoError(t, err)
            }
        })
    }
}

func TestWarpstreamClientConfigMapping(t *testing.T) {
    t.Run("all KafkaConfig fields are mapped correctly", func(t *testing.T) {
        // Verify each field of warpstreamclient.Config against the source KafkaConfig.
    })
}
```

### Review focus

- `clientHolder` is a trivial struct with no logic — confirm the reviewer agrees it is the
  correct way to avoid pointer-to-interface in `atomic.Pointer`.
- `warpstreamClientConfig` maps *every* relevant field; no silently dropped settings.
- The full ingest test suite passes: `go test ./pkg/storage/ingest/...`.
- `make reference-help doc` succeeds after the new flags are added.

---

## Final verification

After all steps pass their individual reviews:

1. `make format` — no diff.
2. `go build ./...` — clean.
3. `go test -race ./pkg/warpstreamclient/... ./pkg/storage/ingest/...` — all pass.
4. `go test -bench=. -benchmem ./pkg/warpstreamclient/...` — no regressions vs. step benchmarks.
5. `make reference-help doc` — config reference updated for new flags.
6. `CHANGELOG.md` — add `[FEATURE]` entry for the new backend.

---

## Deep review: known risks and performance considerations

The following issues were identified during plan review. Each step's implementation must
address the relevant items before passing the code-reviewer agent.

### Correctness

**`LatencyTracker` must be purged when agents leave the cluster.** `LatencyTracker` entries
for dead agents are never automatically removed. If not purged, stale entries with elevated
EMA values skew the cluster baseline used by `HedgeDecision`, potentially suppressing hedging
for healthy agents (because the slow fraction appears higher than it really is). The fix:
`AgentPool.Refresh` returns the set of removed NodeIDs; the background refresh goroutine in
`WarpstreamClient` calls `tracker.Purge(removed)` after every Refresh. Test
`TestLatencyTrackerPurgeDoesNotSkewBaseline` must verify this correction.

**`Close()` ordering is critical.** `buffer.Close()` must complete (all in-flight `FlushFunc`
goroutines finish and all `done` callbacks fire) before `kgo.Client.Close()` is called.
Closing the client first would cause in-flight `Broker.RetriableRequest` calls to fail.

**`RecordBuffer.Close()` must wait for in-flight flushes.** The `FlushFunc` is invoked in a
goroutine (inside `client.go`). If `Close()` only drains the pending batches but does not wait
for the goroutines started by `FlushFunc`, `done` callbacks may fire after `Close()` returns,
leading to use-after-free on shared state. Use a `sync.WaitGroup` inside `RecordBuffer` to
track in-flight flushes.

**Context not propagated to linger callbacks.** Once records are buffered, they will be sent
when the linger timer fires even if the caller's context is later cancelled. This is
intentional (see Step 6 notes) but must be explicitly documented in the `Add` function
comment so future maintainers do not assume otherwise.

**`selectSecondary` with empty `all` slice.** If `all` is empty (no agents known yet), the
function must return `(0, false)` without panicking. Add a guard at the top of the function.

**`LatencyTracker.HedgeDecision` with one agent.** If only one agent is known, `slowFraction`
is either 0% or 100%. The 100% case (one agent is slow) would suppress hedging even though
there is a real problem. This is acceptable: with a single agent there is no secondary to
hedge to, and `PartitionAssignmentStrategy.Secondary()` would return `ok=false` anyway, preventing the
hedge from being triggered at all.

### Performance

**`RecordBuffer.Add` allocates a closure per call** (the `done func(error)` parameter).
This is unavoidable for the first implementation; benchmark to measure the per-call allocation
cost. If profiling shows it as a bottleneck, consider a pre-allocated callback pool.

**`buildProduceRequest` may allocate a scratch buffer** for Snappy compression. Use a
`sync.Pool` of byte slices to eliminate this allocation. Confirm with benchmarks before
introducing the pool.

**`groupByPartition` in `ProduceSync` allocates a map.** For the common case of records all
belonging to one partition (typical in Mimir's per-partition write pattern), a fast path that
skips the map allocation should be added after the initial implementation is benchmarked.

**`LatencyTracker.HedgeDecision` scans all agent stats** to find the median. Clusters range
up to ~1 000 agents, so a full linear scan on every produce request is too expensive. The
median must be maintained incrementally — update a running approximate-median structure (e.g.
a pair of heaps or a P² histogram) when `Record` is called rather than computing it from
scratch on every `HedgeDecision` call. `HedgeDecision` then reads the pre-computed value in
O(1) with no allocation. Avoid `sort.Slice` or any per-call allocation in this function.

**`atomic.Pointer[clientHolder]`** introduces one extra heap allocation per `Store` call
(constructing the `clientHolder`). This happens only at startup and shutdown, not on the hot
path, so it is acceptable.

**`secondaryCache` in `AgentPool`** uses `map[topicPartition]int32`. Since both NodeIDs and
partition-leader assignments are stable across routine Metadata refreshes (Warpstream derives
NodeIDs deterministically from the agent UUID, and the partition assignment strategy used in
production does not rotate leaders), the cache only needs to be invalidated when the agent set
actually changes (agent added or replaced). This makes the cache effective for the full
lifetime of a stable deployment.

### Observability

**Hedge suppression is not observable.** When hedging is suppressed due to widespread slowness,
nothing in the current metrics indicates this. Add a `hedgeSuppressionTotal` counter
incremented whenever `HedgeDecision` returns `(0, false)` due to the `maxSlowFraction` check.
This makes the difference between "primary was healthy" and "hedging suppressed due to outage"
visible in dashboards.

**LatencyTracker warmup is not observable.** Add a gauge `agentSamplesCount{agent}` so
operators can see which agents have reached the `minSamples` threshold for hedging.
