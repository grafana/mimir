# Research: Custom Warpstream-Optimized Kafka Producer for Grafana Mimir

> Status: Research complete. All open questions resolved.

---

## 1. Context and Motivation

Mimir uses the [franz-go](https://github.com/twmb/franz-go) Kafka client for ingest-storage. Writes flow
from distributors → Kafka/Warpstream → ingesters. In Grafana Cloud, Warpstream is the Kafka backend.

The Kafka protocol was designed around vanilla Kafka's architecture: each partition has exactly one
leader broker, and all Produce requests for that partition **must** go to the leader. This makes
hedging impossible in vanilla Kafka without producing duplicates (which would corrupt data in Kafka's
ordering-sensitive model).

Warpstream breaks this constraint: its agents are **fully stateless**. Any Warpstream agent can
accept a Produce request for **any** partition at **any** time. Combined with Mimir's relaxed delivery
requirements (at-least-once, no sequencing guarantee), this opens the door to a Produce-only client
that can hedge requests across agents — something no standard Kafka client supports.

---

## 2. Current Implementation Analysis

**Files:**
- `pkg/storage/ingest/writer_client.go` — KafkaProducer, franz-go client creation
- `pkg/storage/ingest/writer.go` — WriteSync/MultiWriteSync, record serialization
- `pkg/storage/ingest/util.go` — common kgo.Client options
- `pkg/storage/ingest/config.go` — configuration flags
- `pkg/storage/ingest/version.go` — record format v0/v1/v2

**Key configuration used today:**
```
kgo.RecordPartitioner(kgo.ManualPartitioner())   // partitions explicitly set per record
kgo.RequiredAcks(kgo.AllISRAcks())               // wait for all ISR acks
kgo.DisableIdempotentWrite()                     // no exactly-once needed
kgo.ProducerBatchMaxBytes(16_000_000)            // 16 MB batches
kgo.ProducerLinger(50ms)                         // batch up to 50ms
kgo.MaxProduceRequestsInflightPerBroker(20)      // 20 in-flight per broker
kgo.RecordDeliveryTimeout(10s)                   // fail after 10s
kgo.MetadataMinAge(10s), MetadataMaxAge(10s)     // refresh every 10s
```

**Current produce flow:**
```
Distributor computes partition → WriteSync() → serializer.ToRecords() → KafkaProducer.ProduceSync()
  → kgo.Client.Produce() → franz-go routes to partition leader → blocks until ack
```

**Known Warpstream quirks already handled:**
1. `MinBytes` is ignored by Warpstream (uses `MaxWaitMillis` instead)
2. Fetching past high watermark causes excess delay → code guards against this
3. Warpstream returns `BrokerNotAvailable` in unexpected places → special error handling
4. `use-compressed-bytes-as-fetch-max-bytes=false` needed for correct fetch sizing

**What Mimir does NOT need:**
- Exactly-once semantics (`DisableIdempotentWrite()` is set)
- Sequencing within a partition (documented in the design)
- Consumer group management (produce-only concern here)
- Transactions

---

## 3. Warpstream Architecture: The Key Technical Enabler

### Stateless agents

Warpstream agents store nothing locally. All data goes directly to object storage (S3). This means:
- There is no "partition leader" concept per-agent
- **Any agent can accept a Produce request for any partition**
- Agents are completely interchangeable

The official documentation states: _"any Agent can write or read any record for any topic-partition at any time."_

### Agent discovery via Metadata

When a Kafka client sends a MetadataRequest to Warpstream, the response returns the full list of
Warpstream agent addresses as "brokers", and assigns one agent as the authoritative leader for each
partition. This leader assignment exists solely to satisfy the Kafka protocol — which mandates a
partition leader — but it carries no real routing constraint in Warpstream's stateless model.

Franz-go today respects this: it routes partition N's Produce requests to whichever agent Metadata
designated as leader for N. The LB seed address is only used for the initial connection that
bootstraps the Metadata response; subsequent Produce requests go directly to individual agent
addresses returned by Metadata.

**The key insight for hedging:** the full broker list in the Metadata response is the pool of
all available Warpstream agents. A custom client can maintain connections to all of them and, when
hedging, send the same Produce request to a second agent (not the designated leader) — which works
because any agent can handle any partition. Franz-go today does not exploit this: it routes strictly
to the designated leader and never hedges.

### NodeID stability

**NodeID stability (confirmed from Warpstream source):** Warpstream assigns each agent a
NodeID by deterministically hashing the agent's UUID. A running agent always advertises the
same NodeID in every Metadata response — NodeIDs are stable across routine metadata refreshes.
They only change when an agent is replaced (restarted with a new identity), which is an
infrequent operational event.

**Partition-leader assignment stability:** Warpstream supports multiple partition assignment
strategies. The strategy used in our production deployment keeps partition-leader assignments
stable: the same agent remains the designated leader for a given partition across successive
metadata refreshes under normal operation, changing only when the agent set changes.

Note: the blog post "Hacking the Kafka Protocol" describes a round-robin assignment strategy
where the leader can rotate on every MetadataRequest. That strategy is not the one used here
and should not be assumed.

**Implications for the custom client design:**

1. **LatencyTracker** — Stable NodeIDs mean per-agent latency statistics accumulate correctly
   across metadata refreshes. A replaced agent (new NodeID) naturally starts a fresh entry,
   which is the desired behaviour.

2. **Secondary cache keyed on `(topic, partition)`** — Both the primary NodeID and the
   secondary NodeID are stable between Refresh calls as long as the agent set is unchanged.
   The cache only needs invalidation when the agent set changes (new or removed agent). This
   makes the cache effective for the full lifetime of a stable deployment.

### Direct agent connectivity

Warpstream explicitly recommends direct connectivity over load balancers:
> _"while you can run the WarpStream Agents behind a load balancer, it may result in reduced
> performance, and whenever possible, direct connectivity between Kafka clients and the WarpStream
> Agents is preferred."_

Warpstream supports agent targeting via:
- **Agent Groups** (`warpstream_agent_group` config or `ws_ag=<group>` in client ID)
- **Agent Roles** (`warpstream_proxy_target` configuration)
- **Zone-aware routing** (`ws_az=<zone>` encoded in client ID)

---

## 4. Kafka Produce Protocol: Minimum Surface Area

To produce a Kafka record from scratch, a client needs to implement:

### Wire framing
```
RequestHeader:  correlation_id (int32), api_key (int16), api_version (int16), client_id (string)
ResponseHeader: correlation_id (int32), error_code (int16)
```
TCP, big-endian, length-prefixed frames. Supports request pipelining (multiple outstanding requests).

### Required API calls (in order)

| API Key | Name | When needed |
|---------|------|-------------|
| 18 | ApiVersions | Once on connect, to negotiate versions |
| 17 | SASLHandshake | Once on connect, if SASL enabled |
| 36 | SASLAuthenticate | Once per connect, SASL credential exchange |
| 3 | Metadata | On startup and on error, to discover agents |
| 0 | Produce | For every batch of records |

TLS handshake occurs at the TCP layer before any Kafka protocol frames.

### Produce API wire format (v3+, the current standard)

```
ProduceRequest {
  transactional_id: string (null for non-transactional)
  acks: int16 (-1=all ISR, 1=leader only, 0=no ack)
  timeout_ms: int32
  topics[]:
    name: string
    partitions[]:
      index: int32
      records: RecordBatch (see below)
}
```

**RecordBatch format (magic=2, the current standard since Kafka 0.11):**
```
baseOffset:              int64
batchLength:             int32
partitionLeaderEpoch:    int32 (-1 for non-replica producers)
magic:                   int8 (= 2)
crc:                     int32 (CRC-32C of attributes..end)
attributes:              int16 (bits 0-2: compression, bit 4: isTransactional)
lastOffsetDelta:         int32
firstTimestamp:          int64 (ms)
maxTimestamp:            int64 (ms)
producerId:              int64 (-1 if no idempotence)
producerEpoch:           int16 (-1 if no idempotence)
baseSequence:            int32 (-1 if no idempotence)
records[]:
  length:                varint
  attributes:            int8 (currently unused)
  timestampDelta:        varint
  offsetDelta:           varint
  key:                   bytes (varint length, then data)
  value:                 bytes (varint length, then data)
  headers[]:
    key:                 bytes
    value:               bytes
```

**Key simplifications available for Mimir's use case:**
- `transactional_id` = null (no transactions)
- `producerId` = -1, `producerEpoch` = -1, `baseSequence` = -1 (no idempotence)
- Compression: Snappy is required (franz-go default; already used in production). Set `attributes` bits 0-2 = `2` (CodecSnappy). `github.com/klauspost/compress/s2` is already vendored and implements the required format.
- Headers needed only for record version marker (v0/v1/v2)

**CRC calculation:** CRC-32C (Castagnoli) over bytes from `attributes` to end of batch. The Go
standard library has `hash/crc32` with `crc32.MakeTable(crc32.Castagnoli)`.

### What we do NOT need to implement
- Fetch API (consumer-side)
- CreateTopics / DeleteTopics (admin operations)
- OffsetCommit / OffsetFetch (consumer groups)
- InitProducerId (idempotent/transactional producer setup)
- FindCoordinator, JoinGroup, SyncGroup, Heartbeat (consumer group coordination)
- ListOffsets (reader-side)
- LeaderEpoch handling (replica-specific)

---

## 5. Hedging / Edging Design

### The core insight

With Warpstream, since any agent handles any partition, we can **send the same ProduceRequest to two
different agents** and accept the first success. In vanilla Kafka this would route to two different
servers for the same partition (error: NotLeaderForPartition). With Warpstream it works correctly —
the first agent to respond wins, the second becomes a duplicate (which Mimir tolerates).

### Hedging pattern (gRPC-style)

```
t=0ms:   Send ProduceRequest to agent A
t=50ms:  No response yet → Send same ProduceRequest to agent B (hedge)
t=65ms:  Agent B responds with success → return success, cancel agent A if possible
t=80ms:  Agent A responds → ignore (or let it complete; duplicate ingestion is OK)
```

The hedging delay (50ms here) is configurable. Setting it to 0 makes it speculative parallel sends.

### Where to get the second agent address

**From Kafka Metadata response** — the only mechanism needed.

The Warpstream Metadata response returns the full list of individual agent addresses as brokers, and
designates one as the partition leader. The custom client parses this and maintains a connection pool
(one connection per agent). The pool refreshes on error or on a periodic Metadata refresh.

### Secondary agent selection: consistent and deterministic

A key requirement: given the same Metadata response, **all Mimir distributor processes must select
the same secondary agent for a given topic-partition**. This prevents the secondary load from being
randomly distributed and allows predictable, analysable behaviour.

**Stable broker ordering:** sort all brokers by `NodeID` (an int32 assigned per broker in the
Metadata response). This is stable across processes and Metadata refreshes as long as the agent pool
doesn't change.

**Primary:** the broker the Metadata response designates as partition leader (no change from
standard Kafka behaviour).

**Secondary selection algorithm:**

```go
func selectSecondary(topic string, partitionID int32, primary Broker, allBrokers []Broker) Broker {
    // Build candidate list: all agents except primary, sorted by NodeID (stable)
    candidates := sortedByNodeID(excludeBroker(allBrokers, primary))

    // Deterministic selection: hash of (topic, partitionID)
    h := xxhash.Sum64String(topic + ":" + strconv.Itoa(int(partitionID)))
    return candidates[h%uint64(len(candidates))]
}
```

Properties:
- **Consistent across processes**: same sorted broker list + same hash = same secondary, everywhere
- **Spreads secondary load**: different partitions hash to different secondaries, distributing the
  hedge traffic across the agent pool
- **Stable under Metadata refresh**: as long as the agent pool is unchanged, the same secondary is
  selected; if the pool changes (agent added/removed), secondaries may shift — which is acceptable

### Rack field research: node/AZ diversity is not achievable

**Background.** The Kafka Metadata protocol includes a `Rack *string` field per broker
(`MetadataResponseBroker`, API v1+), designed for rack/AZ awareness so clients can prefer
topologically diverse replicas. In standard Kafka this contains the broker's rack label (e.g.
`"us-east-1a"`). The natural idea for the secondary selection would be to prefer agents whose `Rack`
differs from the primary's, guaranteeing the hedge lands on a different failure domain.

**Finding: Warpstream always reports `"warpstream-fake-rack"`.**

As of Warpstream v784 (April 20, 2026), all agents report the literal string `"warpstream-fake-rack"`
as their `Rack` value in every Metadata response, regardless of which AZ or physical node they run
on. Before v784 the actual AZ was reported, but this was changed because misconfigured Kafka clients
that enable the rack-aware consumer strategy (without also configuring Warpstream's own zone-aware
service discovery) were causing excessive consumer group rebalances.

Warpstream's zone-awareness is intentionally asymmetric: clients encode their AZ in the Kafka client
ID (`ws_az=<zone>`), and Warpstream's discovery service uses this to route the Metadata response so
that the partition leader is a same-AZ agent. Agents never expose their own location back to clients
through the Metadata response.

**Decision: node/AZ diversity removed from secondary selection.**

Because `Rack` is always `"warpstream-fake-rack"`, there is no information available in the Metadata
response to determine whether two agents run on different nodes or in different AZs. Node-diverse
secondary selection is therefore not implementable without infrastructure-level changes (e.g.
separate per-AZ seed addresses so the client can build per-AZ agent pools).

The secondary selection algorithm uses only `NodeID` for stable ordering and a deterministic hash
for selection. This guarantees a **different agent process** for the hedge — which still provides
meaningful redundancy against single-agent failures (memory pressure, network glitch, hot partition,
process crash) — but makes no guarantee about node or AZ diversity.

### Lingering (record batching with delay)

**What lingering is.** Rather than sending a ProduceRequest immediately when `ProduceSync` is
called, the client buffers incoming records for a configurable period (`linger`). During that window,
records from other concurrent `ProduceSync` callers accumulate into the same batch. When the linger
timer expires (or the batch fills up), all accumulated records are sent together in one ProduceRequest
per agent. This is the primary throughput optimisation in any Kafka producer: more records per wire
round-trip at the cost of a bounded added latency.

Mimir currently configures `kgo.ProducerLinger(50ms)`. The custom client must replicate this
behaviour. The existing `-ingest-storage.kafka.producer-linger-disable` flag is reused unchanged.

**Reference: how franz-go implements it.**

From exploring franz-go's source (`vendor/github.com/twmb/franz-go/pkg/kgo/sink.go`):

- The linger timer is **per-partition** (`recBuf.lingering *time.Timer`), started when the first
  record for that partition is buffered.
- A single ProduceRequest can carry batches for **multiple topic-partitions** (grouped by agent/sink).
- Flush is triggered by: timer expiry; current batch full (a second batch is started, meaning the
  first must be drained immediately); explicit `Flush()`; max-buffered-records/bytes limit hit.
- **`ProduceSync` bypasses the linger**: after buffering all records it immediately calls
  `unlingerAndManuallyDrain()` on every affected partition. This avoids making a synchronous caller
  wait for the full linger period when it is the only active producer. Other records that were
  already in the buffer (from concurrent callers before the drain was triggered) are included in the
  same flush.

**Design for the custom client.**

The linger buffer sits between `WarpstreamProducer.ProduceSync()` and `agentConn.produce()`:

```
WarpstreamProducer.ProduceSync(records)
  │
  ▼
lingerBuffer.add(records)          ← per-partition accumulation, timer starts on first record
  │
  ├── if batch full → immediate flush trigger
  └── if ProduceSync (synchronous caller) → immediate flush trigger (same as franz-go)
  │
  ▼
flusher goroutine (one per agent)
  │  groups ready partition batches into one ProduceRequest
  ▼
agentConn.produce(req)             ← sends with hedging, waits for ack
  │
  ▼
fire completion channels for all waiting ProduceSync callers
```

**Key structs:**

```go
type topicPartition struct {
    topic     string
    partition int32
}

type partitionBuffer struct {
    records    []*kgo.Record
    wireBytes  int32          // running encoded size
    callbacks  []func(error)  // one entry per ProduceSync call that contributed records
    timer      *time.Timer
}

type lingerBuffer struct {
    mu      sync.Mutex
    bufs    map[topicPartition]*partitionBuffer
    ready   chan struct{}  // closed/reopened to wake the flusher
}
```

**Flush triggers (matching franz-go semantics):**

| Trigger | Action |
|---------|--------|
| Linger timer fires | Wake flusher; drain all ready partitions |
| Record would exceed `ProducerBatchMaxBytes` | Flush current batch immediately, start new batch for overflow record |
| `ProduceSync` caller (synchronous) | Flush immediately after buffering, don't wait for timer |
| Max buffered bytes reached | Flush immediately; new `ProduceSync` calls block until space is freed |
| `Close()` | Flush all pending, drain to zero |

**ProduceSync semantics with lingering:**

`ProduceSync` buffers its records into the linger buffer and then immediately triggers a flush (same
as franz-go). This means:

1. Records are added to the per-partition buffer.
2. The linger timer is cancelled for all affected partitions and an immediate drain is triggered.
3. The flusher groups all currently buffered records (including records added by other concurrent
   callers before the drain was triggered) into one ProduceRequest per agent.
4. The caller blocks on a `chan error` until the broker acknowledges the batch.

This preserves the batching benefit: if goroutines A and B both call `ProduceSync` concurrently for
the same partition, A's drain will include B's records if B buffered before A's drain fired.

**Multi-partition grouping per agent:**

When the flusher drains, it groups all ready partition batches by their primary agent:

```
ready batches: {(topic,0)→agentA, (topic,1)→agentB, (topic,2)→agentA, (topic,3)→agentB}
  →  ProduceRequest to agentA: partitions 0, 2
  →  ProduceRequest to agentB: partitions 1, 3
  (sent concurrently, each with its own hedge)
```

Each ProduceRequest may include multiple topic-partitions. All callbacks for all records in the
request are fired once the agent acknowledges.

**Interaction with hedging:**

Hedging operates at the ProduceRequest level (one request = one linger flush for one agent). The
sequence is:

```
linger flush → build ProduceRequest
  → produce to primary agent (async)
  → if no ack within hedgeDelay → send same ProduceRequest to secondary agentConn
  → first success fires all callbacks for that batch
```

Because Mimir tolerates duplicate records, a secondary ack that arrives after the primary's is
silently discarded.

**Interaction with max in-flight:**

Each `agentConn` has an in-flight semaphore (capacity = `MaxInflightProduceRequests`). The flusher
acquires the semaphore before sending each ProduceRequest and releases it on response. If the
semaphore is saturated, the flusher blocks, and records continue accumulating in the linger buffer
in the meantime.

**Batch splitting:**

If a single `ProduceSync` call delivers more bytes than `ProducerBatchMaxBytes` for one partition,
the records are split across multiple successive `partitionBuffer` flushes. This matches the
existing Mimir behaviour where `Writer.MultiWriteSync` already pre-splits large WriteRequests via
`mimirpb.SplitWriteRequestByMaxMarshalSize` before calling `ProduceSync`.

### Connection management

Each agent gets a dedicated TCP connection. Connection setup (TLS + SASL handshake) happens once,
then the connection is reused for all subsequent Produce requests to that agent.

---

## 6. Feasibility of Reusing franz-go via `Broker.Request()`

### The idea

Instead of building the entire network layer from scratch (Option B/C below), reuse franz-go's
existing connection infrastructure — TLS, SASL, wire framing, pipelining, reconnection — while
retaining full control over which agent receives each ProduceRequest, when it is sent, and whether
a hedge is issued. The key API is `kgo.Client.Broker(nodeID).Request(ctx, req)`.

### API surface

**`Client.Broker(id int) *Broker`** (`client.go:2397`):
Returns a handle to a specific broker by its `NodeID` (the int32 assigned in the Metadata response).
No guarantee the broker exists — requests will fail with an unknown-broker error if it doesn't.

**`Client.DiscoveredBrokers() []*Broker`** (`client.go:2411`):
Returns all brokers seen in prior Metadata responses, without issuing a new Metadata request. Used
to enumerate the full agent pool after a metadata fetch.

**`Broker.Request(ctx, req) (kmsg.Response, error)`** (`client.go:2483`):
Issues any `kmsg.Request` (including `kmsg.ProduceRequest`) to that specific broker. **Not retried.**
Returns the parsed response or an error. The context can cancel it, but note that if the request has
already been written to the wire, the cancellation only affects waiting for the response.

**`Broker.RetriableRequest(ctx, req) (kmsg.Response, error)`** (`client.go:2490`):
Same as above but retries on retryable connection errors (not on Kafka response error codes). The
franz-go docs recommend preferring this over `Broker.Request()`.

### What franz-go handles automatically

When `Broker.Request()` is called with a `kmsg.ProduceRequest`, franz-go:

- Selects (or creates) the **dedicated produce connection** for that broker (`cxnProduce`,
  `broker.go:155`) — a separate TCP+TLS+SASL connection from those used for Fetch, Metadata, etc.
- Runs TLS handshake and SASL negotiation on first use; reuses the authenticated connection
  thereafter; reconnects automatically on idle timeout or connection drop.
- Handles the Kafka wire frame: request header (api key, version, correlation ID, client ID), length
  prefix, big-endian encoding — all transparent to the caller.
- Pipelines requests: multiple concurrent `Broker.Request()` calls to the same broker are enqueued
  in the broker's request ring; writes are serialized (fast) while response reads are parallel.
- **Rewrites `Acks` and `TimeoutMillis`** in `kmsg.ProduceRequest` to match the `kgo.Client`
  configuration (`broker.go:470-474`). This is not a problem: configure the client with
  `kgo.RequiredAcks(kgo.AllISRAcks())` and `kgo.ProduceRequestTimeout(cfg.WriteTimeout)` and the
  rewritten values are correct.

### What we implement on top

| Component | Owner |
|-----------|-------|
| TCP connection lifecycle, TLS, SASL (all mechanisms) | **franz-go** |
| Kafka wire framing, request pipelining, ApiVersions negotiation | **franz-go** |
| Metadata refresh (broker/agent discovery) | **franz-go** |
| Per-partition linger buffer, timer, completion callbacks, flusher | **We write** |
| Agent pool (derive from `DiscoveredBrokers()`) | **We write (trivial)** |
| Primary selection (from Metadata partition-leader field) | **We write (trivial)** |
| Secondary selection (consistent hash over sorted NodeIDs) | **We write** |
| Hedging (two concurrent `Broker.Request()` calls) | **We write** |
| `kmsg.ProduceRequest` construction from `kgo.Record` slice | **We write** |
| ProduceResponse parsing, per-partition error handling | **We write** |
| Prometheus metrics | **We write** |

### Hedging with `Broker.Request()`

```go
func (p *WarpstreamProducer) flushBatch(ctx context.Context,
    primary, secondary *kgo.Broker, req *kmsg.ProduceRequest) error {

    type result struct{ err error }
    ch := make(chan result, 2)

    go func() {
        resp, err := primary.RetriableRequest(ctx, req)
        ch <- result{parseProduceErr(resp, err)}
    }()

    select {
    case res := <-ch:
        return res.err  // primary replied before hedge delay
    case <-time.After(p.hedgeDelay):
        go func() {
            resp, err := secondary.RetriableRequest(ctx, req)
            ch <- result{parseProduceErr(resp, err)}
        }()
    }

    res1 := <-ch
    if res1.err == nil {
        return nil
    }
    res2 := <-ch
    return res2.err
}
```

Both `primary` and `secondary` are obtained via `client.Broker(nodeID)` using the NodeIDs from the
Metadata response. They go to different TCP connections (`cxnProduce` on different broker structs),
so there is no serialization between the two concurrent calls. Duplicates caused by both succeeding
are tolerated (Mimir is at-least-once).

### Obtaining the agent pool and broker selection

```go
// After a metadata request has been issued (franz-go does this on startup and periodically):
brokers := client.DiscoveredBrokers()   // all known agents
// sort by NodeID for stable ordering, then apply consistent hash for secondary selection
```

To get the partition-to-primary mapping, call `client.RequestCachedMetadata()` which serves from kgo's internal cache when fresh, and falls back to a real MetadataRequest only when stale. This avoids duplicate network requests when the kgo background refresh has already done it.
to any broker) and parse `resp.Topics[0].Partitions[i].Leader` to get the primary NodeID per
partition. This can be cached and refreshed on the same 10-second cadence as the current Mimir
configuration.

### Configuring the kgo.Client for this usage

The client is no longer used for its produce machinery — only for connections, metadata, and as a
vehicle for `Broker.Request()`. Producer-specific options (`ProducerLinger`, `ProducerBatchMaxBytes`,
`MaxProduceRequestsInflightPerBroker`, etc.) are irrelevant and should be omitted. The relevant
options are:

```go
kgo.SeedBrokers(cfg.Address)
kgo.ClientID(cfg.ClientID)
kgo.DialTimeout(cfg.DialTimeout)
kgo.MetadataMinAge(10 * time.Second)
kgo.MetadataMaxAge(10 * time.Second)
kgo.RequiredAcks(kgo.AllISRAcks())           // applied to ProduceRequest.Acks by franz-go
kgo.ProduceRequestTimeout(cfg.WriteTimeout)  // applied to ProduceRequest.TimeoutMillis
// TLS and SASL options unchanged from current commonKafkaClientOptions()
```

### Revised implementation scope

**Eliminated vs. Option B (kmsg + custom connections):**
- TCP connection management, reconnection, idle timeout → gone
- TLS handshake → gone
- SASL (PLAIN, SCRAM, OAuth — any mechanism Mimir supports today) → gone
- Kafka wire framing (request headers, length prefix, big-endian) → gone
- Request pipelining, correlation ID tracking, response demux → gone
- ApiVersions negotiation → gone

**Estimated implementation size:** ~800–1,200 lines of Go (excluding tests), down from the
2,000–3,000 lines of Option B. The linger buffer remains the most complex component.

### Caveats

1. **`Acks`/`TimeoutMillis` rewriting**: franz-go overwrites these fields in every `kmsg.ProduceRequest`
   passed through `Broker.Request()`. Not a problem in practice — configure the client with the
   correct values and the rewrite is a no-op. Documented warning in franz-go: _"It is strongly
   recommended to not issue raw kmsg.ProduceRequest's"_ — the concern is exactly this rewriting
   surprising callers who set different values.

2. **No per-request acks override**: All Produce requests through this client will use the same
   `acks` setting. For our use case this is fine (always `AllISRAcks`).

3. **franz-go version coupling**: The `Broker.Request()` API and `DiscoveredBrokers()` are public
   APIs and stable. The internal behaviour (which connection type is selected, rewriting logic) could
   change across franz-go versions, but is unlikely to break our usage.

4. **In-flight per agent**: franz-go does not expose the per-broker in-flight semaphore through the
   `Broker.Request()` path — it is only used in the producer sink. We implement our own in-flight
   limit if needed (e.g., a `chan struct{}` semaphore per agent in the flusher).

---

## 7. Implementation Options

### Option A: Multiple franz-go clients + hedging layer (lowest risk)

Create N franz-go clients (one per known agent endpoint). Add a thin hedging wrapper:

```go
type WarpstreamProducer struct {
    clients     []*KafkaProducer  // existing wrapper around kgo.Client
    hedgeDelay  time.Duration
    mu          sync.Mutex
    nextPrimary int               // round-robin
}

func (p *WarpstreamProducer) ProduceSync(ctx, records) error {
    primary := p.clients[p.nextPrimary % len(p.clients)]
    p.nextPrimary++

    type result struct { err error }
    ch := make(chan result, 2)

    go func() { ch <- result{primary.ProduceSync(ctx, records)} }()

    select {
    case res := <-ch:
        return res.err  // primary responded before hedge delay
    case <-time.After(p.hedgeDelay):
        // hedge
        hedge := p.clients[(p.nextPrimary) % len(p.clients)]
        go func() { ch <- result{hedge.ProduceSync(ctx, records)} }()
        return (<-ch).err  // first of {primary, hedge} to respond
    }
}
```

**Pros:** Minimal new code; reuses all of franz-go's protocol handling, TLS, SASL, retries, metrics.
**Cons:** Each kgo.Client maintains its own metadata connections; slightly wasteful; doesn't give full
control over routing decisions; still relies on franz-go's broker selection within each client.

### Option B: Custom connection layer + franz-go kmsg for protocol (medium complexity)

Use franz-go's `pkg/kmsg` package for protocol encoding/decoding, but implement custom:
- TCP connection management (one connection per agent)
- TLS handshake
- SASL authentication
- Request pipelining
- Hedging logic

The `kmsg` package provides `ProduceRequest`, `MetadataRequest`, etc. with `AppendTo()`/`ReadFrom()`
methods. This avoids reimplementing the Kafka wire format while giving full control over routing.

**Pros:** Reuses battle-tested protocol encoding; full control over connection/routing; no metadata
refresh overhead per-client; clean implementation tailored to Warpstream.
**Cons:** Still takes 2-4 weeks of engineering; need to implement TLS + SASL correctly; need to
handle all error cases; kmsg package may not be designed for standalone use (API could change).

### Option C: Full custom implementation from scratch (highest complexity)

Implement the entire protocol from TCP bytes up:
- TCP + TLS connection management
- Kafka wire framing (length-prefixed, big-endian)
- ApiVersions, SASLHandshake, SASLAuthenticate, Metadata, Produce APIs
- Record batch encoding (magic=2, CRC-32C, varint fields)
- Request pipelining
- Hedging logic

**Pros:** Zero external dependencies; perfectly optimized for Mimir's use case; no compatibility
constraints with franz-go internals; smallest possible binary footprint.
**Cons:** 4-8 weeks of engineering; high risk of subtle protocol bugs; need extensive testing against
both Warpstream and vanilla Kafka; SASL implementation is non-trivial (SCRAM-SHA-256/512 requires
crypto primitives, OAuth requires HTTP token refresh).

---

## 8. Complexity Analysis

### Protocol-level complexity

| Component | Complexity | Notes |
|-----------|-----------|-------|
| TCP connection | Low | `net.Dial`, straightforward |
| TLS | Low | `crypto/tls` standard library |
| SASL/PLAIN | Low | Simple username:password exchange |
| SASL/SCRAM | Medium | Requires PBKDF2, HMAC, base64 challenge-response |
| SASL/OAuth | High | Requires HTTP token fetch, expiry refresh |
| Wire framing | Low | `binary.BigEndian` + length prefix |
| Request pipelining | Medium | Correlation ID tracking, response demux |
| ApiVersions | Low | Single request on connect |
| Metadata API | Medium | Parse broker list, handle updates |
| Produce API | Medium | RecordBatch encoding, CRC-32C, varint |
| Compression (Snappy) | Low-Medium | Compress record batch payload with `klauspost/compress/s2` (already vendored) |
| Error handling | Medium | Error codes, retries with backoff |
| Linger buffer | Medium-High | Per-partition accumulation, timer management, completion callbacks, flusher goroutine |
| Hedging logic | Low-Medium | Timer + goroutine fan-out at ProduceRequest level |
| Testing | High | Need real Warpstream + Kafka environments |

### What SASL mechanism is used in Grafana Cloud?

This significantly impacts Option C's complexity:
- **PLAIN**: Easy to implement (20 lines)
- **SCRAM-SHA-256/512**: Medium (100-200 lines, crypto primitives in stdlib)
- **OAuth**: Complex (requires HTTP client, token refresh, expiry handling)

### Record format complexity

Mimir already serializes WriteRequests into Kafka record bytes in `version.go`. The custom client
only needs to **wrap** those bytes into a Kafka RecordBatch frame. The application-level serialization
is already done; what's left is only the transport-level framing.

The RecordBatch format with magic=2 requires:
1. CRC-32C over a specific byte range (trivial with Go stdlib)
2. Varint encoding for per-record fields (trivial)
3. Timestamp management (use `time.Now().UnixMilli()`)
4. `producerId=-1`, `producerEpoch=-1`, `baseSequence=-1` (no idempotence)

---

## 9. Key Technical Findings

1. **Any Warpstream agent can handle any partition** — this is the fundamental enabler. The
   stateless architecture means we don't need leader routing.

2. **Metadata already returns multiple agents** — franz-go today connects to multiple Warpstream
   agents (one per "broker" in the Metadata response). The client just doesn't hedge between them.

3. **Mimir doesn't need idempotence** — `DisableIdempotentWrite()` is already set. This simplifies
   the Produce request significantly (no `producerId`/`producerEpoch` negotiation).

4. **Mimir doesn't need sequencing** — duplicate records from hedging are explicitly tolerable.

5. **franz-go kmsg package exists** — the protocol encoding/decoding is available as a standalone
   package, making Option B viable without full from-scratch reimplementation.

6. **Production Warpstream config is minimal** — the only Warpstream-specific flag is
   `use-compressed-bytes-as-fetch-max-bytes=false`. Everything else is generic Kafka config.

7. **Existing TLS/SASL config** — Mimir already has well-tested TLS + SASL configuration in
   `util.go` lines 182-212. Whatever approach is used, the configuration surface should be reused.

---

## 9. Recommendation Sketch

**Recommended: `Broker.Request()` on top of a single kgo.Client (Section 6).**

This approach reuses franz-go's entire network layer (TLS, SASL, wire framing, pipelining,
reconnection, Metadata management) while giving us full control over broker selection, linger
batching, and hedging. Estimated at ~800–1,200 lines of new Go code vs. 2,000–3,000 for Option B
or 4,000+ for Option C. All SASL mechanisms Mimir already supports come for free.

The components we write from scratch are the same regardless of approach — linger buffer, hedging,
secondary selection — but the network layer is now a thin wrapper rather than a reimplementation.

**Option A** (multiple franz-go clients) remains a valid proof-of-concept path if we want to
validate hedging quickly before committing to the linger buffer implementation.

**Option B** (kmsg + custom connections) and **Option C** (full custom) are now clearly worse
tradeoffs: significantly more engineering effort with no meaningful benefit over
`Broker.Request()` for this use case.

---

## 10. Component Design and Package Structure

### Guiding principles

1. **Isolated package** — lives at `pkg/warpstreamclient/`, imports nothing from
   `github.com/grafana/mimir/pkg/...`. Only external deps: franz-go, stdlib, `go-kit/log`,
   `prometheus/client_golang`. Follows the pattern of `pkg/vault/` (another self-contained package
   in Mimir with no internal Mimir deps).
2. **Component isolation** — each component has a narrow, interface-based dependency on its
   neighbours so it can be instantiated and tested independently.
3. **Single testability seam** — the `DirectProducer` interface is the only boundary between our logic
   and the real Kafka network. Every component above it can be fully tested with a `mockSender`
   and no real broker.
4. **Follow Mimir patterns** — `promauto.With(reg)` for metrics, `RegisterFlagsWithPrefix` for
   config, manual mock structs in test files (same pattern as `pkg/vault/vault_test.go`).

---

### Package layout

```
pkg/warpstreamclient/
├── client.go        WarpstreamClient — top-level, wires all components, ProduceSync entry point
├── config.go        Config struct + RegisterFlagsWithPrefix
├── direct_producer.go        DirectProducer interface + KafkaDirectProducer (the only kgo.Client boundary)
├── agentpool.go     AgentPool (Metadata refresh, builds DefaultPartitionAssignmentStrategy snapshots)
├── partition_assignment.go   PartitionAssignmentStrategy interface + DefaultPartitionAssignmentStrategy (immutable selection)
├── linger.go        RecordBuffer (per-partition batching, timer, flush triggers)
├── hedger.go        Hedger (concurrent DirectProducer calls, hedge-on-delay pattern)
├── produce.go       buildProduceRequest + parseProduceResponse (pure functions)
└── metrics.go       metrics struct (Prometheus counters / histograms)
```

Test files mirror the above; each component gets its own `_test.go`.

---

### Component: `direct_producer.go`

The single seam between custom logic and the franz-go network stack. **This is the only file
that imports `kgo.Client`.**

```go
// DirectProducer produces to a specific Warpstream agent by its Kafka NodeID.
// Implementations must be safe for concurrent use.
type DirectProducer interface {
    Produce(ctx context.Context, nodeID int32, req *kmsg.ProduceRequest) (*kmsg.ProduceResponse, error)
}

// KafkaDirectProducer implements DirectProducer using kgo.Client.Broker().RetriableRequest().
type KafkaDirectProducer struct{ client *kgo.Client }

func NewKafkaDirectProducer(client *kgo.Client) *KafkaDirectProducer
func (s *KafkaDirectProducer) Produce(ctx context.Context, nodeID int32, req *kmsg.ProduceRequest) (*kmsg.ProduceResponse, error)
```

**Test mock** (`mockDirectProducer`): per-nodeID configurable delays, errors, and a per-nodeID
block channel for deterministic ordering in hedger tests. Records timestamps and final error on
each call so tests can assert ordering and outcomes without time-based flakiness.

---

### Components: `agentpool.go` + `partition_assignment.go`

Two files with a single responsibility each:

- **`agentpool.go`** — `AgentPool` is pure discovery: refreshes Metadata via the kgo.Client and
  produces a new `DefaultPartitionAssignmentStrategy` snapshot on every `Refresh()`.
- **`partition_assignment.go`** — `PartitionAssignmentStrategy` interface +
  `DefaultPartitionAssignmentStrategy`: immutable selection logic on a fixed agent snapshot.
  No locks on the hot path (uses `sync.Map` for write-once-read-many secondary caching).

```go
// partition_assignment.go

// PartitionAssignmentStrategy selects the primary and secondary agent for a given partition.
type PartitionAssignmentStrategy interface {
    Primary(topic string, partition int32) int32
    // Secondary returns (0, false) if no secondary is available.
    Secondary(topic string, partition int32) (nodeID int32, ok bool)
}

// DefaultPartitionAssignmentStrategy is immutable once created and safe for concurrent use.
// A new instance is created by AgentPool.Refresh on every call.
type DefaultPartitionAssignmentStrategy struct {
    agents    []int32                  // sorted ascending, immutable
    leaders   map[topicPartition]int32 // immutable
    secondary sync.Map                 // topicPartition → int32 (noSecondary=-1 if unavailable)
}

// selectSecondary picks a secondary deterministically using xxhash(topic, partition)
// over the sorted candidate list (all agents minus primary). Alloc-free walk.
func selectSecondary(topic string, partition int32, primary int32, all []int32) (int32, bool)
```

```go
// agentpool.go

type AgentPool struct {
    client   *kgo.Client
    topic    string
    mu       sync.RWMutex
    topicID  [16]byte
    agents   []int32 // sorted, used to detect removed agents across refreshes
    strategy atomic.Pointer[DefaultPartitionAssignmentStrategy]
}

func NewAgentPool(client *kgo.Client) *AgentPool

// Refresh uses kgo.Client.RequestCachedMetadata which serves from kgo's internal cache when
// fresh (within MetadataMinAge), avoiding duplicate network requests. Returns removed NodeIDs
// so callers can purge per-agent state (e.g. LatencyTracker). Always creates a new
// DefaultPartitionAssignmentStrategy from the current snapshot.
func (p *AgentPool) Refresh(ctx context.Context) (removed []int32, err error)

func (p *AgentPool) Strategy() PartitionAssignmentStrategy
func (p *AgentPool) TopicID(topic string) [16]byte
func (p *AgentPool) All() []int32
```

A `mockPartitionAssignmentStrategy` test double will be added when the Hedger and RecordBuffer
tests need it.

---

### Components: stats tracker + tracking producer + hedger

The hedging layer is split across four small composable units rather than a single `hedger.go`
(see Section 11 for full architecture):

- **`agent_stats_tracker.go`** — `AgentStatsTracker` interface (`TrackAgentRequest`,
  `PurgeAgents`, `AgentStats(now, nodeID)`, `ClusterStats(now, slowMultiplier, faultyThreshold)`),
  with `AverageAgentStatsTracker` as the default bucketed sliding-window implementation
  (6 × 10s buckets). Exposes `AgentStats { Latency, ErrorRate, RequestCount }` and
  `ClusterStats { BaselineLatency, SlowThreshold, SlowFraction, SlowContributorsCount,
  BaselineErrorRate, FaultyThreshold, FaultyFraction, FaultyContributorsCount }`.
- **`cached_agent_stats_tracker.go`** — caches `ClusterStats` by
  `(slowMultiplier, faultyThreshold)` for a short TTL (avoids re-running the O(agents) gather
  on every call). `AgentStats` and `TrackAgentRequest` forward live; `PurgeAgents` invalidates
  the cache.
- **`tracking_producer.go`** — a `DirectProducer` decorator that records every Produce call's
  outcome into the tracker. Wired between the Hedger and the network leaf
  (`Hedger(TrackingProducer(KafkaDirectProducer))`) so every leg the hedger dispatches —
  primary, hedge, fanout sub-leg, abandoned race loser — is observed exactly once. The Hedger
  does not write to the tracker itself.
- **`hedger.go`** — orchestrator. Implements `DirectProducer`. Decides per-call whether to
  speculatively hedge (`shouldHedge`), and on failure or hedge-timer expiry fans the request
  out across per-partition secondaries (`fanoutToSecondaryAgents`). The fanout is
  all-or-nothing.
- **`produce_split_merge.go`** — pure helpers consumed by the Hedger:
  `splitProduceRequestToSecondaryAgents` (groups partitions by per-partition secondary,
  errors out if any partition lacks one) and `mergeProduceResponses` (concatenates per-topic
  partitions; `ThrottleMillis` is the max).

Notable design properties:

- **Read/write split**: the Hedger reads `AgentStats`/`ClusterStats` for the hedge decision;
  the `TrackingProducer` writes per-leg observations. The two roles live at different layers.
- **Scale-aware fraction floor**: `MaxSlowFraction` and `MaxFaultyFraction` are compared
  against `max(configured, 1/contributors)` via `maxFractionFloor`, so a single bad agent
  never trips suppression regardless of cluster size. At large N the configured value
  dominates; at small N (e.g. 2 agents, 1 faulty → fraction 0.5) the floor opens the gate so
  hedging to the only healthy agent isn't blocked.
- **Fanout result handling**: any sub-leg success returns immediately; if both primary and
  fanout fail only the primary's error is returned (the fanout error is discarded — primary
  is the source of truth, the secondary's error is observable via `TrackingProducer`
  per-agent metrics).

```go
type Hedger struct {
    inner    DirectProducer
    tracker  AgentStatsTracker
    strategy PartitionAssignmentStrategy
    cfg      HedgerConfig
    metrics  *metrics
}

type HedgerConfig struct {
    SlowMultiplier    float64
    MaxSlowFraction   float64
    FaultyThreshold   float64
    MaxFaultyFraction float64
    MinHedgeDelay     time.Duration
}

func NewHedger(inner DirectProducer, tracker AgentStatsTracker, strategy PartitionAssignmentStrategy, cfg HedgerConfig, m *metrics) *Hedger

func (h *Hedger) Produce(ctx context.Context, primaryID int32, req *kmsg.ProduceRequest) (*kmsg.ProduceResponse, error)
```

**Test cases (all with `mockDirectProducer` + `mockPartitionAssignmentStrategy`):**
- No hedge decision → only primary called.
- Primary slow + healthy secondaries → hedge fires; secondary wins the race.
- Primary fails before hedge timer → per-partition fanout retry succeeds.
- Hedge timer fires + secondary wins → `hedgeAttemptsTotal` and `hedgeWinsTotal` incremented.
- Hedge decided but primary wins before timer → no `hedgeAttemptsTotal` increment.
- Multi-partition primary error → fanout splits across per-partition secondaries; merged response.
- Single-agent cluster (no secondaries) → primary error propagates unchanged.
- Both legs fail → returns only the primary error.

---

### Component: `linger.go`

Accumulates records per `(topic, partition)` and triggers flushes. Depends only on
`PartitionAssignmentStrategy` (interface) and a `FlushFunc` callback — no Kafka or network knowledge.

```go
// FlushFunc is called when a batch for one agent is ready to send.
// It must call done(err) exactly once per record after the batch completes.
type FlushFunc func(ctx context.Context, nodeID int32, records []*kgo.Record, done func(error))

// RecordBuffer accumulates records per (topic, partition).
type RecordBuffer struct {
    linger        time.Duration
    maxBatchBytes int32
    selector      PartitionAssignmentStrategy
    flush         FlushFunc
    mu            sync.Mutex
    bufs          map[topicPartition]*partitionBuf
}

type partitionBuf struct {
    records   []*kgo.Record
    wireBytes int32
    callbacks []func(error) // one per Add caller contributing to this batch
    timer     *time.Timer
}

func NewRecordBuffer(linger time.Duration, maxBatchBytes int32, selector PartitionAssignmentStrategy, flush FlushFunc) *RecordBuffer

// Add buffers records. If sync=true the batch flushes immediately (bypassing the
// timer), same as franz-go's ProduceSync behaviour.
func (b *RecordBuffer) Add(ctx context.Context, records []*kgo.Record, sync bool, done func(error))
func (b *RecordBuffer) Close()
```

**Flush triggers:**

| Trigger | Behaviour |
|---------|-----------|
| Linger timer fires | Flush all partitions whose timer has expired |
| Batch reaches `maxBatchBytes` | Flush that partition immediately, start new batch |
| `sync=true` (ProduceSync caller) | Flush all affected partitions immediately |
| `Close()` | Flush everything; block until all callbacks fired |

**Test approach:** inject `mockPartitionAssignmentStrategy` + a `FlushFunc` that immediately calls `done(nil)`.
Verify batching, timer, and immediate-flush behaviour without any network calls.

---

### Component: `produce.go`

Pure functions — no state, no interfaces, no mocks needed. Entirely tested with table-driven
unit tests.

```go
// buildProduceRequest constructs a kmsg.ProduceRequest for records targeting one topic.
// Records are grouped per partition into RecordBatches (magic=2, Snappy, no idempotence).
func buildProduceRequest(topic string, records []*kgo.Record, version int16) *kmsg.ProduceRequest

// parseProduceResponse inspects a ProduceResponse for per-partition errors.
func parseProduceResponse(resp *kmsg.ProduceResponse) error
```

---

### Component: `client.go`

Top-level entry point. Wires all components together.

```go
type WarpstreamClient struct {
    pool    *AgentPool
    buffer  *RecordBuffer
    hedger  *Hedger
    producer  *KafkaDirectProducer
    metrics *metrics
    logger  log.Logger
}

func NewWarpstreamClient(cfg Config, logger log.Logger, reg prometheus.Registerer) (*WarpstreamClient, error)

// ProduceSync produces records and blocks until all are durably stored or ctx expires.
func (c *WarpstreamClient) ProduceSync(ctx context.Context, records []*kgo.Record) kgo.ProduceResults

func (c *WarpstreamClient) Close() error
```

`ProduceSync` calls `buffer.Add(..., sync=true, done)` and waits for `done`.

The `FlushFunc` passed to `RecordBuffer` closes over `hedger` and `pool`:
```go
flush := func(ctx context.Context, nodeID int32, records []*kgo.Record, done func(error)) {
    req := buildProduceRequest(topic, records, produceVersion)
    secondary := pool.Secondary(topic, partition)
    err := hedger.Produce(ctx, nodeID, secondary, req)
    done(err)
}
```

---

### Integration with `pkg/storage/ingest` (one-way import)

`pkg/warpstreamclient` has zero Mimir imports. The ingest package imports it and adapts it
to the existing `kafkaProducer` interface:

```go
// In pkg/storage/ingest/writer_client.go:
import wsc "github.com/grafana/mimir/pkg/warpstreamclient"

type warpstreamKafkaProducer struct{ c *wsc.WarpstreamClient }

func (w *warpstreamKafkaProducer) ProduceSync(ctx context.Context, records []*kgo.Record) kgo.ProduceResults {
    return w.c.ProduceSync(ctx, records)
}
func (w *warpstreamKafkaProducer) Close() { _ = w.c.Close() }
```

---

### Dependency graph (no cycles)

```
pkg/warpstreamclient/
  client.go
    ├── direct_producer.go     → kgo.Client (franz-go)
    ├── agentpool.go  → kgo.Client (franz-go) + PartitionAssignmentStrategy interface
    ├── linger.go     → PartitionAssignmentStrategy interface + FlushFunc
    ├── hedger.go     → Sender interface
    ├── produce.go    → kmsg (franz-go) — pure functions
    └── metrics.go    → prometheus

pkg/storage/ingest/writer_client.go
    └── imports pkg/warpstreamclient   (one-way)
```

### Testability matrix

| Component | Needs real Kafka | Mocks required |
|-----------|-----------------|----------------|
| `produce.go` | No | None |
| `hedger.go` | No | `mockSender` |
| `agentpool.go` | No (fake Metadata resp) | None |
| `linger.go` | No | `mockPartitionAssignmentStrategy` |
| `client.go` integration | No | `mockSender` + `mockPartitionAssignmentStrategy` |

---

## 11. Confirmed Design Decisions

Answers from discussion:

| Question | Answer | Implication |
|----------|--------|-------------|
| Agent topology | Single LB seed; Metadata returns individual agent addresses as partition "leaders" | LB only used for initial Metadata bootstrap. Agent pool = full broker list from Metadata. Partition→leader assignment is Kafka protocol compliance only — any agent handles any partition. |
| Hedging vs edging | Same concept (hedging) | Classic two-wave send: primary first, hedge after configurable delay |
| Scope | Produce-only, Warpstream-only | No Fetch/Admin APIs needed. franz-go stays for ingesters. Local dev keeps franz-go. |
| Build approach | kmsg + custom connections | Use `pkg/kmsg` for protocol encoding; custom TCP/TLS/SASL/pipelining layer |

---

## 11. Recommended Implementation: Broker.Request() + Custom Linger/Hedging

### Architecture overview

```
┌─────────────────────────────────────────────────────┐
│               WarpstreamProducer                     │
│  - kgo.Client (connections, TLS, SASL, Metadata)     │
│  - lingerBuffer (per-partition accumulation)         │
│      map[(topic,partition)] → partitionBuffer        │
│      partitionBuffer: records, timer, callbacks      │
│  - Flusher goroutines (one per agent / NodeID)       │
│      groups ready partitions → kmsg.ProduceRequest   │
│      calls Broker.RetriableRequest() on primary      │
│      hedges to secondary Broker after delay          │
└─────────────────────────────────────────────────────┘
         ↓ kgo.Broker(nodeID).RetriableRequest(ctx, req)
  franz-go handles: TLS, SASL, wire framing, pipelining
```

### Component breakdown

**1. kgo.Client (network layer — entirely from franz-go)**

The `kgo.Client` is initialised with a minimal set of options (no produce machinery, just connection
management and Metadata). All per-broker TCP connections, TLS handshakes, SASL authentication,
reconnection, and request pipelining are handled internally by franz-go. We never touch a `net.Conn`
directly.

**2. Agent pool**

```go
// On startup and on periodic refresh:
brokers := client.DiscoveredBrokers()   // []*kgo.Broker, one per agent in Metadata
// Sorted by NodeID for stable secondary selection ordering.
```

**3. Produce call (via Broker.RetriableRequest)**

```go
func sendToAgent(ctx context.Context, b *kgo.Broker, req *kmsg.ProduceRequest) error {
    resp, err := b.RetriableRequest(ctx, req)
    if err != nil {
        return err
    }
    return parseProduceResponse(resp.(*kmsg.ProduceResponse))
}
```

franz-go rewrites `req.Acks` and `req.TimeoutMillis` to match the client config before sending —
this is correct as long as the client is configured with `AllISRAcks` and the right timeout.

**4. Hedging in WarpstreamProducer**

```go
func (p *WarpstreamProducer) ProduceSync(ctx, records []*Record) error {
    agents := p.agentPool.pick(2)   // primary + hedge candidate
    type result struct{ err error }
    ch := make(chan result, 2)

    // Primary produce
    go func() {
        ch <- result{agents[0].produce(ctx, buildProduceReq(records))}
    }()

    // Hedge after delay
    timer := time.NewTimer(p.hedgeDelay)
    defer timer.Stop()

    select {
    case res := <-ch:
        return res.err  // primary replied before hedge delay
    case <-timer.C:
        go func() {
            ch <- result{agents[1].produce(ctx, buildProduceReq(records))}
        }()
    }

    // Return first success (or last error if both fail)
    res1 := <-ch
    if res1.err == nil { return nil }
    res2 := <-ch
    return res2.err
}
```

**5. Agent pool management**

On startup: connect to the LB seed address, send `MetadataRequest`, parse broker list. Each broker
entry becomes an `agentConn`. Refresh the agent pool periodically and on error; the set of agents
may change as Warpstream scales up or down.

Agent selection: primary is the broker designated as partition leader in the Metadata response.
Secondary is selected by deterministic hash of `(topic, partitionID)` over the remaining sorted
broker list (see Section 5 for the algorithm).

### What franz-go provides vs what we write

| Component | Owner |
|-----------|-------|
| TCP connection lifecycle, reconnection, idle timeout | **franz-go** (`kgo.Client`) |
| TLS handshake | **franz-go** |
| SASL (PLAIN, SCRAM-SHA-256/512, OAuth, MSK IAM — all mechanisms) | **franz-go** |
| Kafka wire framing (request headers, length prefix, big-endian) | **franz-go** |
| Request pipelining + correlation ID tracking per broker | **franz-go** |
| ApiVersions negotiation on connect | **franz-go** |
| Metadata refresh and broker discovery | **franz-go** |
| `ProduceRequest`/`ProduceResponse` encoding (`kmsg` via `AppendTo`/`ReadFrom`) | **kmsg** (franz-go dep) |
| RecordBatch encoding (magic=2, CRC-32C, Snappy compression) | **kmsg** |
| Agent pool (derive from `DiscoveredBrokers()`) | **We write** (trivial) |
| Primary/secondary NodeID selection | **We write** |
| `kmsg.ProduceRequest` construction from `kgo.Record` slice | **We write** |
| Linger buffer (per-partition accumulation, timer, callbacks, flusher) | **We write** |
| Hedging logic (concurrent `Broker.RetriableRequest()` calls) | **We write** |
| ProduceResponse parsing + per-partition error handling | **We write** |
| Prometheus metrics | **We write** |

**Estimated implementation size:** ~800–1,200 lines of Go (excluding tests). The linger buffer remains the most complex single component; the network layer is entirely eliminated.

### What we do NOT need to implement

- Fetch API (consumer side stays on franz-go)
- Consumer group APIs (JoinGroup, SyncGroup, Heartbeat, etc.)
- Admin APIs (CreateTopics, etc.)
- Idempotent/transactional producer (InitProducerId, etc.)
- Multi-codec compression (only Snappy needed, using `klauspost/compress/s2` already in vendor)
- Auto-partitioning (Mimir manually assigns partitions, same as today)
- Vanilla Kafka compatibility fallback (local dev uses franz-go as today)

### Configuration surface (additions to existing KafkaConfig)

```yaml
# Existing flags reused as-is:
ingest-storage.kafka.address               # seed address (LB)
ingest-storage.kafka.tls-enabled
ingest-storage.kafka.sasl-mechanism
ingest-storage.kafka.sasl-username
ingest-storage.kafka.sasl-password
ingest-storage.kafka.write-timeout

# New flags:
ingest-storage.kafka.produce-hedge-delay   # time before sending hedge (default: 50ms, 0=disabled)
ingest-storage.kafka.produce-max-agents    # max agents from Metadata to maintain connections to (default: all)
# Reused unchanged:
# ingest-storage.kafka.producer-linger-disable  # sets linger=0 when true (already exists)
```

---

## 12. Confirmed Additional Parameters

| Parameter | Answer | Implication |
|-----------|--------|-------------|
| SASL mechanism | **PLAIN** | ~20 lines of implementation. Low risk. |
| Maturity target | **Production directly** | Full error handling, observability, tests from day one |
| Integration | **Same interface** | Extract a `kafkaProducer` interface from `KafkaProducer`; both implement it |

---

## 13. Integration Design

### Config option: selecting the Kafka backend

A new `Backend` field is added to `KafkaConfig` (`pkg/storage/ingest/config.go`):

```go
const (
    KafkaBackendKafka       = "kafka"
    KafkaBackendWarpstream  = "warpstream"
)

var kafkaBackendOptions = []string{KafkaBackendKafka, KafkaBackendWarpstream}

// KafkaConfig (existing struct, add one field):
type KafkaConfig struct {
    // ... existing fields unchanged ...

    // Backend selects the Kafka producer implementation.
    // "kafka" uses the franz-go client (default).
    // "warpstream" uses the custom Warpstream-optimized producer (pkg/warpstreamclient).
    Backend string `yaml:"backend"`

    // WarpstreamConfig holds settings for the Warpstream producer backend.
    // Only used when Backend is "warpstream".
    WarpstreamConfig warpstreamclient.Config `yaml:"warpstream"`
}
```

Flag registration (in `KafkaConfig.RegisterFlagsWithPrefix`):

```go
f.StringVar(&cfg.Backend, prefix+"backend", KafkaBackendKafka,
    fmt.Sprintf("The Kafka producer backend. Supported values: %s.",
        strings.Join(kafkaBackendOptions, ", ")))
cfg.WarpstreamConfig.RegisterFlagsWithPrefix(prefix+"warpstream-", f)
```

This produces:
- `-ingest-storage.kafka.backend` — `"kafka"` (default) or `"warpstream"`
- `-ingest-storage.kafka.warpstream-hedge-delay` — hedge delay (e.g. `50ms`)
- `-ingest-storage.kafka.warpstream-max-agents` — max agents to maintain connections to

Validation (in `KafkaConfig.Validate()`):

```go
var ErrInvalidKafkaBackend = fmt.Errorf(
    "the configured Kafka backend is invalid, must be one of: %s",
    strings.Join(kafkaBackendOptions, ", "))

if !slices.Contains(kafkaBackendOptions, cfg.Backend) {
    return ErrInvalidKafkaBackend
}
if cfg.Backend == KafkaBackendWarpstream {
    if err := cfg.WarpstreamConfig.Validate(); err != nil {
        return err
    }
}
```

---

### kafkaProducer interface (new, in `writer_client.go`)

`Writer.client` currently holds `atomic.Pointer[KafkaProducer]` (a concrete type). Changing it to
hold an interface enables both backends transparently:

```go
// kafkaProducer is the internal interface satisfied by both KafkaProducer (franz-go)
// and the warpstream backend adapter.
type kafkaProducer interface {
    ProduceSync(ctx context.Context, records []*kgo.Record) kgo.ProduceResults
    Close()
}
```

`KafkaProducer` already satisfies this interface (its `ProduceSync` and `Close` methods match).

`Writer` change (`writer.go`):

```go
type Writer struct {
    // client was atomic.Pointer[KafkaProducer]; now holds the interface.
    client atomic.Pointer[kafkaProducer]
    ...
}
```

`atomic.Pointer[kafkaProducer]` stores a `*kafkaProducer` (pointer to interface value). Usage:
```go
// Store:
var p kafkaProducer = &KafkaProducer{...}
w.client.Store(&p)

// Load:
if p := w.client.Load(); p != nil {
    (*p).ProduceSync(ctx, records)
}
```

---

### Writer.starting(): backend selection

```go
func (w *Writer) starting(_ context.Context) error {
    if w.kafkaCfg.AutoCreateTopicEnabled {
        if err := CreateTopic(w.kafkaCfg, w.logger); err != nil {
            return err
        }
    }

    clientReg := prometheus.WrapRegistererWithPrefix(writerMetricsPrefix, w.registerer)

    var (
        producer kafkaProducer
        err      error
    )
    switch w.kafkaCfg.Backend {
    case KafkaBackendWarpstream:
        producer, err = newWarpstreamProducer(w.kafkaCfg, w.logger, clientReg)
    default: // KafkaBackendKafka
        producer, err = newGenericProducer(w.kafkaCfg, w.logger, clientReg)
    }
    if err != nil {
        return err
    }

    w.client.Store(&producer)
    return nil
}

func newGenericProducer(cfg KafkaConfig, logger log.Logger, reg prometheus.Registerer) (kafkaProducer, error) {
    maxInflight := cfg.MaxInflightProduceRequests
    if maxInflight == 0 {
        maxInflight = defaultMaxInflightProduceRequests
    }
    client, err := NewKafkaWriterClient(cfg, maxInflight, logger, reg, WithDisableDefaultTopic())
    if err != nil {
        return nil, err
    }
    return NewKafkaProducer(client, cfg.ProducerMaxBufferedBytes, reg), nil
}

func newWarpstreamProducer(cfg KafkaConfig, logger log.Logger, reg prometheus.Registerer) (kafkaProducer, error) {
    c, err := warpstreamclient.NewWarpstreamClient(cfg.WarpstreamConfig, cfg, logger, reg)
    if err != nil {
        return nil, err
    }
    return &warpstreamAdapter{c: c}, nil
}
```

---

### Adapter (in `writer_client.go`)

`warpstreamclient.WarpstreamClient` does not import from `pkg/storage/ingest`. The adapter bridges
the gap:

```go
// warpstreamAdapter adapts *warpstreamclient.WarpstreamClient to the kafkaProducer interface.
type warpstreamAdapter struct {
    c *warpstreamclient.WarpstreamClient
}

func (a *warpstreamAdapter) ProduceSync(ctx context.Context, records []*kgo.Record) kgo.ProduceResults {
    return a.c.ProduceSync(ctx, records)
}

func (a *warpstreamAdapter) Close() {
    _ = a.c.Close()
}
```

---

### `warpstreamclient.NewWarpstreamClient` signature

The constructor takes both the Warpstream-specific config and the `KafkaConfig` (for connection
parameters: address, TLS, SASL, timeouts). To avoid importing `pkg/storage/ingest` from
`pkg/warpstreamclient`, the connection parameters are passed as a separate `ConnectionConfig`
value (or the `KafkaConfig` fields are mapped onto the warpstreamclient's own config type before
the call):

```go
// In pkg/warpstreamclient/config.go:
type Config struct {
    // Warpstream-specific settings:
    HedgeDelay time.Duration `yaml:"hedge_delay"`
    MaxAgents  int           `yaml:"max_agents"`

    // Connection settings (populated from KafkaConfig by the ingest package):
    Address      []string
    DialTimeout  time.Duration
    WriteTimeout time.Duration
    TLS          tls.Config   // stdlib tls.Config, no Mimir dep
    SASLMechanism string
    SASLUsername  string
    SASLPassword  string
    ClientID      string
    Topic         string
}

func NewWarpstreamClient(cfg Config, logger log.Logger, reg prometheus.Registerer) (*WarpstreamClient, error)
```

The ingest package maps `KafkaConfig` → `warpstreamclient.Config` in `newWarpstreamProducer()`
before calling `NewWarpstreamClient`. This keeps the dependency one-way (ingest → warpstreamclient).

---

### Metrics parity

The existing `KafkaProducer` exposes rich metrics. The `WarpstreamClient` should expose equivalent
metrics plus new hedging-specific ones:

| Metric | franz-go backend | Warpstream backend |
|--------|------------------|--------------------|
| `buffered_produce_bytes_distribution` (summary) | ✓ | ✓ |
| `buffered_produce_bytes_limit` (gauge) | ✓ | ✓ |
| `produce_records_enqueued_total` | ✓ | ✓ |
| `produce_records_failed_total{reason}` | ✓ | ✓ |
| `produce_records_enqueue_duration_seconds` | ✓ | ✓ |
| `produce_remaining_deadline_seconds` | ✓ | ✓ |
| `produce_hedge_attempts_total` | — | NEW |
| `produce_hedge_wins_total` | — | NEW |
| `produce_agent_latency_seconds{agent}` | — | NEW |

All metrics are registered under the same `writerMetricsPrefix` prefix via the `reg` passed to the
constructor, so dashboards and alerts need no changes for the shared metrics.

---

## 14. Implementation Scope Summary

### Files changed in `pkg/storage/ingest/`

| File | Change |
|------|--------|
| `config.go` | Add `Backend string`, `WarpstreamConfig warpstreamclient.Config` to `KafkaConfig`; add `ErrInvalidKafkaBackend`; update `Validate()` |
| `writer.go` | Change `client atomic.Pointer[KafkaProducer]` → `atomic.Pointer[kafkaProducer]`; update `starting()` and `stopping()` |
| `writer_client.go` | Add `kafkaProducer` interface; add `warpstreamAdapter` struct; extract `newGenericProducer()` helper |

### New package `pkg/warpstreamclient/`

| File | Contents |
|------|----------|
| `config.go` | `Config` struct + `RegisterFlagsWithPrefix` + `Validate` |
| `direct_producer.go` | `DirectProducer` interface + `KafkaSender` (only kgo.Client boundary) |
| `agentpool.go` | `PartitionAssignmentStrategy` interface + `AgentPool` + `selectSecondary` |
| `linger.go` | `RecordBuffer` + `partitionBuf` + `FlushFunc` |
| `hedger.go` | `Hedger` |
| `produce.go` | `buildProduceRequest` + `parseProduceResponse` (pure functions) |
| `metrics.go` | `metrics` struct |
| `client.go` | `WarpstreamClient` + `NewWarpstreamClient` |

### Config surface (new flags only)

```
-ingest-storage.kafka.backend                      "kafka" or "warpstream" (default: "kafka")
-ingest-storage.kafka.warpstream-hedge-delay       hedge delay (default: 50ms; 0 = disabled)
-ingest-storage.kafka.warpstream-max-agents        max agents to connect to (default: 0 = all)
```

### Out of scope

- Fetch API and consumer path — stay on franz-go
- Consumer group and admin APIs
- Multi-codec compression — only Snappy (already used by franz-go default)
- Vanilla Kafka compatibility — local dev keeps using `-ingest-storage.kafka.backend=kafka`
- SASL mechanisms beyond those already handled by franz-go (all are covered transparently)

---

## 15. Key Risks and Mitigations

| Risk | Likelihood | Mitigation |
|------|-----------|------------|
| Warpstream Metadata response format changes | Low | Read kmsg response carefully; add test coverage |
| SASL PLAIN credentials in wire format incorrect | Low | Compare with franz-go's SASL PLAIN impl in `kgo/sasl.go` |
| Request pipelining bugs (correlation ID mismatches) | Medium | Unit test with concurrent requests, fuzz test |
| Agent connection flapping causes cascading failures | Medium | Exponential backoff on reconnect; health tracking before routing |
| Hedging causes significant duplicate load | Low | Hedge delay configurable; metrics on hedge win rate allow tuning |
| kmsg API changes break compilation | Low | Pin franz-go version; kmsg is stable |
| TCP writes from multiple goroutines race | Medium | Mutex on write (one write per correlationID; reads are concurrent) |

---

## 16. Open Questions (Resolved)

All open questions from initial research have been resolved:

1. ✅ **Agent topology**: Single LB seed; agents discovered via Metadata response
2. ✅ **Hedging semantics**: Classic hedging (primary first, hedge after configurable delay)
3. ✅ **Scope**: Produce-only, Warpstream-only; ingesters keep franz-go
4. ✅ **Build approach**: `Broker.RetriableRequest()` on top of a single `kgo.Client`
5. ✅ **SASL**: Handled transparently by franz-go (all mechanisms)
6. ✅ **Maturity**: Production-quality directly
7. ✅ **Integration**: `kafkaProducer` interface extracted; backend selected via `-ingest-storage.kafka.backend`
8. ✅ **Rack / node diversity**: Not achievable (Warpstream reports `"warpstream-fake-rack"`); secondary selection is agent-diverse by deterministic hash
9. ✅ **Package isolation**: `pkg/warpstreamclient/` with zero Mimir internal imports in production code; test files may import `pkg/util/testkafka` for kfake-based integration tests; ingest package is the only production consumer
