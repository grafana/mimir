# warpstreamclient

A produce-only Kafka client tailored to [Warpstream](https://www.warpstream.com/)'s stateless-agent architecture. Drop-in for the `kafkaProducer` interface used by `pkg/storage/ingest`.

## What it is

`WarpstreamClient` produces records to a Kafka-protocol-compatible cluster — specifically Warpstream — using a custom orchestration layer that sits on top of [franz-go](https://github.com/twmb/franz-go). It reuses franz-go for connection management, Metadata refresh, TLS/SASL, and wire encoding (`pkg/kmsg`), but bypasses franz-go's standard producer state machine. The public surface mirrors `kgo.Client`'s producer methods (`Produce`, `ProduceSync`, `Close`), and `WarpstreamClient` embeds `*kgo.Client` so non-producer methods (`Ping`, `MetadataRequest`, …) work transparently.

Consumer-side functionality (Fetch, consumer groups, admin APIs, transactions) is **out of scope**.

## Why we built it

In vanilla Kafka every partition has exactly one leader broker. The Produce protocol requires the client to send each partition's batch to its current leader; sending the same batch to a non-leader broker fails with `NotLeaderForPartition`. This makes **hedging impossible**.

Warpstream is different. Its agents are fully stateless — every agent can serve every partition at any time. The "leader" returned in a Metadata response is a Kafka-protocol concession, not a real routing constraint. Mimir's produce path also tolerates duplicates: writes are at-least-once and have no in-partition sequencing requirement.

So the cost franz-go pays to honor the Kafka protocol — pinning every partition to one specific broker — buys Mimir nothing on Warpstream. We trade that guarantee away and get something more valuable in return: the ability to **race a slow primary against a different agent and accept whichever responds first**, plus the ability to **route away from a sick agent entirely** while it's degraded.

That's the only thing this client does that franz-go can't.

## Non-negotiable principles

These are load-bearing assumptions; the design only works because all of them hold:

1. **At-least-once delivery only.** Duplicates are tolerable. Any code that assumes exactly-once or in-partition record ordering must stay on franz-go.
2. **No transactional or idempotent producer support.** `DisableIdempotentWrite()` semantics are baked in — no `producerId`/`producerEpoch`/`baseSequence` handshake.
3. **Warpstream-specific.** Hedging the same batch across agents only works because any agent can serve any partition. Pointed at vanilla Kafka, the secondary leg would fail with `NotLeaderForPartition`.
4. **Produce-only.** No Fetch, no consumer groups, no admin APIs. Consumer-side traffic continues to use franz-go.
5. **Manual partitioning.** Callers assign partitions explicitly (matching today's Mimir distributor behaviour). The client never auto-partitions.
6. **Background-only Metadata refresh.** Produce requests never block on Metadata; an out-of-date pool view is preferred to an in-flight stall.
7. **Hedging is a load-management decision, not a correctness one.** Hedging adds wire load; the orchestration must keep that surge bounded and gracefully suppress itself under cluster-wide degradation.

## How it works

The client is a small pipeline of independent components, each with a narrow concern. From caller to wire:

```
Produce(record) ──► routing ──► cluster buffer ──► per-agent buffer ──► Hedger ──► DirectProducer ──► kgo broker.Request
                                                  (linger, batch)              (race, retry)        (wire)
```

### Routing: pick a primary per partition

For every record the client asks a `PartitionAssignmentStrategy` for an ordered candidate list. The first entry is the primary, the rest are fallbacks. Two properties matter:

- **Deterministic.** Given the same Metadata view, every client instance picks the same primary and the same secondary for a given partition. Hedge load is predictable and analysable instead of randomly smeared across agents.
- **State-aware.** A wrapper around the base strategy (the **Demoter**, see below) can mark an agent as demoted so it's elided from the candidate list or surfaced as a probe.

### Buffering: linger by destination agent, not by partition

Records are buffered through a **ClusterRecordBuffer**, which bins them by the destination agent picked at routing time, then through a per-agent **AgentRecordBuffer**, which applies a configurable linger window before flushing. Each flush ships one Produce request to one agent carrying batches for as many partitions as the buffer accumulated.

Linger by agent (not by partition, as franz-go does) is what lets a single wire request fan out across many partitions when the load is uneven, and what makes the hedge cascade efficient: a hedge wave sends one request per fallback-agent, not one per partition.

### Hedger: race the primary against a fallback

When a per-agent buffer flushes, the resulting batch goes to the **Hedger**, which decides whether to race the primary against a secondary agent. Per call it produces one of three outcomes:

- **Primary wins outright.** The primary leg returns first with a clean result; we surface it and the secondary never fires.
- **Primary fails, cascade retries.** A leg counts as failed if *any* partition in its response errors (per-leg outcome is all-or-nothing — successful partitions are not credited when a sibling fails). The Hedger walks down the candidate list and re-attempts the unresolved partitions, up to `MaxHedgeAgents` total per partition. Different partitions can land on different agents in the same wave when their candidate orderings diverge.
- **Hedge timer fires first.** The primary is taking longer than expected. The Hedger fires a fallback alongside the in-flight primary; whichever returns first with a usable result wins, and the loser is cancelled.

The hedge decision is **data-driven**, not unconditional. The Hedger consults rolling per-agent latency and error stats (the same window the Demoter uses, so both components agree on "is agent X bad?"). When the primary looks unhealthy the hedge delay is the cluster's baseline latency, so the fallback fires quickly. When the primary looks healthy the delay is multiplied so we don't stampede the cluster during normal operation — but we still hedge, because tail-latency amplification (every application request fans out to all partitions; one slow primary stalls the whole request) is the dominant cost we're trying to avoid.

Two cluster-wide guard rails suppress hedging entirely:
- **Slow-fraction guard.** Too many agents are slower than baseline → hedging would amplify the problem; back off.
- **Faulty-fraction guard.** Too many agents are erroring → ditto.

These guards make the client fail safely under correlated outages instead of multiplying produce traffic just when the cluster can least afford it.

### Demoter: route away from sick agents

The **Demoter** wraps the routing strategy and adds a "skip demoted agents, occasionally probe them" policy. An agent crosses into the demoted set when its rolling error rate clearly exceeds the cluster baseline. While demoted:

- Most routing decisions skip the demoted agent to the next healthy candidate — no traffic, no further damage.
- A small fraction of decisions (governed by a per-agent `ProbeInterval`) are routed back as **probes**: explicit hedge-eligible requests where we already expect the primary to fail, so the Hedger fires the fallback immediately with no delay.

Demotion is driven only by error rate, not by latency. Latency-only demotion has a "stuck slow" failure mode (a demoted agent's window can't recover because it only sees sparse probes) and a cascade mode (demote slow agent → load redistributes → next slowest deteriorates → demote that one → …). Errors recover instantly on the first successful probe, so they're a safer signal.

The Hedger handles the "slow but working" case; the Demoter handles the "really broken" case. Together they cover the spectrum without either component having to decide both.

### Stats: a single rolling-window view shared by both controllers

Every produce attempt (primary or hedge wave) feeds latency and error data into a rolling per-agent stats tracker. Both the Hedger and the Demoter read from the same tracker through the same `HealthCheckConfig`, so they always agree on which agents are slow and which are faulty. The tracker is bucketed (a few seconds per bucket, a small number of buckets total) so a transient blip doesn't trigger immediate intervention and a recovery is reflected quickly.

### Wire layer: franz-go, used as a transport

The bottom layer is a thin **KafkaDirectProducer** that hands a built `ProduceRequest` to `kgo.Client.Broker(id).Request(ctx, req)`. We do not use `kgo.Client.Produce` because that's where franz-go's leader-pinning lives. By dropping into the raw `Broker.Request` path we keep all of franz-go's connection pooling, TLS/SASL, and wire encoding while taking complete control of which broker each request actually goes to.

## Integration tests

The Warpstream client comes with a set of integration tests that attempt to simulate different scenarios. To run them:

```
WARPSTREAM_INTEGRATION=1 go test -v -run 'TestScenario.*' -count=1 -parallel=10 -tags=warpstream_integration ./pkg/warpstreamclient/integration/
```
