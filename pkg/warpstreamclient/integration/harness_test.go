// SPDX-License-Identifier: AGPL-3.0-only

package integration

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/kversion"

	"github.com/grafana/mimir/pkg/util/testkafka"
	"github.com/grafana/mimir/pkg/warpstreamclient"
)

// scenarioHarness owns one kfake cluster per client so the two clients
// never share broker capacity. Each cluster is configured identically (same
// broker count, same per-broker behavior overrides) so the comparison stays
// apples-to-apples. behaviors is the atomically-swappable per-broker
// behavior map; the kfake control function and the latency hooks read
// through it so a single swap atomically retargets every produce path.
type scenarioHarness struct {
	topic         string
	numPartitions int32

	wsCluster *kfake.Cluster
	wsClient  *warpstreamclient.WarpstreamClient
	wsReg     *prometheus.Registry

	kgoCluster *kfake.Cluster
	kgoClient  produceClient

	behaviors *behaviorsSwapper
}

// readWSProduceCounters reads the (primary, hedge) Produce wire-request
// counters from the WarpstreamClient registry. Used by the counter sampler
// to capture deltas at bucket boundaries. Lookup is by metric name, which
// is the public Prometheus contract — renaming a metric on the production
// side is already an externally-breaking change.
func (h *scenarioHarness) readWSProduceCounters() (primary, hedge int64) {
	return gatherCounter(h.wsReg, "produce_requests_primary_total"),
		gatherCounter(h.wsReg, "produce_requests_hedge_total")
}

// gatherCounter returns the value of the named counter from reg, or 0 if
// the metric is not registered (which would be a contract bug — the
// integration test should fail fast if so). The lookup is O(metrics) per
// call but the sampler runs at most once per 10s, so cost is negligible.
func gatherCounter(reg *prometheus.Registry, name string) int64 {
	mfs, err := reg.Gather()
	if err != nil {
		panic(fmt.Errorf("gatherCounter(%q): %w", name, err))
	}
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		var sum float64
		for _, m := range mf.GetMetric() {
			if c := m.GetCounter(); c != nil {
				sum += c.GetValue()
			}
		}
		return int64(sum)
	}
	panic(fmt.Errorf("gatherCounter: metric %q not registered", name))
}

func newScenarioHarness(t *testing.T, initial map[int32]brokerBehavior) *scenarioHarness {
	t.Helper()

	beh := newBehaviorsSwapper(initial)

	wsCluster, wsAddr := testkafka.CreateCluster(t, integrationClusterSize, integrationTopic, testkafka.WithNumBrokers(int(integrationClusterSize)))
	installFailureControl(t, wsCluster, beh.load)
	wsClient, wsReg := newIntegrationWarpstreamClient(t, wsAddr)
	// Latency is injected at KafkaDirectProducer's post-response hook so
	// the TrackingProducer below it observes the simulated latency and
	// feeds the Hedger/Demoter realistic stats. Installed once at
	// construction so the hook is never reinstalled concurrently with
	// in-flight produce traffic. Surge accounting reads Prometheus
	// counters directly — see readWSProduceCounters.
	wsLatency := newLatencyHook(beh.load)
	wsClient.SetTestProduceResponseHook(func(ctx context.Context, nodeID int32, _ *kmsg.ProduceResponse, _ error) {
		wsLatency(ctx, nodeID)
	})

	kgoCluster, kgoAddr := testkafka.CreateCluster(t, integrationClusterSize, integrationTopic, testkafka.WithNumBrokers(int(integrationClusterSize)))
	installFailureControl(t, kgoCluster, beh.load)
	// The kgo baseline uses rec.Partition (not the underlying broker
	// NodeID) to key the latency. Correct for this baseline because
	// vanilla kgo with ManualPartitioner does not hedge or re-route:
	// each partition has one designated leader and records for partition
	// P always go to that leader, so "partition P is slow" and "P's
	// leader is slow" are equivalent from the producer's perspective.
	// Hook-based broker discovery (HookProduceBatchWritten) was
	// considered but its callback fires from a different goroutine than
	// the record promise and may race with concurrent batches.
	kgoLatency := newLatencyHook(beh.load)
	kgoClient := &latencyDelayedKgoClient{
		Client:  newIntegrationKgoClient(t, kgoAddr),
		latency: kgoLatency,
	}

	return &scenarioHarness{
		topic:         integrationTopic,
		numPartitions: integrationClusterSize,
		wsCluster:     wsCluster,
		wsClient:      wsClient,
		wsReg:         wsReg,
		kgoCluster:    kgoCluster,
		kgoClient:     kgoClient,
		behaviors:     beh,
	}
}

// installFailureControl installs the per-broker FAILURE-injection control
// function on the kfake cluster. Latency is NOT applied here: kfake
// serializes requests per broker (each broker's reqCh is processed
// one-at-a-time even with SleepControl), so cluster-side latency would cap
// per-broker throughput at 1/latency. Latency is applied client-side via
// the latency hooks so kfake responds instantly and broker parallelism is
// preserved.
func installFailureControl(t *testing.T, cluster *kfake.Cluster, loadBehaviors func() map[int32]brokerBehavior) {
	t.Helper()
	rng := rand.New(rand.NewPCG(1, 2))
	var rngMu sync.Mutex
	cluster.ControlKey(int16(kmsg.Produce), func(req kmsg.Request) (kmsg.Response, error, bool) {
		cluster.KeepControl()
		nodeID := cluster.CurrentNode()
		b := loadBehaviors()[nodeID]
		shouldFail := b.fail
		if !shouldFail && b.failRate > 0 {
			rngMu.Lock()
			shouldFail = rng.Float64() < b.failRate
			rngMu.Unlock()
		}
		preq := req.(*kmsg.ProduceRequest)
		presp := preq.ResponseKind().(*kmsg.ProduceResponse)
		presp.Version = preq.Version
		var partitionErrCode int16
		if shouldFail {
			partitionErrCode = kerr.NotLeaderForPartition.Code
		}
		for _, rt := range preq.Topics {
			outTopic := rt.Topic
			if outTopic == "" {
				outTopic = integrationTopic
			}
			out := kmsg.ProduceResponseTopic{Topic: outTopic, TopicID: rt.TopicID}
			for _, rp := range rt.Partitions {
				out.Partitions = append(out.Partitions, kmsg.ProduceResponseTopicPartition{
					Partition: rp.Partition,
					ErrorCode: partitionErrCode,
				})
			}
			presp.Topics = append(presp.Topics, out)
		}
		return presp, nil, true
	})
}

// newLatencyHook builds a closure that draws a per-broker latency from the
// currently-active behaviors map (thread-safe via the supplied rng + mutex)
// and sleeps before returning. Used both by the WarpstreamClient (via
// SetTestProduceResponseHook) and by the kgo baseline wrapper.
func newLatencyHook(loadBehaviors func() map[int32]brokerBehavior) func(ctx context.Context, nodeID int32) {
	rng := rand.New(rand.NewPCG(11, 23))
	var rngMu sync.Mutex
	return func(ctx context.Context, nodeID int32) {
		b, ok := loadBehaviors()[nodeID]
		if !ok || b.latencyFn == nil {
			return
		}
		rngMu.Lock()
		d := b.latencyFn(rng)
		rngMu.Unlock()
		if d <= 0 {
			return
		}
		select {
		case <-time.After(d):
		case <-ctx.Done():
		}
	}
}

// latencyDelayedKgoClient wraps a kgo.Client and delays every Produce
// promise by the latency drawn from the per-partition behavior. See the
// comment at the kgoClient construction site for why partition-keying is
// equivalent to broker-keying for this baseline.
type latencyDelayedKgoClient struct {
	*kgo.Client
	latency func(ctx context.Context, nodeID int32)
}

func (c *latencyDelayedKgoClient) Produce(ctx context.Context, r *kgo.Record, promise func(*kgo.Record, error)) {
	c.Client.Produce(ctx, r, func(rec *kgo.Record, err error) {
		go func() {
			c.latency(ctx, rec.Partition)
			// If the simulated latency was cut short because the caller's
			// ctx (e.g. app-request timeout) fired, surface that as the
			// record's outcome — kgo itself would have observed the
			// broker not respond in time.
			if err == nil && ctx.Err() != nil {
				err = ctx.Err()
			}
			promise(rec, err)
		}()
	})
}

func newIntegrationWarpstreamClient(t *testing.T, addr string) (*warpstreamclient.WarpstreamClient, *prometheus.Registry) {
	t.Helper()
	cfg := warpstreamclient.Config{
		Address:       []string{addr},
		Topic:         integrationTopic,
		ClientID:      "ws-integration",
		DialTimeout:   integrationTestDialTimeout,
		WriteTimeout:  integrationTestWriteTimeout,
		Linger:        integrationTestLinger,
		MaxBatchBytes: integrationTestBatchMaxBytes,
		HealthCheck: warpstreamclient.HealthCheckConfig{
			SlowMultiplier:    2.0,
			MaxSlowFraction:   0.3,
			FaultyThreshold:   0.05,
			MaxFaultyFraction: 0.3,
		},
		Hedger: warpstreamclient.HedgerConfig{
			MinHedgeDelay:  time.Second,
			MaxHedgeAgents: 3,
		},
		Demoter: warpstreamclient.DemoterConfig{
			ProbeInterval: time.Second,
		},
		ClusterStatsTTL:         time.Second,
		MetadataRefreshInterval: integrationTestMetadataRefresh,
		DirectProducer: warpstreamclient.KafkaDirectProducerConfig{
			ProduceRequestTimeout:         integrationTestWriteTimeout,
			ProduceRequestTimeoutOverhead: integrationTestRequestTimeoutOverhead,
		},
	}
	reg := prometheus.NewPedanticRegistry()
	c, err := warpstreamclient.NewWarpstreamClient(cfg, log.NewNopLogger(), reg)
	require.NoError(t, err)
	t.Cleanup(c.Close)
	return c, reg
}

// newIntegrationKgoClient mirrors the kgo.Client configuration that
// pkg/storage/ingest/writer_client.go applies in production (see
// NewKafkaWriterClient + commonKafkaClientOptions). Any drift would make
// the scenario comparison misleading, so the two sets of options are kept
// in lockstep here. Differences from the production setup are limited to
// test-only concerns (no SASL/TLS, no metrics hooks, no custom logger).
func newIntegrationKgoClient(t *testing.T, addr string) *kgo.Client {
	t.Helper()

	// Cap Produce at the version this package generates so request and
	// response payloads carry the topic name on the wire — matches the
	// WarpstreamClient's own pinning.
	v := kversion.Stable()
	v.SetMaxKeyVersion(kmsg.Produce.Int16(), integrationProduceAPIVersion)

	opts := []kgo.Opt{
		kgo.SeedBrokers(addr),
		kgo.ClientID("kgo-integration"),
		kgo.DialTimeout(integrationTestDialTimeout),
		kgo.MetadataMinAge(integrationTestMetadataRefresh),
		kgo.MetadataMaxAge(integrationTestMetadataRefresh),

		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.ProducerBatchMaxBytes(integrationTestBatchMaxBytes),
		kgo.DisableIdempotentWrite(),
		kgo.ProducerLinger(integrationTestLinger),
		kgo.MaxProduceRequestsInflightPerBroker(integrationTestMaxInflight),

		// Mirrors writer_client.go: unlimited retries, deadline on the
		// max time a record can take to be delivered.
		kgo.RecordRetries(math.MaxInt64),
		kgo.RecordDeliveryTimeout(integrationTestWriteTimeout),
		kgo.ProduceRequestTimeout(integrationTestWriteTimeout),
		kgo.RequestTimeoutOverhead(integrationTestRequestTimeoutOverhead),

		kgo.MaxBufferedRecords(math.MaxInt),
		kgo.MaxBufferedBytes(0),

		kgo.MaxVersions(v),
	}
	c, err := kgo.NewClient(opts...)
	require.NoError(t, err)
	t.Cleanup(c.Close)
	return c
}
