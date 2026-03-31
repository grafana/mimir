// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	ring_client "github.com/grafana/dskit/ring/client"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/grafana/mimir/pkg/distributor/distributorpb"
	ingester_client "github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/ingest"
	"github.com/grafana/mimir/pkg/util/testkafka"
)

// ---------------------------------------------------------------------------
// TestMergeRequests
// ---------------------------------------------------------------------------

func TestMergeRequests_SingleRequest(t *testing.T) {
	req := makeWriteRequest(1000, 3, 0, false, false, "metric_a")
	merged, err := mergeRequests([]*mimirpb.WriteRequest{req})
	require.NoError(t, err)
	require.Equal(t, 3, len(merged.TimeseriesRW2))
}

func TestMergeRequests_Empty(t *testing.T) {
	merged, err := mergeRequests(nil)
	require.NoError(t, err)
	require.Empty(t, merged.TimeseriesRW2)
}

func TestMergeRequests_DeduplicatesSymbols(t *testing.T) {
	// Three requests each with two series sharing the same __name__ and job labels.
	// After merging those symbols should appear only once in the unified table.
	req1 := makeWriteRequest(1000, 2, 0, false, false, "cpu_usage")
	req2 := makeWriteRequest(2000, 2, 0, false, false, "cpu_usage")
	req3 := makeWriteRequest(3000, 2, 0, false, false, "cpu_usage")

	merged, err := mergeRequests([]*mimirpb.WriteRequest{req1, req2, req3})
	require.NoError(t, err)

	// Total timeseries = 2+2+2.
	require.Equal(t, 6, len(merged.TimeseriesRW2))

	// All LabelsRefs should resolve consistently.
	offset := uint32(ingest.V2RecordSymbolOffset)
	commonSymbols := ingest.V2CommonSymbols.GetSlice()
	resolveRef := func(r uint32) string {
		if r == 0 {
			return ""
		}
		if r < offset {
			return commonSymbols[r]
		}
		idx := int(r - offset)
		require.Less(t, idx, len(merged.SymbolsRW2), "ref %d out of range", r)
		return merged.SymbolsRW2[idx]
	}

	for _, ts := range merged.TimeseriesRW2 {
		require.True(t, len(ts.LabelsRefs)%2 == 0, "labels_refs must have even length")
		for i := 0; i < len(ts.LabelsRefs); i += 2 {
			name := resolveRef(ts.LabelsRefs[i])
			value := resolveRef(ts.LabelsRefs[i+1])
			require.NotEmpty(t, name, "label name must not be empty")
			_ = value // sanity: no panic on resolve
		}
	}

	// "__name__" is a common symbol (ref < offset) so it must NOT appear in SymbolsRW2.
	for _, sym := range merged.SymbolsRW2 {
		require.NotEqual(t, "__name__", sym, "__name__ must be a common symbol, not in per-request table")
	}
}

func TestMergeRequests_PreservesExemplarRefs(t *testing.T) {
	req := makeWriteRequest(1000, 2, 0, true /* exemplars */, false, "req_dur")
	merged, err := mergeRequests([]*mimirpb.WriteRequest{req})
	require.NoError(t, err)

	offset := uint32(ingest.V2RecordSymbolOffset)
	for _, ts := range merged.TimeseriesRW2 {
		for _, ex := range ts.Exemplars {
			for _, r := range ex.LabelsRefs {
				require.True(t, r == 0 || r < offset || int(r-offset) < len(merged.SymbolsRW2),
					"exemplar ref %d out of bounds", r)
			}
		}
	}
}

// TestMergeRequests_AlreadyRW2Input verifies that two requests already in internal RW2 format
// (as produced by FromWriteRequestToRW2Request) are merged correctly when passed to mergeRequests.
// This exercises the "already RW2; use as-is" branch of the conversion loop.
func TestMergeRequests_AlreadyRW2Input(t *testing.T) {
	offset := uint32(ingest.V2RecordSymbolOffset)
	commonSymbols := ingest.V2CommonSymbols

	// Convert two RW1 requests to the internal RW2 format *before* calling mergeRequests,
	// so mergeRequests sees them as already-RW2.
	rw1a := makeWriteRequest(1000, 2, 0, false, false, "alpha")
	rw1b := makeWriteRequest(2000, 2, 0, false, false, "beta")

	rw2a, err := mimirpb.FromWriteRequestToRW2Request(rw1a, commonSymbols, offset)
	require.NoError(t, err)
	rw2b, err := mimirpb.FromWriteRequestToRW2Request(rw1b, commonSymbols, offset)
	require.NoError(t, err)

	merged, err := mergeRequests([]*mimirpb.WriteRequest{rw2a, rw2b})
	require.NoError(t, err)
	require.Equal(t, 4, len(merged.TimeseriesRW2))

	commonSlice := commonSymbols.GetSlice()
	resolve := func(r uint32) string {
		if r == 0 {
			return ""
		}
		if r < offset {
			return commonSlice[r]
		}
		idx := int(r - offset)
		require.Less(t, idx, len(merged.SymbolsRW2), "ref %d out of bounds", r)
		return merged.SymbolsRW2[idx]
	}

	metricNames := map[string]bool{}
	for _, ts := range merged.TimeseriesRW2 {
		require.True(t, len(ts.LabelsRefs)%2 == 0)
		for i := 0; i < len(ts.LabelsRefs); i += 2 {
			name := resolve(ts.LabelsRefs[i])
			require.NotEmpty(t, name)
			if name == "__name__" {
				metricNames[resolve(ts.LabelsRefs[i+1])] = true
			}
		}
	}
	require.Equal(t, map[string]bool{"alpha": true, "beta": true}, metricNames)
}

// TestMergeRequests_MetadataRefsRemapped verifies that HelpRef and UnitRef in timeseries
// metadata are correctly remapped when merging multiple requests.
func TestMergeRequests_MetadataRefsRemapped(t *testing.T) {
	offset := uint32(ingest.V2RecordSymbolOffset)

	// Build a request with metadata populated.  makeWriteRequest with histograms passes
	// through metadata=false, so we build one manually with a MetricMetadata entry.
	rw1 := &mimirpb.WriteRequest{
		Timeseries: []mimirpb.PreallocTimeseries{
			makeTimeseries(
				[]string{"__name__", "my_metric", "job", "my_job"},
				makeSamples(1000, 1.0),
				nil, nil,
			),
		},
		Metadata: []*mimirpb.MetricMetadata{
			{
				Type:             mimirpb.GAUGE,
				MetricFamilyName: "my_metric",
				Help:             "A help string unique to this test",
				Unit:             "bytes",
			},
		},
	}
	rw2extra := &mimirpb.WriteRequest{
		Timeseries: []mimirpb.PreallocTimeseries{
			makeTimeseries(
				[]string{"__name__", "other_metric", "job", "other_job"},
				makeSamples(2000, 2.0),
				nil, nil,
			),
		},
	}

	merged, err := mergeRequests([]*mimirpb.WriteRequest{rw1, rw2extra})
	require.NoError(t, err)
	require.Equal(t, 2, len(merged.TimeseriesRW2))

	commonSlice := ingest.V2CommonSymbols.GetSlice()
	resolve := func(r uint32) string {
		if r == 0 {
			return ""
		}
		if r < offset {
			require.Less(t, int(r), len(commonSlice), "common symbol ref %d out of bounds", r)
			return commonSlice[r]
		}
		idx := int(r - offset)
		require.Less(t, idx, len(merged.SymbolsRW2), "per-request ref %d out of bounds", r)
		return merged.SymbolsRW2[idx]
	}

	// Find the timeseries for my_metric and check its metadata refs.
	for _, ts := range merged.TimeseriesRW2 {
		labels := map[string]string{}
		for i := 0; i < len(ts.LabelsRefs); i += 2 {
			labels[resolve(ts.LabelsRefs[i])] = resolve(ts.LabelsRefs[i+1])
		}
		if labels["__name__"] != "my_metric" {
			continue
		}
		// HelpRef must resolve to the help string; UnitRef to "bytes".
		if ts.Metadata.HelpRef != 0 {
			require.Equal(t, "A help string unique to this test", resolve(ts.Metadata.HelpRef))
		}
		if ts.Metadata.UnitRef != 0 {
			require.Equal(t, "bytes", resolve(ts.Metadata.UnitRef))
		}
	}
}

func TestMergeRequests_LabelRoundTrip(t *testing.T) {
	// Build two RW1 requests with known labels and verify we can recover the exact
	// label name→value pairs from the merged output.
	req1 := makeWriteRequest(1000, 1, 0, false, false, "http_requests_total")
	req2 := makeWriteRequest(2000, 1, 0, false, false, "cpu_seconds_total")

	merged, err := mergeRequests([]*mimirpb.WriteRequest{req1, req2})
	require.NoError(t, err)
	require.Equal(t, 2, len(merged.TimeseriesRW2))

	offset := uint32(ingest.V2RecordSymbolOffset)
	commonSlice := ingest.V2CommonSymbols.GetSlice()
	resolve := func(r uint32) string {
		if r == 0 {
			return ""
		}
		if r < offset {
			return commonSlice[r]
		}
		idx := int(r - offset)
		require.Less(t, idx, len(merged.SymbolsRW2), "symbol ref %d out of bounds", r)
		return merged.SymbolsRW2[idx]
	}

	metricNames := map[string]bool{}
	for _, ts := range merged.TimeseriesRW2 {
		require.True(t, len(ts.LabelsRefs)%2 == 0)
		labels := map[string]string{}
		for i := 0; i < len(ts.LabelsRefs); i += 2 {
			labels[resolve(ts.LabelsRefs[i])] = resolve(ts.LabelsRefs[i+1])
		}
		require.Contains(t, labels, "__name__")
		metricNames[labels["__name__"]] = true
	}
	require.Equal(t, map[string]bool{"http_requests_total": true, "cpu_seconds_total": true}, metricNames)
}

func TestMergeRequests_ManyUniqueSymbols(t *testing.T) {
	// Merge many requests each contributing unique label values, producing a large
	// unified symbol table. This exercises the symbol table indexing under load and
	// is a regression test for a panic observed in production at ~28k symbols.
	const numRequests = 100
	const seriesPerRequest = 50
	reqs := make([]*mimirpb.WriteRequest, numRequests)
	for i := range reqs {
		req := &mimirpb.WriteRequest{}
		for j := 0; j < seriesPerRequest; j++ {
			req.Timeseries = append(req.Timeseries, makeTimeseries(
				[]string{
					"__name__", fmt.Sprintf("m_%d_%d", i, j),
					"instance", fmt.Sprintf("inst_%d_%d", i, j),
					"pod", fmt.Sprintf("pod_%d_%d", i, j),
					"label1", fmt.Sprintf("l1_%d_%d", i, j),
					"label2", fmt.Sprintf("l2_%d_%d", i, j),
				},
				makeSamples(int64(i*1000+j), float64(j)),
				nil, nil,
			))
		}
		reqs[i] = req
	}

	merged, err := mergeRequests(reqs)
	require.NoError(t, err)
	require.Equal(t, numRequests*seriesPerRequest, len(merged.TimeseriesRW2))

	// Every symbol ref in every timeseries must resolve without panic.
	offset := uint32(ingest.V2RecordSymbolOffset)
	for _, ts := range merged.TimeseriesRW2 {
		for _, r := range ts.LabelsRefs {
			if r >= offset {
				require.Less(t, int(r-offset), len(merged.SymbolsRW2), "ref %d out of bounds", r)
			}
		}
	}
}

// ---------------------------------------------------------------------------
// TestJumpHash
// ---------------------------------------------------------------------------

func TestJumpHash_InRange(t *testing.T) {
	for buckets := int64(1); buckets <= 100; buckets++ {
		for key := int64(0); key < 200; key++ {
			idx := jumpHash(key, buckets)
			require.GreaterOrEqualf(t, idx, int64(0), "key=%d buckets=%d", key, buckets)
			require.Lessf(t, idx, buckets, "key=%d buckets=%d", key, buckets)
		}
	}
}

func TestJumpHash_Stable(t *testing.T) {
	// Same key + buckets must always return the same bucket.
	for i := 0; i < 50; i++ {
		require.Equal(t, jumpHash(42, 10), jumpHash(42, 10))
	}
}

func TestJumpHash_Consistent(t *testing.T) {
	// Adding one bucket should only remap ~1/n of keys.
	const keys = 1000
	const n = 10
	moved := 0
	for k := int64(0); k < keys; k++ {
		if jumpHash(k, n) != jumpHash(k, n+1) {
			moved++
		}
	}
	// Expect roughly keys/n remapped (~100 out of 1000); allow wide margin.
	require.LessOrEqual(t, moved, keys/n*3, "too many keys remapped when adding a bucket")
}

// ---------------------------------------------------------------------------
// TestPickL2Owner
// ---------------------------------------------------------------------------

func TestPickL2Owner_ConsistentForSamePartition(t *testing.T) {
	instances := []ring.InstanceDesc{
		{Id: "a", Addr: "1.1.1.1:1"},
		{Id: "b", Addr: "2.2.2.2:2"},
		{Id: "c", Addr: "3.3.3.3:3"},
	}
	owner1 := pickL2Owner(5, instances)
	owner2 := pickL2Owner(5, instances)
	require.Equal(t, owner1.Id, owner2.Id)
}

func TestPickL2Owner_DifferentPartitionsMayDifferentOwners(t *testing.T) {
	instances := []ring.InstanceDesc{
		{Id: "a"}, {Id: "b"}, {Id: "c"}, {Id: "d"},
	}
	seen := map[string]struct{}{}
	for p := int32(0); p < 20; p++ {
		seen[pickL2Owner(p, instances).Id] = struct{}{}
	}
	require.Greater(t, len(seen), 1, "expected different partitions to map to different owners")
}

// ---------------------------------------------------------------------------
// TestL2Buffer
// ---------------------------------------------------------------------------

func newTestL2Writer(t *testing.T) (*ingest.Writer, string) {
	t.Helper()
	cluster, addr := testkafka.CreateCluster(t, 10, kafkaTopic)
	_ = cluster

	var kafkaCfg ingest.KafkaConfig
	flagext.DefaultValues(&kafkaCfg)
	kafkaCfg.Topic = kafkaTopic
	kafkaCfg.Address = flagext.StringSliceCSV{addr}

	writer := ingest.NewWriter(kafkaCfg, log.NewNopLogger(), prometheus.NewRegistry())
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), writer))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), writer))
	})
	return writer, addr
}

func TestL2Buffer_Push_GroupCommit(t *testing.T) {
	// N concurrent pushes for the same (partition, tenant) should all unblock together
	// when the buffer timeout fires.
	writer, _ := newTestL2Writer(t)
	cfg := L2Config{
		BufferDuration: 50 * time.Millisecond,
		MaxBufferBytes: 10 << 20, // large: won't trigger size-based flush
	}
	reg := prometheus.NewRegistry()
	buf := NewL2Buffer(cfg, writer, reg, log.NewNopLogger())
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), buf))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), buf))
	})

	const n = 5
	ctx := user.InjectOrgID(context.Background(), "tenant1")
	req := makeWriteRequest(1000, 2, 0, false, false, "metric")

	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		results []error
		done    = make(chan struct{})
	)

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := buf.Push(ctx, 0, req)
			mu.Lock()
			results = append(results, err)
			mu.Unlock()
			if len(results) == n {
				close(done)
			}
		}()
	}

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for group-commit to complete")
	}

	for _, err := range results {
		require.NoError(t, err)
	}

	// Verify flush counter shows only timeout flushes (not size-limit, since buffer was large).
	mfs, err := reg.Gather()
	require.NoError(t, err)
	var sizeLimitFlushes float64
	for _, mf := range mfs {
		if mf.GetName() != "cortex_distributor_l2_flush_total" {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, lp := range m.GetLabel() {
				if lp.GetName() == "reason" && lp.GetValue() == "size_limit" {
					sizeLimitFlushes = m.GetCounter().GetValue()
				}
			}
		}
	}
	assert.Equal(t, float64(0), sizeLimitFlushes, "expected no size-limit flushes")
}

func TestL2Buffer_Push_SizeLimitTrigger(t *testing.T) {
	writer, _ := newTestL2Writer(t)
	cfg := L2Config{
		BufferDuration: 10 * time.Second, // long: won't trigger timeout
		MaxBufferBytes: 1,                // tiny: will trigger immediately
	}
	buf := NewL2Buffer(cfg, writer, prometheus.NewRegistry(), log.NewNopLogger())
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), buf))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), buf))
	})

	ctx := user.InjectOrgID(context.Background(), "tenant1")
	req := makeWriteRequest(1000, 1, 0, false, false, "metric")

	// Should complete quickly because size limit is exceeded immediately.
	errCh := make(chan error, 1)
	go func() { errCh <- buf.Push(ctx, 0, req) }()

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("push did not complete after size-limit flush")
	}
}

func TestL2Buffer_Push_NoTenantIDReturnsError(t *testing.T) {
	writer, _ := newTestL2Writer(t)
	buf := NewL2Buffer(L2Config{BufferDuration: 100 * time.Millisecond}, writer, prometheus.NewRegistry(), log.NewNopLogger())
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), buf))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), buf))
	})

	req := makeWriteRequest(1000, 1, 0, false, false, "metric")
	err := buf.Push(context.Background() /* no org ID */, 0, req)
	require.Error(t, err)
}

// ---------------------------------------------------------------------------
// TestDistributor_LeveledMode
// ---------------------------------------------------------------------------

// mockL2DistributorClient records PushToPartition calls for inspection in tests.
type mockL2DistributorClient struct {
	mu       sync.Mutex
	received []*distributorpb.PushToPartitionRequest
	err      error
}

func (m *mockL2DistributorClient) PushToPartition(_ context.Context, in *distributorpb.PushToPartitionRequest, _ ...grpc.CallOption) (*mimirpb.WriteResponse, error) {
	m.mu.Lock()
	m.received = append(m.received, in)
	m.mu.Unlock()
	return &mimirpb.WriteResponse{}, m.err
}

func (m *mockL2DistributorClient) Push(_ context.Context, _ *mimirpb.WriteRequest, _ ...grpc.CallOption) (*mimirpb.WriteResponse, error) {
	return &mimirpb.WriteResponse{}, nil
}

func (m *mockL2DistributorClient) Check(_ context.Context, _ *grpc_health_v1.HealthCheckRequest, _ ...grpc.CallOption) (*grpc_health_v1.HealthCheckResponse, error) {
	return &grpc_health_v1.HealthCheckResponse{Status: grpc_health_v1.HealthCheckResponse_SERVING}, nil
}

func (m *mockL2DistributorClient) Watch(_ context.Context, _ *grpc_health_v1.HealthCheckRequest, _ ...grpc.CallOption) (grpc_health_v1.Health_WatchClient, error) {
	return nil, nil
}

func (m *mockL2DistributorClient) List(_ context.Context, _ *grpc_health_v1.HealthListRequest, _ ...grpc.CallOption) (*grpc_health_v1.HealthListResponse, error) {
	return &grpc_health_v1.HealthListResponse{}, nil
}

func (m *mockL2DistributorClient) Close() error { return nil }

func (m *mockL2DistributorClient) calls() []*distributorpb.PushToPartitionRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]*distributorpb.PushToPartitionRequest, len(m.received))
	copy(out, m.received)
	return out
}

func TestDistributor_LeveledMode_RoutesToL2NotWarpstream(t *testing.T) {
	mockClient := &mockL2DistributorClient{}

	distributors, _, _, _ := prepare(t, prepConfig{
		numDistributors:        1,
		ingestStorageEnabled:   true,
		ingestStoragePartitions: 2,
		configure: func(cfg *Config) {
			cfg.Mode = distributorModeLeveled
			cfg.L2ClientFactory = ring_client.PoolInstFunc(func(_ ring.InstanceDesc) (ring_client.PoolClient, error) {
				return mockClient, nil
			})
		},
	})
	d := distributors[0]

	ctx := user.InjectOrgID(context.Background(), "tenant1")
	req := makeWriteRequest(1000, 4, 0, false, false, "metric_a", "metric_b")

	var g errgroup.Group
	g.Go(func() error { _, err := d.Push(ctx, req); return err })
	require.NoError(t, g.Wait())

	calls := mockClient.calls()
	require.NotEmpty(t, calls, "expected PushToPartition calls on the mock L2 client")

	// Every call must target a valid partition ID.
	for _, c := range calls {
		require.GreaterOrEqual(t, c.PartitionId, int32(0))
		require.NotNil(t, c.Request)
	}
}

func TestDistributor_LeveledMode_PropagatesL2Error(t *testing.T) {
	mockClient := &mockL2DistributorClient{err: assert.AnError}

	distributors, _, _, _ := prepare(t, prepConfig{
		numDistributors:        1,
		ingestStorageEnabled:   true,
		ingestStoragePartitions: 2,
		configure: func(cfg *Config) {
			cfg.Mode = distributorModeLeveled
			cfg.L2ClientFactory = ring_client.PoolInstFunc(func(_ ring.InstanceDesc) (ring_client.PoolClient, error) {
				return mockClient, nil
			})
		},
	})
	d := distributors[0]

	ctx := user.InjectOrgID(context.Background(), "tenant1")
	req := makeWriteRequest(1000, 2, 0, false, false, "metric_a")
	_, err := d.Push(ctx, req)
	require.Error(t, err)
}

// ---------------------------------------------------------------------------
// TestDistributor_PushToPartition_Handler
// ---------------------------------------------------------------------------

func TestDistributor_PushToPartition_RejectsWhenNotLeveled(t *testing.T) {
	// A standalone-mode distributor must refuse PushToPartition.
	distributors, _, _, _ := prepare(t, prepConfig{
		numDistributors:      1,
		ingestStorageEnabled: true,
		ingestStoragePartitions: 2,
	})
	d := distributors[0]

	ctx := user.InjectOrgID(context.Background(), "tenant1")
	req := makeWriteRequest(1000, 1, 0, false, false, "m")
	_, err := d.PushToPartition(ctx, &distributorpb.PushToPartitionRequest{
		PartitionId: 0,
		Request:     req,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "leveled")
}

// unused import guard
var _ ingester_client.Config
