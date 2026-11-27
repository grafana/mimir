// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/user"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/ingest/kmeta"
	"github.com/grafana/mimir/pkg/storage/seriesstore/store"
	"github.com/grafana/mimir/pkg/storage/sharding"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/util/validation"
)

type mockNativeMetricMetadataServer struct {
	client.Ingester_NativeMetricMetadataServer
	ctx       context.Context
	responses []*client.NativeMetricMetadataResponse
}

func (s *mockNativeMetricMetadataServer) Context() context.Context { return s.ctx }

func (s *mockNativeMetricMetadataServer) Send(resp *client.NativeMetricMetadataResponse) error {
	s.responses = append(s.responses, resp)
	return nil
}

type mockNativeResourcesServer struct {
	client.Ingester_NativeResourcesServer
	ctx       context.Context
	responses []*client.NativeResourcesResponse
}

func (s *mockNativeResourcesServer) Context() context.Context { return s.ctx }

func (s *mockNativeResourcesServer) Send(resp *client.NativeResourcesResponse) error {
	s.responses = append(s.responses, resp)
	return nil
}

// nativeLookupData is two targets' series, with their resources and metric metadata. Target a's resource changes, and
// its series see the change at their own next samples: pod-0 a minute after the first samples, pod-1 and pod-2 15s
// later. Target b's doesn't change. Each target's resource has its own identity, as the instance label derives from
// service.instance.id.
func nativeLookupData(base int64) []*mimirpb.WriteRequest {
	resource := func(instance, host string) mimirpb.ResourceEntry {
		return mimirpb.ResourceEntry{Attributes: []mimirpb.ResourceAttribute{
			{Key: "service.name", StringValue: "api"},
			{Key: "service.instance.id", StringValue: instance},
			{Key: "host.name", StringValue: host},
		}}
	}
	series := func(pod, instance string, timestamp int64, resourceRef uint32) mimirpb.PreallocTimeseries {
		ts := compatSeries("requests_total", mimirpb.Sample{TimestampMs: timestamp, Value: 1})
		ts.Labels = append(ts.Labels,
			mimirpb.LabelAdapter{Name: "instance", Value: instance},
			mimirpb.LabelAdapter{Name: "job", Value: "api"},
			mimirpb.LabelAdapter{Name: "pod", Value: pod})
		ts.ResourceRef = resourceRef
		ts.SeriesMetadata = mimirpb.SeriesMetadata{Type: mimirpb.COUNTER, Help: "Requests.", Unit: "requests"}
		return ts
	}
	return []*mimirpb.WriteRequest{
		{
			ResourceTable: []mimirpb.ResourceEntry{resource("a", "node-1"), resource("b", "node-3")},
			Timeseries:    []mimirpb.PreallocTimeseries{series("pod-0", "a", base, 1), series("pod-1", "a", base, 1), series("pod-2", "a", base, 1), series("pod-3", "b", base, 2)},
		},
		{
			ResourceTable: []mimirpb.ResourceEntry{resource("a", "node-2"), resource("b", "node-3")},
			Timeseries:    []mimirpb.PreallocTimeseries{series("pod-0", "a", base+60_000, 1), series("pod-3", "b", base+60_000, 2)},
		},
		{
			ResourceTable: []mimirpb.ResourceEntry{resource("a", "node-2")},
			Timeseries:    []mimirpb.PreallocTimeseries{series("pod-1", "a", base+75_000, 1), series("pod-2", "a", base+75_000, 1)},
		},
	}
}

// nativeLookupIngester starts an ingester of the engine with limits, which has ingested the requests.
func nativeLookupIngester(t *testing.T, engine string, limits validation.Limits, requests []*mimirpb.WriteRequest) *Ingester {
	return nativeLookupIngesterWithOverrides(t, engine, validation.NewOverrides(limits, nil), requests)
}

// nativeLookupIngesterWithOverrides is nativeLookupIngester with overrides, which may change while it runs.
func nativeLookupIngesterWithOverrides(t *testing.T, engine string, overrides *validation.Overrides, requests []*mimirpb.WriteRequest) *Ingester {
	cfg := defaultIngesterTestConfig(t)
	cfg.IngestStorageConfig.KafkaConfig.ConsumeFromPositionAtStartup = "start"
	cfg.BlocksStorageConfig.TSDB.Engine = engine
	// The sequential pusher appends each record by itself.
	cfg.IngestStorageConfig.KafkaConfig.IngestionConcurrencyMax = 0
	ingester, _, _ := createTestIngesterWithIngestStorage(t, &cfg, overrides, nil, nil, nil)
	write, last := compatProducer(t, cfg.IngestStorageConfig.KafkaConfig)
	for _, req := range requests {
		write("tenant", req)
	}
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), ingester))
	t.Cleanup(func() { require.NoError(t, services.StopAndAwaitTerminated(context.Background(), ingester)) })
	waitCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	require.NoError(t, ingester.ingestReader.WaitReadConsistencyUntilOffsets(waitCtx, kmeta.NewSingleClusterPartitionOffsets(last())))
	return ingester
}

func nativeLookupMatchers(matchers ...*client.LabelMatcher) []*client.LabelMatchers {
	return []*client.LabelMatchers{{Matchers: matchers}}
}

var requestsTotal = &client.LabelMatcher{Type: client.EQUAL, Name: "__name__", Value: "requests_total"}

func TestIngester_NativeResources(t *testing.T) {
	skipIfSeriesstore(t, "it sets the engine itself")
	base := time.Now().Truncate(time.Minute).Add(-time.Hour).UnixMilli()
	version := func(minTime, maxTime int64, instance, host string) client.NativeResourceVersion {
		return client.NativeResourceVersion{
			MinTimeMs:   minTime,
			MaxTimeMs:   maxTime,
			Identifying: map[string]string{"service.name": "api", "service.instance.id": instance},
			Descriptive: map[string]string{"host.name": host},
		}
	}
	podLabels := func(pod, instance string) []mimirpb.LabelAdapter {
		return []mimirpb.LabelAdapter{{Name: "__name__", Value: "requests_total"}, {Name: "instance", Value: instance}, {Name: "job", Value: "api"}, {Name: "pod", Value: pod}}
	}
	targetLabels := func(instance string) []mimirpb.LabelAdapter {
		return []mimirpb.LabelAdapter{{Name: "instance", Value: instance}, {Name: "job", Value: "api"}}
	}

	for _, engine := range []string{mimir_tsdb.EngineSeriesstore} {
		t.Run(engine, func(t *testing.T) {
			limits := defaultLimitsTestConfig()
			limits.NativeResourcesEnabled = true
			// The caps are told apart: a lookup by series of all four fails, one by target doesn't.
			limits.NativeMetadataLookupMaxSeries = 3
			limits.NativeMetadataGroupedLookupMaxSeries = 4
			// Besides the targets' series, a series without a resource, as one sent over Remote Write.
			withoutResource := compatSeries("up", mimirpb.Sample{TimestampMs: base, Value: 1})
			withoutResource.Labels = append(withoutResource.Labels, mimirpb.LabelAdapter{Name: "instance", Value: "c"}, mimirpb.LabelAdapter{Name: "job", Value: "api"})
			requests := append(nativeLookupData(base), &mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{withoutResource}})
			ingester := nativeLookupIngester(t, engine, limits, requests)
			ctx := user.InjectOrgID(context.Background(), "tenant")
			lookup := func(req *client.NativeResourcesRequest) (*mockNativeResourcesServer, error) {
				stream := &mockNativeResourcesServer{ctx: ctx}
				return stream, ingester.NativeResources(req, stream)
			}

			// Each series' last version ends at its newest sample, and the others where the next starts. The limit lets
			// the first two series by labels through, but the lookup visits all four.
			pod0 := nativeLookupMatchers(requestsTotal, &client.LabelMatcher{Type: client.REGEX_MATCH, Name: "pod", Value: "pod-[01]"})
			stream, err := lookup(&client.NativeResourcesRequest{MatcherSets: pod0, StartTimestampMs: base, EndTimestampMs: base + 75_000, Limit: 1})
			require.NoError(t, err)
			require.Len(t, stream.responses, 1)
			require.Equal(t, []client.NativeResourcesSeries{
				{Labels: podLabels("pod-0", "a"), Versions: []client.NativeResourceVersion{version(base, base+59_999, "a", "node-1"), version(base+60_000, base+60_000, "a", "node-2")}},
			}, stream.responses[0].Series)
			require.True(t, stream.responses[0].Limited)
			require.Equal(t, int64(2), stream.responses[0].SeriesVisited)

			// By target, each target's series merge into one entry of its job and instance, whose versions don't overlap:
			// the change is in force from the first series that saw it, until the newest sample of any.
			stream, err = lookup(&client.NativeResourcesRequest{MatcherSets: nativeLookupMatchers(requestsTotal), StartTimestampMs: base, EndTimestampMs: base + 75_000, GroupByTarget: true})
			require.NoError(t, err)
			require.Equal(t, []client.NativeResourcesSeries{
				{Labels: targetLabels("a"), Versions: []client.NativeResourceVersion{version(base, base+59_999, "a", "node-1"), version(base+60_000, base+75_000, "a", "node-2")}},
				{Labels: targetLabels("b"), Versions: []client.NativeResourceVersion{version(base, base+60_000, "b", "node-3")}},
			}, stream.responses[0].Series)
			require.False(t, stream.responses[0].Limited)
			require.Equal(t, int64(4), stream.responses[0].SeriesVisited)

			// The limit is on targets.
			stream, err = lookup(&client.NativeResourcesRequest{MatcherSets: nativeLookupMatchers(requestsTotal), StartTimestampMs: base, EndTimestampMs: base + 75_000, GroupByTarget: true, Limit: 1})
			require.NoError(t, err)
			require.Len(t, stream.responses[0].Series, 1)
			require.True(t, stream.responses[0].Limited)

			_, err = lookup(&client.NativeResourcesRequest{MatcherSets: nativeLookupMatchers(requestsTotal), StartTimestampMs: base, EndTimestampMs: base + 75_000})
			require.True(t, client.IsNativeLookupTooManySeries(err), err)

			// A matcher set has to narrow the lookup down.
			_, err = lookup(&client.NativeResourcesRequest{MatcherSets: nativeLookupMatchers(&client.LabelMatcher{Type: client.REGEX_MATCH, Name: "pod", Value: ".*"})})
			require.Equal(t, codes.InvalidArgument, status.Code(err))
			// A query shard keeps a share of the series, but doesn't narrow them down either.
			shard := &client.LabelMatcher{Type: client.EQUAL, Name: sharding.ShardLabel, Value: sharding.FormatShardIDLabelValue(0, 2)}
			_, err = lookup(&client.NativeResourcesRequest{MatcherSets: nativeLookupMatchers(&client.LabelMatcher{Type: client.REGEX_MATCH, Name: "pod", Value: ".*"}, shard)})
			require.Equal(t, codes.InvalidArgument, status.Code(err))

			// Before the series' first versions, the lookup finds none in force, but the engine's native resources
			// started at base: the querier's cutover waits for it, so that the stored target_info answers for the range.
			stream, err = lookup(&client.NativeResourcesRequest{MatcherSets: pod0, StartTimestampMs: base - 10*60_000, EndTimestampMs: base - 60_000})
			require.NoError(t, err)
			require.Empty(t, stream.responses[0].Series)
			require.Equal(t, base, stream.responses[0].CoverageStartMs)

			// Likewise for series without a resource, such as a target's that stopped before its native resources
			// started, which the lookup visits none of.
			stream, err = lookup(&client.NativeResourcesRequest{MatcherSets: nativeLookupMatchers(&client.LabelMatcher{Type: client.EQUAL, Name: "__name__", Value: "up"}), StartTimestampMs: base, EndTimestampMs: base + 75_000, GroupByTarget: true})
			require.NoError(t, err)
			require.Empty(t, stream.responses[0].Series)
			require.Zero(t, stream.responses[0].SeriesVisited)
			require.Equal(t, base, stream.responses[0].CoverageStartMs)
		})
	}
}

// An engine whose native resources haven't started holds back the querier's cutover only if it visits series with
// versions it kept, which an engine whose flag went off and on again keeps until its next observation: they don't
// cover the off period. One that visits none has nothing to hold back, as a partition holding only target_info series,
// whose own series carry no resource.
func TestIngester_NativeResourcesCoverageStartBeforeTheEngineStarts(t *testing.T) {
	skipIfSeriesstore(t, "it sets the engine itself")
	base := time.Now().Truncate(time.Minute).Add(-time.Hour).UnixMilli()
	limits := defaultLimitsTestConfig()
	limits.NativeResourcesEnabled = true
	ctx := user.InjectOrgID(context.Background(), "tenant")
	lookup := func(ingester *Ingester, start, end int64) *client.NativeResourcesResponse {
		stream := &mockNativeResourcesServer{ctx: ctx}
		require.NoError(t, ingester.NativeResources(&client.NativeResourcesRequest{MatcherSets: nativeLookupMatchers(requestsTotal), StartTimestampMs: start, EndTimestampMs: end, GroupByTarget: true}, stream))
		require.Len(t, stream.responses, 1)
		return stream.responses[0]
	}

	t.Run("no resource observed", func(t *testing.T) {
		// The tenant's series, without their resources, as Remote Write sends them.
		requests := nativeLookupData(base)
		for _, req := range requests {
			req.ResourceTable = nil
			for i := range req.Timeseries {
				req.Timeseries[i].ResourceRef = 0
			}
		}
		ingester := nativeLookupIngester(t, mimir_tsdb.EngineSeriesstore, limits, requests)
		resp := lookup(ingester, base, base+75_000)
		require.Zero(t, resp.SeriesVisited)
		require.Equal(t, int64(math.MinInt64), resp.CoverageStartMs)
	})

	t.Run("flag off and on again", func(t *testing.T) {
		ingester := nativeLookupIngester(t, mimir_tsdb.EngineSeriesstore, limits, nativeLookupData(base))
		// The ingester sets the flag from the tenant's limits with each batch; no batch comes after these.
		engine := ingester.getTSDB("tenant").nativeResources.(seriesstoreEngine)
		engine.SetNativeResources(false)
		engine.SetNativeResources(true)

		resp := lookup(ingester, base, base+75_000)
		require.NotEmpty(t, resp.Series)
		require.Equal(t, int64(math.MaxInt64), resp.CoverageStartMs)
		// Before the kept versions too, where the lookup finds none in force.
		resp = lookup(ingester, base-10*60_000, base-60_000)
		require.Empty(t, resp.Series)
		require.Equal(t, int64(math.MaxInt64), resp.CoverageStartMs)
	})
}

func TestIngester_NativeMetricMetadata(t *testing.T) {
	skipIfSeriesstore(t, "it sets the engine itself")
	base := time.Now().Truncate(time.Minute).Add(-time.Hour).UnixMilli()
	for _, engine := range []string{mimir_tsdb.EngineSeriesstore} {
		t.Run(engine, func(t *testing.T) {
			limits := defaultLimitsTestConfig()
			limits.NativeMetricMetadataEnabled = true
			ingester := nativeLookupIngester(t, engine, limits, nativeLookupData(base))
			stream := &mockNativeMetricMetadataServer{ctx: user.InjectOrgID(context.Background(), "tenant")}
			require.NoError(t, ingester.NativeMetricMetadata(&client.NativeMetricMetadataRequest{MatcherSets: nativeLookupMatchers(requestsTotal), Limit: 1}, stream))
			require.Len(t, stream.responses, 1)
			require.Equal(t, []client.NativeMetricMetadataSeries{{
				Labels:   []mimirpb.LabelAdapter{{Name: "__name__", Value: "requests_total"}, {Name: "instance", Value: "a"}, {Name: "job", Value: "api"}, {Name: "pod", Value: "pod-0"}},
				Versions: []client.NativeMetricMetadataVersion{{MinTimeMs: base, MaxTimeMs: base + 60_000, Type: mimirpb.COUNTER, Unit: "requests", Help: "Requests."}},
			}}, stream.responses[0].Series)
			require.True(t, stream.responses[0].Limited)
			require.Equal(t, int64(4), stream.responses[0].SeriesVisited)
			require.Equal(t, base, stream.responses[0].CoverageStartMs)

			// A lookup matching no series still answers the engine's start, which the metadata covers from.
			stream = &mockNativeMetricMetadataServer{ctx: user.InjectOrgID(context.Background(), "tenant")}
			require.NoError(t, ingester.NativeMetricMetadata(&client.NativeMetricMetadataRequest{MatcherSets: nativeLookupMatchers(&client.LabelMatcher{Type: client.EQUAL, Name: "__name__", Value: "missing"})}, stream))
			require.Empty(t, stream.responses[0].Series)
			require.Equal(t, base, stream.responses[0].CoverageStartMs)
		})
	}
}

// The handler reads the tenant's switches of the seriesstore engine's optional lookup paths on every lookup, and passes
// each to its own path: changing one override between two lookups moves that path's counter alone, without a restart,
// so that each can be backed out at once, and the cell's measurements tell them apart by switching. A test of the
// engine alone, given the options directly, couldn't fail on the handler passing one in another's place, reading an
// override once, or leaving one out. The answers are the same whichever paths the lookup took.
func TestIngester_NativeResourcesSwitches(t *testing.T) {
	skipIfSeriesstore(t, "it sets the engine itself")
	base := time.Now().Truncate(time.Minute).Add(-time.Hour).UnixMilli()
	defaults := defaultLimitsTestConfig()
	defaults.NativeResourcesEnabled = true
	tenantLimits := defaults
	overrides := validation.NewOverrides(defaults, validation.NewMockTenantLimits(map[string]*validation.Limits{"tenant": &tenantLimits}))
	ingester := nativeLookupIngesterWithOverrides(t, mimir_tsdb.EngineSeriesstore, overrides, nativeLookupData(base))
	ctx := user.InjectOrgID(context.Background(), "tenant")

	// More instances than maxAcceptedValues, as info() asks for, so that the set mode applies; the data's are a and b.
	instances := []string{"a", "b"}
	for n := range 300 {
		instances = append(instances, fmt.Sprintf("other-%d", n))
	}
	byInstance := &client.LabelMatcher{Type: client.REGEX_MATCH, Name: "instance", Value: strings.Join(instances, "|")}
	req := &client.NativeResourcesRequest{MatcherSets: nativeLookupMatchers(requestsTotal, byInstance), StartTimestampMs: math.MinInt64, EndTimestampMs: math.MaxInt64, GroupByTarget: true}
	lookup := func() []client.NativeResourcesSeries {
		server := &mockNativeResourcesServer{ctx: ctx}
		require.NoError(t, ingester.NativeResources(req, server))
		var series []client.NativeResourcesSeries
		for _, resp := range server.responses {
			series = append(series, resp.Series...)
		}
		return series
	}
	paths := func() (set, unsorted, memo uint64) { return store.NativeResourcesPaths() }

	want := lookup()
	require.Len(t, want, 2)
	for _, step := range []struct {
		name                        string
		setMatching, unsorted, memo bool
	}{
		{name: "the set on", setMatching: true},
		{name: "the sort skip on too", setMatching: true, unsorted: true},
		{name: "the memo on too", setMatching: true, unsorted: true, memo: true},
		{name: "the set off again", unsorted: true, memo: true},
		{name: "the sort skip off again", memo: true},
		{name: "all off again"},
	} {
		tenantLimits.NativeResourcesLookupSetMatching, tenantLimits.NativeResourcesLookupUnsorted, tenantLimits.NativeResourcesLookupMemo = step.setMatching, step.unsorted, step.memo
		setBefore, unsortedBefore, memoBefore := paths()
		require.Equal(t, want, lookup(), step.name)
		setAfter, unsortedAfter, memoAfter := paths()
		moved := func(on bool) uint64 {
			if on {
				return 1
			}
			return 0
		}
		require.Equal(t, moved(step.setMatching), setAfter-setBefore, "%s: the set's path", step.name)
		require.Equal(t, moved(step.unsorted), unsortedAfter-unsortedBefore, "%s: the sort skip's path", step.name)
		require.Equal(t, moved(step.memo), memoAfter-memoBefore, "%s: the memo's path", step.name)
	}
}

// A querier falls back to target_info for a tenant whose native metadata an ingester doesn't keep, which it tells apart
// from one without such series by the status: a tenant with native metadata off, on either engine, and any tenant on
// the TSDB engine, which keeps none. The seriesstore engine would answer a tenant with it off with nothing.
func TestIngester_NativeLookupsOfADisabledTenant(t *testing.T) {
	skipIfSeriesstore(t, "it runs both engines itself")
	base := time.Now().Truncate(time.Minute).Add(-time.Hour).UnixMilli()
	on := defaultLimitsTestConfig()
	on.NativeMetricMetadataEnabled = true
	on.NativeResourcesEnabled = true
	for name, test := range map[string]struct {
		engine string
		limits validation.Limits
	}{
		"prometheus engine, tenant off":  {mimir_tsdb.EnginePrometheus, defaultLimitsTestConfig()},
		"seriesstore engine, tenant off": {mimir_tsdb.EngineSeriesstore, defaultLimitsTestConfig()},
		"prometheus engine, tenant on":   {mimir_tsdb.EnginePrometheus, on},
	} {
		t.Run(name, func(t *testing.T) {
			ingester := nativeLookupIngester(t, test.engine, test.limits, nativeLookupData(base))
			ctx := user.InjectOrgID(context.Background(), "tenant")
			err := ingester.NativeResources(&client.NativeResourcesRequest{MatcherSets: nativeLookupMatchers(requestsTotal)}, &mockNativeResourcesServer{ctx: ctx})
			require.True(t, client.IsNativeMetadataDisabled(err), fmt.Sprint(err))
			err = ingester.NativeMetricMetadata(&client.NativeMetricMetadataRequest{MatcherSets: nativeLookupMatchers(requestsTotal)}, &mockNativeMetricMetadataServer{ctx: ctx})
			require.True(t, client.IsNativeMetadataDisabled(err), fmt.Sprint(err))
		})
	}
}

// A lookup by target answers the same whatever the order of the engine's series, which it may return in shard order
// rather than label order. The series have few targets, times, contents and newest samples, so that a target's series
// tie on each.
func TestGroupNativeResourcesByTarget_AnyOrder(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	targets := [][]mimirpb.LabelAdapter{
		{{Name: "instance", Value: "a:9100"}, {Name: "job", Value: "node"}},
		{{Name: "instance", Value: "b:9100"}, {Name: "job", Value: "node"}},
		{{Name: "job", Value: "batch"}},
	}
	hosts := []string{"a", "b", "c"}
	for range 500 {
		var series []client.NativeResourcesSeries
		for i := range 2 + r.Intn(10) {
			target := targets[r.Intn(len(targets))]
			lbls := append([]mimirpb.LabelAdapter{{Name: "__name__", Value: fmt.Sprintf("metric_%d", i)}}, target...)
			minTime := []int64{100, 200}[r.Intn(2)]
			newest := []int64{300, 400}[r.Intn(2)]
			versions := []client.NativeResourceVersion{{MinTimeMs: minTime, MaxTimeMs: newest, Identifying: map[string]string{"service.name": "api"}, Descriptive: map[string]string{"host.name": hosts[r.Intn(len(hosts))]}}}
			series = append(series, client.NativeResourcesSeries{Labels: lbls, Versions: versions, Coalesced: r.Intn(4) == 0})
		}
		want := groupNativeResourcesByTarget(series)
		for range 5 {
			shuffled := slices.Clone(series)
			r.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
			require.Equal(t, want, groupNativeResourcesByTarget(shuffled))
		}
	}
}
