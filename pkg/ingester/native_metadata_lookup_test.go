// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"fmt"
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
	cfg := defaultIngesterTestConfig(t)
	cfg.IngestStorageConfig.KafkaConfig.ConsumeFromPositionAtStartup = "start"
	cfg.BlocksStorageConfig.TSDB.Engine = engine
	// The sequential pusher appends each record by itself: #17740 keeps a series' first resource of an appender, which
	// the parallel pusher may append several records with.
	cfg.IngestStorageConfig.KafkaConfig.IngestionConcurrencyMax = 0
	ingester, _, _ := createTestIngesterWithIngestStorage(t, &cfg, validation.NewOverrides(limits, nil), nil, nil, nil)
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
	skipIfSeriesstore(t, "it runs both engines itself")
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

	for _, engine := range []string{mimir_tsdb.EnginePrometheus, mimir_tsdb.EngineSeriesstore} {
		t.Run(engine, func(t *testing.T) {
			limits := defaultLimitsTestConfig()
			limits.NativeResourcesEnabled = true
			// The caps are told apart: a lookup by series of all four fails, one by target doesn't.
			limits.NativeMetadataLookupMaxSeries = 3
			limits.NativeMetadataGroupedLookupMaxSeries = 4
			ingester := nativeLookupIngester(t, engine, limits, nativeLookupData(base))
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
		})
	}
}

func TestIngester_NativeMetricMetadata(t *testing.T) {
	skipIfSeriesstore(t, "it runs both engines itself")
	base := time.Now().Truncate(time.Minute).Add(-time.Hour).UnixMilli()
	for _, engine := range []string{mimir_tsdb.EnginePrometheus, mimir_tsdb.EngineSeriesstore} {
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
		})
	}
}

// A querier falls back to target_info for a tenant whose native metadata an ingester doesn't keep, which it tells apart
// from one without such series by the status. The seriesstore engine would answer such a tenant with nothing.
func TestIngester_NativeLookupsOfADisabledTenant(t *testing.T) {
	skipIfSeriesstore(t, "it runs both engines itself")
	base := time.Now().Truncate(time.Minute).Add(-time.Hour).UnixMilli()
	for _, engine := range []string{mimir_tsdb.EnginePrometheus, mimir_tsdb.EngineSeriesstore} {
		t.Run(engine, func(t *testing.T) {
			ingester := nativeLookupIngester(t, engine, defaultLimitsTestConfig(), nativeLookupData(base))
			ctx := user.InjectOrgID(context.Background(), "tenant")
			err := ingester.NativeResources(&client.NativeResourcesRequest{MatcherSets: nativeLookupMatchers(requestsTotal)}, &mockNativeResourcesServer{ctx: ctx})
			require.True(t, client.IsNativeMetadataDisabled(err), fmt.Sprint(err))
			err = ingester.NativeMetricMetadata(&client.NativeMetricMetadataRequest{MatcherSets: nativeLookupMatchers(requestsTotal)}, &mockNativeMetricMetadataServer{ctx: ctx})
			require.True(t, client.IsNativeMetadataDisabled(err), fmt.Sprint(err))
		})
	}
}
