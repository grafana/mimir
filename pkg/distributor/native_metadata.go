// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"context"
	"errors"
	"io"
	"math"
	"slices"
	"strings"
	"sync"

	"github.com/go-kit/log/level"
	"github.com/prometheus/prometheus/model/labels"

	ingester_client "github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/globalerror"
	"github.com/grafana/mimir/pkg/util/spanlogger"
	"github.com/grafana/mimir/pkg/util/validation"
)

// NativeMetricMetadataResult is a lookup of native metric metadata, merged over the ingesters.
type NativeMetricMetadataResult struct {
	// The series, in label order.
	Series []ingester_client.NativeMetricMetadataSeries
	// Whether more series matched than the limit let through.
	Limited bool
	// When every ingester's native metadata has started, which is when the lookup covers the series' changes.
	CoverageStart int64
}

// NativeResourcesResult is a lookup of native resources, merged over the ingesters.
type NativeResourcesResult struct {
	// The series, or targets, in label order.
	Series []ingester_client.NativeResourcesSeries
	// Whether more series, or targets, matched than the limit let through.
	Limited bool
	// When every ingester's native resources have started, which is when the lookup covers the series' changes.
	CoverageStart int64
}

// NativeMetricMetadata looks up the native metric metadata of the series matching any of the matcher sets, in the
// ingesters, and merges their answers. A tenant whose native metadata an ingester doesn't keep fails the lookup with
// an error ingester_client.IsNativeMetadataDisabled reports, and so does a lookup over an ingester's cap with
// ingester_client.IsNativeLookupTooManySeries.
func (d *Distributor) NativeMetricMetadata(ctx context.Context, matcherSets [][]*labels.Matcher, limit int) (NativeMetricMetadataResult, error) {
	req := &ingester_client.NativeMetricMetadataRequest{Limit: int64(limit)}
	var err error
	if req.MatcherSets, err = toLabelMatcherSets(matcherSets); err != nil {
		return NativeMetricMetadataResult{}, err
	}
	merge := newNativeLookupMerge[ingester_client.NativeMetricMetadataVersion]()
	err = d.nativeLookup(ctx, "Distributor.NativeMetricMetadata", matcherSets, func(ctx context.Context, client ingester_client.IngesterClient) error {
		stream, err := client.NativeMetricMetadata(ctx, req)
		if err != nil {
			return err
		}
		defer func() { _ = util.CloseAndExhaust[*ingester_client.NativeMetricMetadataResponse](stream) }()
		for {
			msg, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				return nil
			}
			if err != nil {
				return err
			}
			merge.addResponse(ctx, msg.Size(), msg.Limited, msg.CoverageStartMs, msg.SeriesVisited)
			for _, s := range msg.Series {
				merge.addSeries(s.Labels, s.Versions, s.Truncated, s.Coalesced)
			}
			// The merge has copied the labels, the only fields that refer to the buffer.
			msg.FreeBuffer()
		}
	})
	if err != nil {
		return NativeMetricMetadataResult{}, err
	}
	merged, limited := merge.result(limit, ingester_client.MergeNativeMetricMetadataVersions)
	result := NativeMetricMetadataResult{Series: make([]ingester_client.NativeMetricMetadataSeries, 0, len(merged)), Limited: limited, CoverageStart: merge.coverageStart}
	for _, s := range merged {
		result.Series = append(result.Series, ingester_client.NativeMetricMetadataSeries{Labels: s.labels, Versions: s.versions, Truncated: s.truncated, Coalesced: s.coalesced})
	}
	return result, nil
}

// NativeResources looks up the native resources in force during [start, end] of the series matching any of the
// matcher sets, or with groupByTarget, of their targets, in the ingesters, and merges their answers. It fails like
// NativeMetricMetadata does.
func (d *Distributor) NativeResources(ctx context.Context, matcherSets [][]*labels.Matcher, start, end int64, limit int, groupByTarget bool) (NativeResourcesResult, error) {
	req := &ingester_client.NativeResourcesRequest{StartTimestampMs: start, EndTimestampMs: end, Limit: int64(limit), GroupByTarget: groupByTarget}
	var err error
	if req.MatcherSets, err = toLabelMatcherSets(matcherSets); err != nil {
		return NativeResourcesResult{}, err
	}
	merge := newNativeLookupMerge[ingester_client.NativeResourceVersion]()
	err = d.nativeLookup(ctx, "Distributor.NativeResources", matcherSets, func(ctx context.Context, client ingester_client.IngesterClient) error {
		stream, err := client.NativeResources(ctx, req)
		if err != nil {
			return err
		}
		defer func() { _ = util.CloseAndExhaust[*ingester_client.NativeResourcesResponse](stream) }()
		for {
			msg, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				return nil
			}
			if err != nil {
				return err
			}
			merge.addResponse(ctx, msg.Size(), msg.Limited, msg.CoverageStartMs, msg.SeriesVisited)
			for _, s := range msg.Series {
				merge.addSeries(s.Labels, s.Versions, false, s.Coalesced)
			}
			// As for the metadata, the merge has copied the labels.
			msg.FreeBuffer()
		}
	})
	if err != nil {
		return NativeResourcesResult{}, err
	}
	merged, limited := merge.result(limit, ingester_client.MergeNativeResourceVersions)
	result := NativeResourcesResult{Series: make([]ingester_client.NativeResourcesSeries, 0, len(merged)), Limited: limited, CoverageStart: merge.coverageStart}
	for _, s := range merged {
		result.Series = append(result.Series, ingester_client.NativeResourcesSeries{Labels: s.labels, Versions: s.versions, Coalesced: s.coalesced})
	}
	return result, nil
}

// nativeLookup runs lookup on the ingesters of every partition, or replication set, that the matcher sets' series may
// be in, until each one's quorum.
func (d *Distributor) nativeLookup(ctx context.Context, operation string, matcherSets [][]*labels.Matcher, lookup func(context.Context, ingester_client.IngesterClient) error) error {
	// Compartments route by the matchers of one set; with several, every compartment may have their series.
	var routing []*labels.Matcher
	if len(matcherSets) == 1 {
		routing = matcherSets[0]
	}
	replicationSets, err := d.getIngesterReplicationSetsForQuery(ctx, routing)
	if err != nil {
		return err
	}
	quorumConfig := d.queryQuorumConfigForReplicationSets(ctx, replicationSets)
	// Another ingester of the partition answers a disabled tenant, or a lookup over the cap, the same.
	quorumConfig.IsTerminalError = func(err error) bool {
		return ingester_client.IsNativeMetadataDisabled(err) || ingester_client.IsNativeLookupTooManySeries(err) || validation.IsLimitError(err)
	}
	_, err = forReplicationSetsWithQuorumConfig(ctx, d, replicationSets, quorumConfig, func(ctx context.Context, client ingester_client.IngesterClient) (struct{}, error) {
		log, ctx := spanlogger.New(ctx, d.log, tracer, operation)
		defer log.Finish()
		err := lookup(ctx, client)
		if err != nil && !errors.Is(globalerror.WrapGRPCErrorWithContextError(ctx, err), context.Canceled) {
			level.Debug(log).Log("msg", "native metadata lookup failed", "err", err)
		}
		return struct{}{}, err
	})
	return err
}

func toLabelMatcherSets(matcherSets [][]*labels.Matcher) ([]*ingester_client.LabelMatchers, error) {
	out := make([]*ingester_client.LabelMatchers, 0, len(matcherSets))
	for _, matchers := range matcherSets {
		m, err := ingester_client.ToLabelMatchers(matchers)
		if err != nil {
			return nil, err
		}
		out = append(out, &ingester_client.LabelMatchers{Matchers: m})
	}
	return out, nil
}

// nativeLookupMerge collects the ingesters' answers about each series, or target, to merge them by their change
// points.
type nativeLookupMerge[V any] struct {
	mtx           sync.Mutex
	series        map[string]*mergedNativeSeries[V]
	limited       bool
	coverageStart int64
}

type mergedNativeSeries[V any] struct {
	labels               []mimirpb.LabelAdapter
	answers              [][]V
	versions             []V
	truncated, coalesced bool
}

func newNativeLookupMerge[V any]() *nativeLookupMerge[V] {
	return &nativeLookupMerge[V]{series: map[string]*mergedNativeSeries[V]{}, coverageStart: math.MinInt64}
}

// addResponse adds a response's lookup-wide fields, and counts its bytes and the series its ingester visited for the
// query's stats.
func (m *nativeLookupMerge[V]) addResponse(ctx context.Context, size int, limited bool, coverageStart, seriesVisited int64) {
	queryStats := stats.FromContext(ctx)
	queryStats.AddNativeLookupResponseBytes(uint64(size))
	queryStats.AddNativeLookupSeriesVisited(uint64(seriesVisited))
	m.mtx.Lock()
	defer m.mtx.Unlock()
	m.limited = m.limited || limited
	// The merged lookup covers the series' changes from when every ingester's native metadata has started.
	m.coverageStart = max(m.coverageStart, coverageStart)
}

func (m *nativeLookupMerge[V]) addSeries(lbls []mimirpb.LabelAdapter, versions []V, truncated, coalesced bool) {
	key := nativeSeriesKey(lbls)
	m.mtx.Lock()
	defer m.mtx.Unlock()
	s, ok := m.series[key]
	if !ok {
		// The labels refer to the response's buffer.
		s = &mergedNativeSeries[V]{labels: make([]mimirpb.LabelAdapter, len(lbls))}
		for i, l := range lbls {
			s.labels[i] = mimirpb.LabelAdapter{Name: strings.Clone(l.Name), Value: strings.Clone(l.Value)}
		}
		m.series[key] = s
	}
	s.answers = append(s.answers, versions)
	s.truncated = s.truncated || truncated
	s.coalesced = s.coalesced || coalesced
}

// result is the merged series in label order, at most limit of them when positive, and whether more matched, here or
// in an ingester.
func (m *nativeLookupMerge[V]) result(limit int, mergeVersions func([][]V) []V) ([]*mergedNativeSeries[V], bool) {
	out := make([]*mergedNativeSeries[V], 0, len(m.series))
	for _, s := range m.series {
		s.versions = mergeVersions(s.answers)
		out = append(out, s)
	}
	slices.SortFunc(out, func(a, b *mergedNativeSeries[V]) int {
		return labels.Compare(mimirpb.FromLabelAdaptersToLabels(a.labels), mimirpb.FromLabelAdaptersToLabels(b.labels))
	})
	limited := m.limited
	if limit > 0 && len(out) > limit {
		out, limited = out[:limit], true
	}
	return out, limited
}

func nativeSeriesKey(lbls []mimirpb.LabelAdapter) string {
	var b strings.Builder
	for _, l := range lbls {
		b.WriteString(l.Name)
		b.WriteByte(0xff)
		b.WriteString(l.Value)
		b.WriteByte(0xff)
	}
	return b.String()
}
