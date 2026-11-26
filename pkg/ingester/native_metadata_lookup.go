// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"errors"
	"fmt"
	"math"
	"slices"

	"github.com/grafana/dskit/tenant"
	"github.com/prometheus/prometheus/model/labels"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/seriesstore/store"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

// nativeLookupMaxBatchBytes bounds each message of a native metadata lookup's stream, as the active series' are.
const nativeLookupMaxBatchBytes = activeSeriesMaxSizeBytes

// NativeMetricMetadata implements the NativeMetricMetadata RPC: the series' own metric metadata, as versions over
// time, of the series matching any of the request's matcher sets.
func (i *Ingester) NativeMetricMetadata(req *client.NativeMetricMetadataRequest, stream client.Ingester_NativeMetricMetadataServer) (err error) {
	defer func() { err = i.mapNativeLookupError(err) }()

	spanlog, ctx := spanlogger.New(stream.Context(), i.logger, tracer, "Ingester.NativeMetricMetadata")
	defer spanlog.Finish()

	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return err
	}
	matcherSets, err := nativeLookupMatcherSets(req.MatcherSets)
	if err != nil {
		return err
	}
	// The tenant's switch decides, not the engine: the seriesstore engine answers a tenant without native metadata with
	// nothing, which a querier couldn't tell from a tenant without such series.
	if !i.limits.NativeMetricMetadataEnabled(userID) {
		return client.ErrNativeMetadataDisabled
	}
	if err := i.enforceReadConsistency(ctx, userID); err != nil {
		return err
	}
	db := i.getTSDB(userID)
	if db == nil {
		// No series of the tenant here, so nothing for the merge's cutover to wait for.
		return stream.Send(&client.NativeMetricMetadataResponse{CoverageStartMs: math.MinInt64})
	}
	if db.nativeMetadata == nil {
		return client.ErrNativeMetadataDisabled
	}
	maxSeries := i.limits.NativeMetadataLookupMaxSeries(userID, false)
	result, err := db.nativeMetadata.nativeMetricMetadata(ctx, matcherSets, int(req.Limit), maxSeries)
	if err != nil {
		return nativeLookupEngineError(err, maxSeries)
	}

	resp := &client.NativeMetricMetadataResponse{Limited: result.Limited, CoverageStartMs: lookupCoverageStart(result.CoverageStart, result.SeriesVisited), SeriesVisited: int64(result.SeriesVisited)}
	size := 0
	for _, s := range result.Series {
		series := client.NativeMetricMetadataSeries{
			Labels:    mimirpb.FromLabelsToLabelAdapters(s.Labels),
			Versions:  nativeMetricMetadataVersions(s.Versions, s.NewestSample),
			Truncated: s.Truncated,
			Coalesced: s.Coalesced,
		}
		if size > 0 && size+series.Size() > nativeLookupMaxBatchBytes {
			if err := stream.Send(resp); err != nil {
				return fmt.Errorf("error sending response: %w", err)
			}
			resp, size = &client.NativeMetricMetadataResponse{}, 0
		}
		resp.Series = append(resp.Series, series)
		size += series.Size()
	}
	return stream.Send(resp)
}

// NativeResources implements the NativeResources RPC: the series' OTel resources, as versions over time, of the series
// matching any of the request's matcher sets, or with group_by_target, of their targets.
func (i *Ingester) NativeResources(req *client.NativeResourcesRequest, stream client.Ingester_NativeResourcesServer) (err error) {
	defer func() { err = i.mapNativeLookupError(err) }()

	spanlog, ctx := spanlogger.New(stream.Context(), i.logger, tracer, "Ingester.NativeResources")
	defer spanlog.Finish()

	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return err
	}
	matcherSets, err := nativeLookupMatcherSets(req.MatcherSets)
	if err != nil {
		return err
	}
	if !i.limits.NativeResourcesEnabled(userID) {
		return client.ErrNativeMetadataDisabled
	}
	if err := i.enforceReadConsistency(ctx, userID); err != nil {
		return err
	}
	db := i.getTSDB(userID)
	if db == nil {
		return stream.Send(&client.NativeResourcesResponse{CoverageStartMs: math.MinInt64})
	}
	if db.nativeResources == nil {
		return client.ErrNativeMetadataDisabled
	}
	maxSeries := i.limits.NativeMetadataLookupMaxSeries(userID, req.GroupByTarget)
	limit := int(req.Limit)
	if req.GroupByTarget {
		// The limit is on targets, which every series of is needed for.
		limit = 0
	}
	// The tenant's switches are read on every lookup, so that turning one off takes effect at once. A lookup by series
	// keeps its label order.
	setMatching, unsorted := i.limits.NativeResourcesLookupOptions(userID)
	opts := store.NativeResourcesOptions{SetMatching: setMatching, Unsorted: unsorted && req.GroupByTarget}
	result, err := db.nativeResources.nativeResources(ctx, matcherSets, req.StartTimestampMs, req.EndTimestampMs, limit, maxSeries, opts)
	if err != nil {
		return nativeLookupEngineError(err, maxSeries)
	}
	series := make([]client.NativeResourcesSeries, 0, len(result.Series))
	for _, s := range result.Series {
		series = append(series, client.NativeResourcesSeries{
			Labels:    mimirpb.FromLabelsToLabelAdapters(s.Labels),
			Versions:  nativeResourceVersions(s.Versions, s.NewestSample),
			Coalesced: s.Coalesced,
		})
	}
	limited := result.Limited
	if req.GroupByTarget {
		series = groupNativeResourcesByTarget(series)
		if req.Limit > 0 && len(series) > int(req.Limit) {
			series, limited = series[:req.Limit], true
		}
	}

	resp := &client.NativeResourcesResponse{Limited: limited, CoverageStartMs: lookupCoverageStart(result.CoverageStart, result.SeriesVisited), SeriesVisited: int64(result.SeriesVisited)}
	size := 0
	for _, s := range series {
		if size > 0 && size+s.Size() > nativeLookupMaxBatchBytes {
			if err := stream.Send(resp); err != nil {
				return fmt.Errorf("error sending response: %w", err)
			}
			resp, size = &client.NativeResourcesResponse{}, 0
		}
		resp.Series = append(resp.Series, s)
		size += s.Size()
	}
	return stream.Send(resp)
}

// lookupCoverageStart is when a lookup's answer covers the series' changes from: the engine's start, whatever the
// lookup's range found, since before it the stored target_info is in force. An engine that hasn't started, at
// math.MaxInt64, and visited none of the matching series with native metadata, has nothing for the querier's cutover
// to wait for: an empty head, or one holding only series without, as a partition holding only target_info series,
// whose own series carry no resource. One that visited some holds the cutover back: after its flag went off and on
// again, it keeps the versions from before until its next observation, which don't cover the off period.
func lookupCoverageStart(coverageStart int64, visited int) int64 {
	if coverageStart == math.MaxInt64 && visited == 0 {
		return math.MinInt64
	}
	return coverageStart
}

// nativeLookupMatcherSets are the request's matcher sets, each of which has to have a matcher that doesn't match the
// empty string, as series requests do: otherwise a lookup goes through every series of the tenant.
func nativeLookupMatcherSets(sets []*client.LabelMatchers) ([][]*labels.Matcher, error) {
	if len(sets) == 0 {
		return nil, status.Error(codes.InvalidArgument, "a native metadata lookup needs a matcher set")
	}
	out := make([][]*labels.Matcher, 0, len(sets))
	for _, set := range sets {
		matchers, err := client.FromLabelMatchers(set.Matchers)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "error parsing label matchers: %v", err)
		}
		if !slices.ContainsFunc(matchers, func(m *labels.Matcher) bool { return !m.Matches("") }) {
			return nil, status.Error(codes.InvalidArgument, "every matcher set of a native metadata lookup needs a matcher that doesn't match the empty string")
		}
		out = append(out, matchers)
	}
	return out, nil
}

// nativeLookupEngineError is the status of an engine's error that a querier tells apart from failures.
func nativeLookupEngineError(err error, maxSeries int) error {
	if errors.Is(err, errNativeLookupTooManySeries) {
		return client.NativeLookupTooManySeriesError(maxSeries)
	}
	return err
}

// mapNativeLookupError keeps the statuses a querier tells apart, which the read path's mapping would make internal
// errors, and maps the others like the read path's.
func (i *Ingester) mapNativeLookupError(err error) error {
	if s, ok := status.FromError(err); ok && (s.Code() == codes.InvalidArgument || client.IsNativeMetadataDisabled(err) || client.IsNativeLookupTooManySeries(err)) {
		return err
	}
	return i.mapReadErrorToErrorWithStatus(err)
}

// nativeMetricMetadataVersions are the engine's change points as versions, each ending where the next starts, and the
// last at the series' newest sample. A version never ends before it starts, which the last would for a series whose
// samples the engine no longer holds.
func nativeMetricMetadataVersions(versions []nativeMetricMetadataVersion, newestSample int64) []client.NativeMetricMetadataVersion {
	out := make([]client.NativeMetricMetadataVersion, len(versions))
	for n, v := range versions {
		end := newestSample
		if n+1 < len(versions) {
			end = versions[n+1].EffectiveFrom - 1
		}
		out[n] = client.NativeMetricMetadataVersion{
			MinTimeMs: v.EffectiveFrom,
			MaxTimeMs: max(end, v.EffectiveFrom),
			Type:      mimirpb.MetricTypeToMetricMetadataMetricType(v.Metadata.Type),
			Unit:      v.Metadata.Unit,
			Help:      v.Metadata.Help,
		}
	}
	return out
}

// nativeResourceVersions are the engine's change points as versions, like nativeMetricMetadataVersions'.
func nativeResourceVersions(versions []nativeResourceVersion, newestSample int64) []client.NativeResourceVersion {
	out := make([]client.NativeResourceVersion, len(versions))
	for n, v := range versions {
		end := newestSample
		if n+1 < len(versions) {
			end = versions[n+1].EffectiveFrom - 1
		}
		out[n] = client.NativeResourceVersion{
			MinTimeMs:   v.EffectiveFrom,
			MaxTimeMs:   max(end, v.EffectiveFrom),
			Identifying: v.Identifying,
			Descriptive: v.Descriptive,
		}
	}
	return out
}

// groupNativeResourcesByTarget merges the series' resources by their target, its job and instance, into one entry
// each, with versions that don't overlap. It's in label order, as the series were.
func groupNativeResourcesByTarget(series []client.NativeResourcesSeries) []client.NativeResourcesSeries {
	type target struct{ job, instance string }
	answers := map[target][][]client.NativeResourceVersion{}
	coalesced := map[target]bool{}
	for _, s := range series {
		var t target
		for _, l := range s.Labels {
			switch l.Name {
			case "job":
				t.job = l.Value
			case "instance":
				t.instance = l.Value
			}
		}
		answers[t] = append(answers[t], s.Versions)
		coalesced[t] = coalesced[t] || s.Coalesced
	}
	out := make([]client.NativeResourcesSeries, 0, len(answers))
	for t, versions := range answers {
		var lbls []mimirpb.LabelAdapter
		if t.instance != "" {
			lbls = append(lbls, mimirpb.LabelAdapter{Name: "instance", Value: t.instance})
		}
		if t.job != "" {
			lbls = append(lbls, mimirpb.LabelAdapter{Name: "job", Value: t.job})
		}
		out = append(out, client.NativeResourcesSeries{Labels: lbls, Versions: client.MergeNativeResourceVersions(versions), Coalesced: coalesced[t]})
	}
	slices.SortFunc(out, func(a, b client.NativeResourcesSeries) int {
		return labels.Compare(mimirpb.FromLabelAdaptersToLabels(a.Labels), mimirpb.FromLabelAdaptersToLabels(b.Labels))
	})
	return out
}
