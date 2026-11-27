// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/grafana/dskit/tenant"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/common/model"
	"github.com/prometheus/otlptranslator"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/distributor"
	ingester_client "github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	querierapi "github.com/grafana/mimir/pkg/querier/api"
	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/storage/series"
	"github.com/grafana/mimir/pkg/util/propagation"
)

// The native metadata prototype's native target_info: info() function's target_info series, synthesized from the
// ingesters' native resources rather than read from the stored series the OTLP conversion writes. It follows the
// conversion's rules, with two documented differences: a version's synthesized series has no gaps where the stored
// one would, and identities that collapse onto one job and instance resolve to the later change point.

// NativeTargetInfoExtractor carries api.NativeTargetInfoHeader from a request to its context.
type NativeTargetInfoExtractor struct{}

func (e *NativeTargetInfoExtractor) ExtractFromCarrier(ctx context.Context, carrier propagation.Carrier) (context.Context, error) {
	switch value := carrier.Get(querierapi.NativeTargetInfoHeader); value {
	case "":
		return ctx, nil
	case "native", "join":
		return querierapi.ContextWithNativeTargetInfo(ctx, value == "native"), nil
	default:
		return nil, apierror.Newf(apierror.TypeBadData, "invalid value '%s' for '%s' header, must be 'native' or 'join'", value, querierapi.NativeTargetInfoHeader)
	}
}

// NativeTargetInfoLimits are the tenant settings native target_info follows.
type NativeTargetInfoLimits interface {
	NativeResourcesEnabled(userID string) bool
	OTelKeepIdentifyingResourceAttributes(userID string) bool
	OTelTranslationStrategy(userID string) otlptranslator.TranslationStrategyOption
	OTelLabelNameUnderscoreSanitization(userID string) bool
	OTelLabelNamePreserveMultipleUnderscores(userID string) bool
	NativeInfoOwnResourcesEnabled(userID string) bool
}

// The reasons a query with native target_info falls back to the join, by the fallback counter's reason label.
const (
	nativeTargetInfoFallbackDisabled  = "disabled"
	nativeTargetInfoFallbackLimited   = "limited"
	nativeTargetInfoFallbackCap       = "cap"
	nativeTargetInfoFallbackUnbounded = "unbounded"
)

// NativeTargetInfoMetrics count native target_info's lookups and fallbacks.
type NativeTargetInfoMetrics struct {
	lookups   prometheus.Counter
	fallbacks *prometheus.CounterVec
	// The target_info series with native samples, which the lookups synthesized.
	synthesized prometheus.Counter
	// info() inner series in own mode, by what enriched them.
	innerSeries *prometheus.CounterVec
}

func NewNativeTargetInfoMetrics(reg prometheus.Registerer) *NativeTargetInfoMetrics {
	return &NativeTargetInfoMetrics{
		lookups: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_querier_native_target_info_lookups_total",
			Help: "Selects of target_info that info() answered from the ingesters' native resources.",
		}),
		fallbacks: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_querier_native_target_info_fallbacks_total",
			Help: "Selects of target_info that info() answered from the stored target_info instead of the ingesters' native resources, by reason.",
		}, []string{"reason"}),
		synthesized: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_querier_native_target_info_series_synthesized_total",
			Help: "Series of target_info with samples that info() synthesized from the ingesters' native resources.",
		}),
		innerSeries: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_querier_native_info_inner_series_total",
			Help: "Inner series of info() in own mode, by source: own, for those enriched from their own native resources, or the reason they took the stored target_info instead.",
		}, []string{"source"}),
	}
}

// NewNativeTargetInfoQueryable returns a Queryable for info() function's info series: target_info from the ingesters'
// native resources, for tenants with native resources, unless the request asks for the join, and every other info
// series from next. Before the lookup's coverage starts, target_info is next's.
func NewNativeTargetInfoQueryable(next storage.Queryable, d NativeMetadataDistributor, limits NativeTargetInfoLimits, lookbackDelta time.Duration, metrics *NativeTargetInfoMetrics) storage.Queryable {
	return storage.QueryableFunc(func(mint, maxt int64) (storage.Querier, error) {
		querier, err := next.Querier(mint, maxt)
		if err != nil {
			return nil, err
		}
		return &nativeTargetInfoQuerier{Querier: querier, distributor: d, limits: limits, interval: lookbackDelta / 2, metrics: metrics}, nil
	})
}

type nativeTargetInfoQuerier struct {
	storage.Querier
	distributor NativeMetadataDistributor
	limits      NativeTargetInfoLimits
	// The synthesized samples' interval, as the OTLP conversion's: half the lookback delta, so that a step always
	// finds one.
	interval time.Duration
	metrics  *NativeTargetInfoMetrics
}

const targetInfoMetricName = "target_info"

func (q *nativeTargetInfoQuerier) Select(ctx context.Context, sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	selectsTargetInfo, onlyTargetInfo := selectsMetricName(matchers, targetInfoMetricName)
	if !selectsTargetInfo || hints == nil {
		return q.Querier.Select(ctx, sortSeries, hints, matchers...)
	}
	if querierapi.NativeInfoOwnModeFromContext(ctx) {
		// info()'s own mode enriches what it can from the series' own resources, which it looks up itself, and the
		// rest by the join, from the stored series.
		return q.Querier.Select(ctx, sortSeries, hints, matchers...)
	}
	tenantID, err := tenant.TenantID(ctx)
	if err != nil {
		return storage.ErrSeriesSet(err)
	}
	if native, set := querierapi.NativeTargetInfoFromContext(ctx); (set && !native) || (!set && !q.limits.NativeResourcesEnabled(tenantID)) {
		return q.Querier.Select(ctx, sortSeries, hints, matchers...)
	}

	native, cutover, reason, err := q.nativeTargetInfo(ctx, tenantID, hints, matchers)
	if err != nil {
		return storage.ErrSeriesSet(err)
	}
	if reason != "" {
		q.metrics.fallbacks.WithLabelValues(reason).Inc()
		// Per query too, so that a caller comparing native with the join tells a native answer that is the join.
		stats.FromContext(ctx).AddNativeTargetInfoFallbacks(1)
		return q.Querier.Select(ctx, sortSeries, hints, matchers...)
	}
	q.metrics.lookups.Inc()

	sets := make([]storage.SeriesSet, 0, 2)
	stitched, err := q.stitch(ctx, hints, matchers, native, cutover)
	if err != nil {
		return storage.ErrSeriesSet(err)
	}
	sets = append(sets, stitched)
	if !onlyTargetInfo {
		// The other info series the select matches: distributors keep writing target_info, which mustn't come in twice.
		others := append(slices.Clone(matchers), labels.MustNewMatcher(labels.MatchNotEqual, labels.MetricName, targetInfoMetricName))
		sets = append(sets, q.Querier.Select(ctx, true, hints, others...))
	}
	return storage.NewMergeSeriesSet(sets, 0, storage.ChainedSeriesMerge)
}

// NativeInfoOwnMode reports whether the query's info() takes its inner series' own resources: the tenant's switch is
// on, and the request reads target_info natively, as Select would.
func (q *nativeTargetInfoQuerier) NativeInfoOwnMode(ctx context.Context) bool {
	tenantID, err := tenant.TenantID(ctx)
	if err != nil {
		return false
	}
	if native, set := querierapi.NativeTargetInfoFromContext(ctx); (set && !native) || (!set && !q.limits.NativeResourcesEnabled(tenantID)) {
		return false
	}
	return q.limits.NativeInfoOwnResourcesEnabled(tenantID)
}

// NativeSeriesResources looks up the native resources of the series matchers select, over hints' range, and converts
// each version to the labels of the target_info series the OTLP conversion writes for it. A lookup the ingesters
// refuse, disabled or over a cap, counts as a fallback, as Select's does, and isn't ok. Its answers count against the
// query's memory until the returned resources' Release.
func (q *nativeTargetInfoQuerier) NativeSeriesResources(ctx context.Context, hints *storage.SelectHints, matchers ...*labels.Matcher) (querierapi.NativeSeriesResources, bool, error) {
	tenantID, err := tenant.TenantID(ctx)
	if err != nil {
		return querierapi.NativeSeriesResources{}, false, err
	}
	res, release, err := q.distributor.NativeSeriesResources(ctx, [][]*labels.Matcher{matchers}, hints.Start, hints.End)
	fallback := func(reason string) (querierapi.NativeSeriesResources, bool, error) {
		q.metrics.fallbacks.WithLabelValues(reason).Inc()
		stats.FromContext(ctx).AddNativeTargetInfoFallbacks(1)
		return querierapi.NativeSeriesResources{}, false, nil
	}
	switch {
	case ingester_client.IsNativeMetadataDisabled(err):
		return fallback(nativeTargetInfoFallbackDisabled)
	case ingester_client.IsNativeLookupTooManySeries(err):
		return fallback(nativeTargetInfoFallbackCap)
	case err != nil:
		return querierapi.NativeSeriesResources{}, false, err
	}
	q.metrics.lookups.Inc()

	translation := distributor.TargetInfoTranslation{
		AllowUTF8:                   !q.limits.OTelTranslationStrategy(tenantID).ShouldEscape(),
		UnderscoreSanitization:      q.limits.OTelLabelNameUnderscoreSanitization(tenantID),
		PreserveMultipleUnderscores: q.limits.OTelLabelNamePreserveMultipleUnderscores(tenantID),
	}
	keepIdentifying := q.limits.OTelKeepIdentifyingResourceAttributes(tenantID)
	// A resource's versions repeat over its series, and each takes a conversion to label.
	labeled := map[string]labels.Labels{}
	out := make([]querierapi.NativeSeriesResource, 0, len(res.Series))
	for _, s := range res.Series {
		versions := make([]querierapi.NativeSeriesResourceVersion, 0, len(s.Versions))
		for _, v := range s.Versions {
			key := resourceKey(v)
			lbls, ok := labeled[key]
			if !ok {
				l, written, err := distributor.TargetInfoLabels(ctx, v.Identifying, v.Descriptive, keepIdentifying, translation)
				if err != nil {
					release()
					return querierapi.NativeSeriesResources{}, false, err
				}
				if !written {
					l = labels.EmptyLabels()
				}
				labeled[key], lbls = l, l
			}
			versions = append(versions, querierapi.NativeSeriesResourceVersion{MinTime: v.MinTimeMs, MaxTime: v.MaxTimeMs, Labels: lbls})
		}
		out = append(out, querierapi.NativeSeriesResource{Labels: mimirpb.FromLabelAdaptersToLabels(s.Labels), Versions: versions})
	}
	return querierapi.NativeSeriesResources{Series: out, CoverageStart: res.CoverageStart, Release: release}, true, nil
}

// ObserveNativeInfoInnerSeries counts count of info()'s inner series in own mode by what enriched them.
func (q *nativeTargetInfoQuerier) ObserveNativeInfoInnerSeries(source string, count int) {
	q.metrics.innerSeries.WithLabelValues(source).Add(float64(count))
}

// selectsMetricName reports whether matchers select series named name, and whether only those.
func selectsMetricName(matchers []*labels.Matcher, name string) (selects, only bool) {
	selects = true
	for _, m := range matchers {
		if m.Name != labels.MetricName {
			continue
		}
		selects = selects && m.Matches(name)
		only = only || (m.Type == labels.MatchEqual && m.Value == name)
	}
	return selects, selects && only
}

// nativeTargetInfo synthesizes the target_info series in force during the select's range from the ingesters' native
// resources of the targets that the select's job and instance matchers match. It returns them by label set, with the
// lookup's coverage start, or the reason to fall back to the join.
func (q *nativeTargetInfoQuerier) nativeTargetInfo(ctx context.Context, tenantID string, hints *storage.SelectHints, matchers []*labels.Matcher) (map[string]*nativeTargetInfoSeries, int64, string, error) {
	// info() gives the select the job and instance of its inner series, which find the targets; any other matcher is
	// on the info series' data labels.
	var lookup, dataLabels []*labels.Matcher
	for _, m := range matchers {
		switch m.Name {
		case labels.MetricName:
		case model.JobLabel, model.InstanceLabel:
			lookup = append(lookup, m)
		default:
			dataLabels = append(dataLabels, m)
		}
	}
	// A lookup has to narrow down the series, which a matcher that matches the empty string doesn't.
	if !slices.ContainsFunc(lookup, func(m *labels.Matcher) bool { return !m.Matches("") }) {
		return nil, 0, nativeTargetInfoFallbackUnbounded, nil
	}

	res, err := q.distributor.NativeResources(ctx, [][]*labels.Matcher{lookup}, hints.Start, hints.End, 0, true)
	switch {
	case ingester_client.IsNativeMetadataDisabled(err):
		return nil, 0, nativeTargetInfoFallbackDisabled, nil
	case ingester_client.IsNativeLookupTooManySeries(err):
		return nil, 0, nativeTargetInfoFallbackCap, nil
	case err != nil:
		return nil, 0, "", err
	case res.Limited:
		return nil, 0, nativeTargetInfoFallbackLimited, nil
	}

	translation := distributor.TargetInfoTranslation{
		AllowUTF8:                   !q.limits.OTelTranslationStrategy(tenantID).ShouldEscape(),
		UnderscoreSanitization:      q.limits.OTelLabelNameUnderscoreSanitization(tenantID),
		PreserveMultipleUnderscores: q.limits.OTelLabelNamePreserveMultipleUnderscores(tenantID),
	}
	keepIdentifying := q.limits.OTelKeepIdentifyingResourceAttributes(tenantID)
	// Versions repeat over targets and time, and each takes a conversion to label.
	labeled := map[string]labels.Labels{}
	out := map[string]*nativeTargetInfoSeries{}
	// Native samples start at the cutover: the stored target_info is in force before it.
	from := max(hints.Start, res.CoverageStart)
	for _, target := range res.Series {
		for _, v := range target.Versions {
			if v.MaxTimeMs < from || v.MinTimeMs > hints.End {
				continue
			}
			key := resourceKey(v)
			lbls, ok := labeled[key]
			if !ok {
				l, written, err := distributor.TargetInfoLabels(ctx, v.Identifying, v.Descriptive, keepIdentifying, translation)
				if err != nil {
					return nil, 0, "", err
				}
				if !written {
					l = labels.EmptyLabels()
				}
				labeled[key], lbls = l, l
			}
			if lbls.IsEmpty() || !matchesAll(dataLabels, lbls) {
				continue
			}
			s, ok := out[lbls.String()]
			if !ok {
				s = &nativeTargetInfoSeries{labels: lbls}
				out[lbls.String()] = s
			}
			s.samples = appendTargetInfoSamples(s.samples, v.MinTimeMs, v.MaxTimeMs, from, hints.End, q.interval.Milliseconds())
		}
	}
	return out, res.CoverageStart, "", nil
}

type nativeTargetInfoSeries struct {
	labels  labels.Labels
	samples []model.SamplePair
}

// appendTargetInfoSamples appends a version's synthesized samples within [from, to]: at its start, every interval
// after it, and at its end, as the OTLP conversion writes target_info over a request's samples. Versions don't
// overlap, and come in order, so a series' samples stay sorted.
func appendTargetInfoSamples(samples []model.SamplePair, minTime, maxTime, from, to, interval int64) []model.SamplePair {
	add := func(t int64) {
		if t >= from && t <= to && (len(samples) == 0 || int64(samples[len(samples)-1].Timestamp) < t) {
			samples = append(samples, model.SamplePair{Timestamp: model.Time(t), Value: 1})
		}
	}
	t := minTime
	if interval > 0 && from > minTime {
		// Skip the samples before from: the select's start already has the lookback before its first step.
		t = minTime + (from-minTime)/interval*interval
	}
	for ; t < maxTime && t <= to; t += max(interval, 1) {
		add(t)
	}
	add(maxTime)
	return samples
}

// stitch returns one series per label set: the stored target_info's samples before the cutover, then the native
// ones from it. A lookback after the cutover bridges to the last stored sample, as the TSDB arm's first change point
// after a restart is each series' first sample since it reopened.
func (q *nativeTargetInfoQuerier) stitch(ctx context.Context, hints *storage.SelectHints, matchers []*labels.Matcher, native map[string]*nativeTargetInfoSeries, cutover int64) (storage.SeriesSet, error) {
	stored := map[string][]model.SamplePair{}
	if cutover > hints.Start {
		storedHints := *hints
		storedHints.End = min(hints.End, cutover-1)
		targetInfo := append(slices.Clone(matchers), labels.MustNewMatcher(labels.MatchEqual, labels.MetricName, targetInfoMetricName))
		set := q.Querier.Select(ctx, false, &storedHints, targetInfo...)
		var it chunkenc.Iterator
		for set.Next() {
			s := set.At()
			it = s.Iterator(it)
			var samples []model.SamplePair
			for vt := it.Next(); vt != chunkenc.ValNone; vt = it.Next() {
				if vt != chunkenc.ValFloat {
					continue
				}
				t, v := it.At()
				if t >= cutover {
					break
				}
				samples = append(samples, model.SamplePair{Timestamp: model.Time(t), Value: model.SampleValue(v)})
			}
			if err := it.Err(); err != nil {
				return nil, err
			}
			key := s.Labels().String()
			if _, ok := native[key]; !ok {
				native[key] = &nativeTargetInfoSeries{labels: s.Labels().Copy()}
			}
			stored[key] = samples
		}
		if err := set.Err(); err != nil {
			return nil, err
		}
	}
	// Synthesized series aren't storage's, whose labels storage accounts for until the query reads them: the query
	// accounts for the labels it keeps itself.
	out := make([]storage.Series, 0, len(native))
	for key, s := range native {
		if len(s.samples) > 0 {
			q.metrics.synthesized.Inc()
		}
		samples := s.samples
		if before := stored[key]; len(before) > 0 {
			samples = append(before, samples...)
		}
		if len(samples) > 0 {
			out = append(out, series.NewConcreteSeries(s.labels, samples, nil))
		}
	}
	return series.NewConcreteSeriesSetFromUnsortedSeries(out), nil
}

func matchesAll(matchers []*labels.Matcher, lbls labels.Labels) bool {
	for _, m := range matchers {
		if !m.Matches(lbls.Get(m.Name)) {
			return false
		}
	}
	return true
}

// resourceKey identifies a version's resource by its attributes.
func resourceKey(v ingester_client.NativeResourceVersion) string {
	var b strings.Builder
	for _, attrs := range []map[string]string{v.Identifying, v.Descriptive} {
		keys := make([]string, 0, len(attrs))
		for k := range attrs {
			keys = append(keys, k)
		}
		slices.Sort(keys)
		for _, k := range keys {
			fmt.Fprintf(&b, "%q=%q,", k, attrs[k])
		}
		b.WriteByte('|')
	}
	return b.String()
}
