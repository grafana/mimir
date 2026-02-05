// SPDX-License-Identifier: AGPL-3.0-only

package labelaccess

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/user"
	"github.com/grafana/mimir/pkg/util/spanlogger"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/util/annotations"
	"go.opentelemetry.io/otel"
)

var tracer = otel.Tracer("pkg/labelaccess")

// WrapQueryable wraps the provided queryable with a labelAccessQueryable.
func WrapQueryable(next storage.SampleAndChunkQueryable, logger log.Logger) storage.SampleAndChunkQueryable {
	return &labelAccessQueryable{next, logger}
}

// promSelector encompasses multiple Prometheus label Matchers, translated from label policy types.
//
// Uses of promSelector are usually slices of them, each slice representing an LBAC policy. The slices
// are unioned together while the matchers within each selector are an intersection. The effect of this
// is that results (samples, exemplars) must match all matchers in a promSelector but may match any
// promSelector instance.
type promSelector []*labels.Matcher

// matches returns whether the labels satisfy all matchers.
func (s promSelector) matches(labels labels.Labels) bool {
	for _, m := range s {
		if v := labels.Get(m.Name); !m.Matches(v) {
			return false
		}
	}
	return true
}

func (s promSelector) String() string {
	matcherStrings := make([]string, len(s))
	for i, m := range s {
		matcherStrings[i] = m.String()
	}

	return "{" + strings.Join(matcherStrings, ", ") + "}"
}

// labelPoliciesToPromSelectors converts label policy types to groups of Prometheus matchers.
func labelPoliciesToPromSelectors(policies []*LabelPolicy) []promSelector {
	var selectors []promSelector
	for _, p := range policies {
		var selector []*labels.Matcher
		selector = append(selector, p.Selector...)
		selectors = append(selectors, selector)
	}

	return selectors
}

type labelAccessQueryable struct {
	next   storage.SampleAndChunkQueryable
	logger log.Logger
}

// Querier returns a new Querier on the storage. Fulfils the storage.Queryable interface.
func (l *labelAccessQueryable) Querier(mint int64, maxt int64) (storage.Querier, error) {
	nextQuerier, err := l.next.Querier(mint, maxt)
	if err != nil {
		return nil, err
	}

	return &labelAccessQuerier{
		labelQuerier: nextQuerier,
		querier:      nextQuerier,
		logger:       l.logger,
	}, nil
}

// ChunkQuerier provides querying access over time series data of a fixed time range with labels matching the
// specified selectors. Fulfills the storage.ChunkQueryable interface.
func (l *labelAccessQueryable) ChunkQuerier(mint, maxt int64) (storage.ChunkQuerier, error) {
	nextQuerier, err := l.next.ChunkQuerier(mint, maxt)
	if err != nil {
		return nil, err
	}

	return &labelAccessChunkQuerier{
		labelAccessQuerier: labelAccessQuerier{
			labelQuerier: nextQuerier,
			logger:       l.logger,
		},
		next: nextQuerier,
	}, nil
}

type labelAccessQuerier struct {
	querier      storage.Querier
	labelQuerier storage.LabelQuerier // Separate field to allow reuse in labelAccessChunkQuerier.
	logger       log.Logger
}

// LabelValues returns all potential values for a label name.
// It is not safe to use the strings beyond the lifetime of the querier.
func (l *labelAccessQuerier) LabelValues(ctx context.Context, name string, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	spanlog, _ := spanlogger.New(ctx, l.logger, tracer, "labelAccessQuerier.LabelValues")
	defer spanlog.Finish()

	selectors, err := l.selectors(ctx)
	if err != nil {
		return nil, nil, err
	}

	// There are no LBAC selectors that we have to enforce, just delegate to upstream.
	if len(selectors) == 0 {
		spanlog.DebugLog("msg", "no LBAC selectors, delegating to upstream")
		return l.labelQuerier.LabelValues(ctx, name, hints, matchers...)
	}

	// If there's only a single selector we don't need to call the upstream LabelValues method
	// multiple times and union the results together, we can just append our LBAC matchers
	// directly to the matchers already being passed upstream and return.
	if len(selectors) == 1 {
		spanlog.DebugLog("msg", "label values filtered by single LBAC selector as upstream matchers", "selector", selectors[0])
		matchers = l.mergeMatchers(matchers, selectors[0])
		return l.labelQuerier.LabelValues(ctx, name, hints, matchers...)
	}

	unique := make(map[string]struct{})
	// LBAC policies can be applied to any labels associated with a series. Because of this we
	// need to include the matchers from each distinct LBAC selector in the call to the upstream
	// querier because we don't have any other way to make sure they are applied to the various
	// labels they might reference. I.e. we can't just select all label values and filter them
	// afterwards like we do for series or exemplars.
	for _, selector := range selectors {
		// We repeat the call to the upstream label querier for each LBAC selector we have since
		// LBAC selectors are unioned together but the results of an individual call must be the
		// intersection of all matchers. The only way to achieve this is to merge each of our
		// selectors with the matchers provided to the upstream querier.
		ourMatchers := make([]*labels.Matcher, 0, len(matchers)+len(selector))
		ourMatchers = append(ourMatchers, matchers...)
		ourMatchers = l.mergeMatchers(ourMatchers, selector)
		lbls, _, err := l.labelQuerier.LabelValues(ctx, name, hints, ourMatchers...)

		// Return an error here immediately even if there are some partial values we could return
		// to callers. We'd rather return an error so that callers could retry instead of partial
		// (incorrect) results.
		if err != nil {
			return nil, nil, fmt.Errorf("unable to get label values for %s with selector %s: %w", name, selector, err)
		}

		spanlog.DebugLog("msg", "label values filtered by LBAC selector", "name", name, "selector", selector, "results", len(lbls))
		for _, lbl := range lbls {
			unique[lbl] = struct{}{}
		}
	}

	values := make([]string, 0, len(unique))
	for k := range unique {
		values = append(values, k)
	}

	return values, nil, nil
}

// LabelNames returns all the unique label names present in the block in sorted order.
func (l *labelAccessQuerier) LabelNames(ctx context.Context, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	spanlog, _ := spanlogger.New(ctx, l.logger, tracer, "labelAccessQuerier.LabelNames")
	defer spanlog.Finish()

	selectors, err := l.selectors(ctx)
	if err != nil {
		return nil, nil, err
	}

	// There are no LBAC selectors that we have to enforce, just delegate to upstream.
	if len(selectors) == 0 {
		spanlog.DebugLog("msg", "no LBAC selectors, delegating to upstream")
		return l.labelQuerier.LabelNames(ctx, hints, matchers...)
	}

	// If there's only a single selector we don't need to call the upstream LabelNames method
	// multiple times and union the results together, we can just append our LBAC matchers
	// directly to the matchers already being passed upstream and return.
	if len(selectors) == 1 {
		spanlog.DebugLog("msg", "label names filtered by single LBAC selector as upstream matchers", "selector", selectors[0])
		matchers = l.mergeMatchers(matchers, selectors[0])
		return l.labelQuerier.LabelNames(ctx, hints, matchers...)
	}

	unique := make(map[string]struct{})
	// LBAC policies can be applied to any labels associated with a series. Because of this we
	// need to include the matchers from each distinct LBAC selector in the call to the upstream
	// querier because we don't have any other way to make sure they are applied to the various
	// labels they might reference. I.e. we can't just select all label names and filter them
	// afterwards like we do for series or exemplars.
	for _, selector := range selectors {
		// We repeat the call to the upstream label querier for each LBAC selector we have since
		// LBAC selectors are unioned together but the results of an individual call must be the
		// intersection of all matchers. The only way to achieve this is to merge each of our
		// selectors with the matchers provided to the upstream querier.
		ourMatchers := make([]*labels.Matcher, 0, len(matchers)+len(selector))
		ourMatchers = append(ourMatchers, matchers...)
		ourMatchers = l.mergeMatchers(ourMatchers, selector)
		lbls, _, err := l.labelQuerier.LabelNames(ctx, hints, ourMatchers...)

		// Return an error here immediately even if there are some partial values we could return
		// to callers. We'd rather return an error so that callers could retry instead of partial
		// (incorrect) results.
		if err != nil {
			return nil, nil, fmt.Errorf("unable to get label names with selector %s: %w", selector, err)
		}

		spanlog.DebugLog("msg", "label names filtered by LBAC selector", "selector", selector, "results", len(lbls))
		for _, lbl := range lbls {
			unique[lbl] = struct{}{}
		}
	}

	names := make([]string, 0, len(unique))
	for k := range unique {
		names = append(names, k)
	}
	// Label names are defined to be sorted in the Querier interface.
	// Note: we could potentially avoid this sort by taking advantage of the fact that the
	// upstream querier is required to return sorted label names if performance becomes a concern.
	sort.Strings(names)

	return names, nil, nil
}

// Close releases the resources of the Querier.
func (l *labelAccessQuerier) Close() error {
	return l.querier.Close()
}

// Select returns a set of series that matches the given label matchers and match one of the policy selectors.
func (l *labelAccessQuerier) Select(
	ctx context.Context,
	sortSeries bool,
	hints *storage.SelectHints,
	matchers ...*labels.Matcher,
) storage.SeriesSet {
	spanlog, _ := spanlogger.New(ctx, l.logger, tracer, "labelAccessQuerier.Select")
	defer spanlog.Finish()

	selectors, err := l.selectors(ctx)
	if err != nil {
		return storage.ErrSeriesSet(err)
	}

	if len(selectors) == 0 {
		spanlog.DebugLog("msg", "no LBAC selectors, delegating to upstream")
		return l.querier.Select(ctx, sortSeries, hints, matchers...)
	}

	// If there is only one LBAC policy attached, then we can skip selecting everything and then filtering.
	// We can just add the LBAC selectors to matchers and select only what is required.
	if len(selectors) == 1 {
		spanlog.DebugLog("msg", "series filtered by single LBAC selector as upstream matchers", "selector", selectors[0])
		matchers = l.mergeMatchers(matchers, selectors[0])
		return l.querier.Select(ctx, sortSeries, hints, matchers...)
	}

	return &labelAccessSeriesSet{
		selectors: selectors,
		upstream:  l.querier.Select(ctx, sortSeries, hints, matchers...),
		logger:    l.logger,
	}
}

type hashableMatcher struct {
	Type  labels.MatchType
	Name  string
	Value string
}

// mergeMatchers appends non-duplicate LBAC matchers to the input matchers and returns the input slice.
func (l *labelAccessQuerier) mergeMatchers(matchers []*labels.Matcher, lbacMatchers []*labels.Matcher) []*labels.Matcher {
	unique := make(map[hashableMatcher]struct{})
	for _, m := range matchers {
		unique[hashableMatcher{
			Type:  m.Type,
			Name:  m.Name,
			Value: m.Value,
		}] = struct{}{}
	}

	for _, lbac := range lbacMatchers {
		if _, ok := unique[hashableMatcher{
			Type:  lbac.Type,
			Name:  lbac.Name,
			Value: lbac.Value,
		}]; !ok {
			matchers = append(matchers, lbac)
		}
	}

	return matchers
}

// selectors attempts to construct promSelectors based on the tenant name in the context.
func (l *labelAccessQuerier) selectors(ctx context.Context) ([]promSelector, error) {
	instancePolicyMap, err := ExtractLabelMatchersContext(ctx)
	if err != nil {
		// If there are no policies in the context, that's fine - just return empty selectors.
		// This means no filtering will be applied.
		if err == errNoMatcherSource {
			return nil, nil
		}
		level.Error(l.logger).Log("msg", "unable to find instance policy map", "err", err)
		return nil, err
	}

	instanceName, err := user.ExtractOrgID(ctx)
	if err != nil {
		level.Error(l.logger).Log("msg", "unable to find instance name", "err", err)
		return nil, err
	}

	policies, exists := instancePolicyMap[instanceName]
	if !exists {
		level.Info(l.logger).Log("msg", "no policies found for tenant", "instance_name", instanceName)
		return nil, nil
	}

	level.Info(l.logger).Log("msg", "enforcing label policy on query", "instance_name", instanceName, "num_policies", len(policies))
	return labelPoliciesToPromSelectors(policies), nil
}

type labelAccessSeriesSet struct {
	selectors []promSelector
	upstream  storage.SeriesSet
	logger    log.Logger
	curSeries storage.Series
}

// Next iterates through the upstream series set and returns true for the next series that is found that matches the
// specified set of selectors.
func (l *labelAccessSeriesSet) Next() bool {
	var seriesToSkip []storage.Series

	for l.upstream.Next() {
		l.curSeries = l.upstream.At()
		lbls := l.curSeries.Labels()
		for _, s := range l.selectors {
			if s.matches(lbls) {
				level.Debug(l.logger).Log("msg", "label match found on query", "selector", s, "series", lbls)

				if len(seriesToSkip) > 0 {
					// When ingester to querier or store-gateway to querier chunks streaming is enabled with LBAC enabled,
					// if we want to omit a series from a result because it does not match the access policy, we still need
					// to create the iterator for the forbidden series. This is because creating the iterator triggers reading
					// the chunks for the forbidden series, and if we don't read the chunks for the forbidden series, we'll get out
					// of sync with the stream of chunks and get errors like:
					//   execution: attempted to read series at index 1 from ingester chunks stream, but the stream has series with index 0
					l.curSeries = &skipPreviousSeries{
						seriesToSkip: seriesToSkip,
						inner:        l.curSeries,
					}
				}

				return true
			}

			level.Debug(l.logger).Log("msg", "label match not found on query", "selector", s, "series", lbls)
		}

		seriesToSkip = append(seriesToSkip, l.curSeries)
	}
	l.curSeries = nil
	return false
}

// At returns the current upstream series.
func (l *labelAccessSeriesSet) At() storage.Series {
	return l.curSeries
}

// Err returns the upstream series set error.
func (l *labelAccessSeriesSet) Err() error {
	return l.upstream.Err()
}

// Warnings returns any warning from the upstream series set.
func (l *labelAccessSeriesSet) Warnings() annotations.Annotations {
	return l.upstream.Warnings()
}

type skipPreviousSeries struct {
	seriesToSkip []storage.Series
	inner        storage.Series
}

func (s *skipPreviousSeries) Labels() labels.Labels {
	return s.inner.Labels()
}

func (s *skipPreviousSeries) Iterator(iterator chunkenc.Iterator) chunkenc.Iterator {
	for _, series := range s.seriesToSkip {
		iterator = series.Iterator(iterator)
	}

	return s.inner.Iterator(iterator)
}

type labelAccessChunkQuerier struct {
	labelAccessQuerier
	next storage.ChunkQuerier
}

// Close releases the resources of the Querier.
func (l *labelAccessChunkQuerier) Close() error {
	return l.next.Close()
}

// Select returns a set of series that matches the given label matchers.
func (l *labelAccessChunkQuerier) Select(
	ctx context.Context,
	sortSeries bool,
	hints *storage.SelectHints,
	matchers ...*labels.Matcher,
) storage.ChunkSeriesSet {
	selectors, err := l.selectors(ctx)
	if err != nil {
		return storage.ErrChunkSeriesSet(err)
	}

	if len(selectors) == 0 {
		return l.next.Select(ctx, sortSeries, hints, matchers...)
	}

	return &labelAccessChunkSeriesSet{
		selectors: selectors,
		upstream:  l.next.Select(ctx, sortSeries, hints, matchers...),
		logger:    l.logger,
	}
}

type labelAccessChunkSeriesSet struct {
	selectors []promSelector
	upstream  storage.ChunkSeriesSet
	curSeries storage.ChunkSeries
	logger    log.Logger
}

// Next iterates through the upstream series set and returns true for the next series that is found that matches the
// specified set of selectors.
func (l *labelAccessChunkSeriesSet) Next() bool {
	for l.upstream.Next() {

		l.curSeries = l.upstream.At()
		for _, s := range l.selectors {
			if s.matches(l.curSeries.Labels()) {
				return true
			}
		}
	}
	l.curSeries = nil
	return false
}

// At returns the current upstream series.
func (l *labelAccessChunkSeriesSet) At() storage.ChunkSeries {
	return l.curSeries
}

// Err returns the upstream series set error.
func (l *labelAccessChunkSeriesSet) Err() error {
	return l.upstream.Err()
}

// Warnings returns any warning from the upstream series set.
func (l *labelAccessChunkSeriesSet) Warnings() annotations.Annotations {
	return l.upstream.Warnings()
}
