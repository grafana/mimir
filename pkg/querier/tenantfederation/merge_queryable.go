// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/tenantfederation/merge_queryable.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package tenantfederation

import (
	"context"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/concurrency"
	"github.com/grafana/dskit/tenant"
	"github.com/grafana/dskit/user"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/util/spanlogger"
)

// NewQueryable returns a queryable that iterates through all the tenant IDs
// that are part of the request and aggregates the query results for tenant.
// By setting bypassWithSingleID to true the mergeQuerier gets bypassed
// and results for requests with a single ID will not contain the
// "__tenant_id__" label. This allows for a smoother transition, when enabling
// tenant federation in a cluster.
// The result contains a label "__tenant_id__" to identify the tenant ID that
// it originally resulted from.
// If the label "__tenant_id__" already exists, its value is overwritten
// by the tenant ID and the previous value is exposed through a new label
// prefixed with "original_". This behaviour is not implemented recursively.
func NewQueryable(upstream storage.Queryable, bypassWithSingleID bool, maxConcurrency int, reg prometheus.Registerer, logger log.Logger) storage.Queryable {
	callbacks := MergeQueryableCallbacks{
		Querier: func(mint, maxt int64) (MergeQuerierUpstream, error) {
			q, err := upstream.Querier(mint, maxt)
			if err != nil {
				return nil, errors.Wrap(err, "construct querier")
			}

			return &tenantQuerier{
				upstream: q,
			}, nil
		},
	}
	return NewMergeQueryable(defaultTenantLabel, callbacks, tenant.NewMultiResolver(), bypassWithSingleID, maxConcurrency, reg, logger, nil)
}

// MergeQueryableCallbacks contains callbacks to NewMergeQueryable, for customizing its behaviour.
type MergeQueryableCallbacks struct {
	// Querier returns a MergeQuerierUpstream implementation for mint and maxt.
	Querier func(mint, maxt int64) (MergeQuerierUpstream, error)
}

// MergeQuerierUpstream mirrors storage.Querier, except every query method also takes a federation ID.
type MergeQuerierUpstream interface {
	Select(ctx context.Context, id string, sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet
	LabelValues(ctx context.Context, id string, name string, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error)
	LabelNames(ctx context.Context, id string, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error)
	Close() error
}

// tenantQuerier implements MergeQuerierUpstream, wrapping a storage.Querier.
// The federation ID gets injected into the context as a tenant ID.
type tenantQuerier struct {
	upstream storage.Querier
}

func (q *tenantQuerier) Select(ctx context.Context, id string, sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	return q.upstream.Select(user.InjectOrgID(ctx, id), sortSeries, hints, matchers...)
}

func (q *tenantQuerier) LabelValues(ctx context.Context, id string, name string, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return q.upstream.LabelValues(user.InjectOrgID(ctx, id), name, hints, matchers...)
}

func (q *tenantQuerier) LabelNames(ctx context.Context, id string, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return q.upstream.LabelNames(user.InjectOrgID(ctx, id), hints, matchers...)
}

func (q *tenantQuerier) Close() error {
	return q.upstream.Close()
}

// NewMergeQueryable returns a queryable that merges results for all involved
// federation IDs. The underlying querier is returned by a callback in
// MergeQueryableCallbacks.
//
// By setting bypassWithSingleID to true the mergeQuerier gets bypassed,
// and results for requests with a single ID will not contain the ID label.
// This allows for a smoother transition, when enabling tenant federation in a
// cluster.
//
// Each result contains a label `idLabelName` to identify the federation ID it originally resulted from.
// If the label `idLabelName` already exists, its value is overwritten and
// the previous value is exposed through a new label prefixed with "original_".
// This behaviour is not implemented recursively.
//
// Passing in multiTenantSelectFunc allows for customizing the behavior of the
// Select method that is used to query multiple tenants in parallel. When left
// nil, the default behavior is used.
func NewMergeQueryable(idLabelName string, callbacks MergeQueryableCallbacks, resolver tenant.Resolver, bypassWithSingleID bool, maxConcurrency int, reg prometheus.Registerer, logger log.Logger, multiTenantSelectFunc MultiTenantSelectFunc) storage.Queryable {
	if multiTenantSelectFunc == nil {
		multiTenantSelectFunc = defaultMultiTenantSelectFunc
	}
	metrics := mergeQueryableMetrics{
		tenantsQueried: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Name:    "cortex_querier_federation_tenants_queried",
			Help:    "Number of tenants queried for a single standard query.",
			Buckets: []float64{1, 2, 4, 8, 16, 32},
		}),

		// Experimental: Observe time to kick off upstream query jobs as a native histogram
		upstreamQueryWaitDuration: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Name:                            "cortex_querier_federation_upstream_query_wait_duration_seconds",
			Help:                            "Time spent waiting to run upstream queries",
			NativeHistogramBucketFactor:     1.1,
			NativeHistogramMaxBucketNumber:  100,
			NativeHistogramMinResetDuration: 1 * time.Hour,
		}),
	}
	// Note that we allow tenant.Resolver to be injected instead of using the
	// tenant.TenantIDs() method because GEM needs to inject different behavior
	// here for the cluster federation feature.
	return &mergeQueryable{
		logger:                logger,
		idLabelName:           idLabelName,
		callbacks:             callbacks,
		resolver:              resolver,
		bypassWithSingleID:    bypassWithSingleID,
		maxConcurrency:        maxConcurrency,
		multiTenantSelectFunc: multiTenantSelectFunc,
		mergeQueryableMetrics: metrics,
	}
}

type mergeQueryable struct {
	logger                log.Logger
	idLabelName           string
	bypassWithSingleID    bool
	callbacks             MergeQueryableCallbacks
	resolver              tenant.Resolver
	maxConcurrency        int
	multiTenantSelectFunc MultiTenantSelectFunc

	mergeQueryableMetrics
}

type mergeQueryableMetrics struct {
	tenantsQueried            prometheus.Histogram
	upstreamQueryWaitDuration prometheus.Histogram
}

// Querier returns a new mergeQuerier, which aggregates results for multiple federation IDs
// into a single result.
func (m *mergeQueryable) Querier(mint int64, maxt int64) (storage.Querier, error) {
	upstream, err := m.callbacks.Querier(mint, maxt)
	if err != nil {
		return nil, err
	}
	return &mergeQuerier{
		logger:                    m.logger,
		idLabelName:               m.idLabelName,
		callbacks:                 m.callbacks,
		resolver:                  m.resolver,
		upstream:                  upstream,
		maxConcurrency:            m.maxConcurrency,
		bypassWithSingleID:        m.bypassWithSingleID,
		tenantsQueried:            m.tenantsQueried,
		upstreamQueryWaitDuration: m.upstreamQueryWaitDuration,
		multiTenantSelectFunc:     m.multiTenantSelectFunc,
	}, nil
}

// mergeQuerier aggregates the results for involved federation IDs, and adds a
// label `idLabelName` to identify the ID each metric resulted from.
// If the label `idLabelName` already exists, its value is overwritten and
// the previous value is exposed through a new label prefixed with "original_".
// This behaviour is not implemented recursively
type mergeQuerier struct {
	logger                    log.Logger
	callbacks                 MergeQueryableCallbacks
	resolver                  tenant.Resolver
	upstream                  MergeQuerierUpstream
	idLabelName               string
	maxConcurrency            int
	bypassWithSingleID        bool
	tenantsQueried            prometheus.Histogram
	upstreamQueryWaitDuration prometheus.Histogram
	multiTenantSelectFunc     MultiTenantSelectFunc
}

// LabelValues returns all potential values for a label name given involved federation IDs.
// It is not safe to use the strings beyond the lifefime of the querier.
// For the label `idLabelName` it will return all the underlying IDs available.
// For the label "original_" + `idLabelName it will return all values
// for the original `idLabelName` label.
func (m *mergeQuerier) LabelValues(ctx context.Context, name string, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	ids, err := m.resolver.TenantIDs(ctx)
	if err != nil {
		return nil, nil, err
	}

	m.tenantsQueried.Observe(float64(len(ids)))
	if m.bypassWithSingleID && len(ids) == 1 {
		return m.upstream.LabelValues(ctx, ids[0], name, hints, matchers...)
	}

	spanlog, ctx := spanlogger.NewWithLogger(ctx, m.logger, "mergeQuerier.LabelValues")
	defer spanlog.Finish()

	matchedIDs, filteredMatchers := FilterValuesByMatchers(m.idLabelName, ids, matchers...)

	if name == m.idLabelName {
		labelValues := make([]string, 0, len(matchedIDs))
		for _, id := range ids {
			if _, matched := matchedIDs[id]; matched {
				labelValues = append(labelValues, id)
			}
		}
		return labelValues, nil, nil
	}

	// ensure the name of a retained label gets handled under the original
	// label name
	if name == retainExistingPrefix+m.idLabelName {
		name = m.idLabelName
	}

	return m.mergeDistinctStringSliceWithTenants(ctx, matchedIDs, func(ctx context.Context, id string) ([]string, annotations.Annotations, error) {
		return m.upstream.LabelValues(ctx, id, name, hints, filteredMatchers...)
	})
}

// LabelNames returns all the unique label names present for involved federation IDs.
// It also adds the `idLabelName` and if present in the original results the original `idLabelName`.
func (m *mergeQuerier) LabelNames(ctx context.Context, hints *storage.LabelHints, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	ids, err := m.resolver.TenantIDs(ctx)
	if err != nil {
		return nil, nil, err
	}

	m.tenantsQueried.Observe(float64(len(ids)))
	if m.bypassWithSingleID && len(ids) == 1 {
		return m.upstream.LabelNames(ctx, ids[0], hints, matchers...)
	}

	spanlog, ctx := spanlogger.NewWithLogger(ctx, m.logger, "mergeQuerier.LabelNames")
	defer spanlog.Finish()

	matchedIDs, filteredMatchers := FilterValuesByMatchers(m.idLabelName, ids, matchers...)

	labelNames, warnings, err := m.mergeDistinctStringSliceWithTenants(ctx, matchedIDs, func(ctx context.Context, id string) ([]string, annotations.Annotations, error) {
		return m.upstream.LabelNames(ctx, id, hints, filteredMatchers...)
	})
	if err != nil {
		return nil, nil, err
	}

	// check if the `idLabelName` exists in the original result
	var idLabelNameExists bool
	labelPos := sort.SearchStrings(labelNames, m.idLabelName)
	if labelPos < len(labelNames) && labelNames[labelPos] == m.idLabelName {
		idLabelNameExists = true
	}

	labelToAdd := m.idLabelName

	// if `idLabelName` already exists, we need to add the name prefix with
	// retainExistingPrefix.
	if idLabelNameExists {
		labelToAdd = retainExistingPrefix + m.idLabelName
		labelPos = sort.SearchStrings(labelNames, labelToAdd)
	}

	// insert label at the correct position
	labelNames = append(labelNames, "")
	copy(labelNames[labelPos+1:], labelNames[labelPos:])
	labelNames[labelPos] = labelToAdd

	return labelNames, warnings, nil
}

type stringSliceFunc func(context.Context, string) ([]string, annotations.Annotations, error)

type stringSliceFuncJob struct {
	id       string
	result   []string
	warnings annotations.Annotations
}

// mergeDistinctStringSliceWithTenants aggregates stringSliceFunc call
// results for provided tenants. It removes duplicates and sorts the result.
// It doesn't require the output of the stringSliceFunc to be sorted, as results
// of LabelValues are not sorted.
func (m *mergeQuerier) mergeDistinctStringSliceWithTenants(ctx context.Context, ids map[string]struct{}, f stringSliceFunc) ([]string, annotations.Annotations, error) {
	jobs := make([]*stringSliceFuncJob, 0, len(ids))
	for id := range ids {
		jobs = append(jobs, &stringSliceFuncJob{
			id: id,
		})
	}

	run := func(ctx context.Context, idx int) (err error) {
		job := jobs[idx]
		job.result, job.warnings, err = f(ctx, job.id)
		if err != nil {
			return errors.Wrapf(err, "error querying %s %s", rewriteLabelName(m.idLabelName), job.id)
		}

		return nil
	}

	err := concurrency.ForEachJob(ctx, len(jobs), m.maxConcurrency, run)
	if err != nil {
		return nil, nil, err
	}

	// aggregate warnings and deduplicate string results
	var warnings annotations.Annotations
	resultMap := make(map[string]struct{})
	for _, job := range jobs {
		for _, e := range job.result {
			resultMap[e] = struct{}{}
		}

		for _, w := range job.warnings {
			warnings.Add(errors.Wrapf(w, "warning querying %s %s", rewriteLabelName(m.idLabelName), job.id))
		}
	}

	result := make([]string, 0, len(resultMap))
	for e := range resultMap {
		result = append(result, e)
	}
	slices.Sort(result)
	return result, warnings, nil
}

// Close releases the resources of the Querier.
func (m *mergeQuerier) Close() error {
	return m.upstream.Close()
}

// Select returns a set of series that matches the given label matchers, given involved federation IDs.
// If the `idLabelName` is matched on, it only considers matching IDs.
// The forwarded labelSelector does not contain those that operate on `idLabelName`.
func (m *mergeQuerier) Select(ctx context.Context, sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	start := time.Now()
	ids, err := m.resolver.TenantIDs(ctx)
	if err != nil {
		return storage.ErrSeriesSet(err)
	}

	m.tenantsQueried.Observe(float64(len(ids)))
	if m.bypassWithSingleID && len(ids) == 1 {
		return m.upstream.Select(ctx, ids[0], sortSeries, hints, matchers...)
	}

	spanlog, ctx := spanlogger.NewWithLogger(ctx, m.logger, "mergeQuerier.Select")
	defer spanlog.Finish()

	matchedIDs, filteredMatchers := FilterValuesByMatchers(m.idLabelName, ids, matchers...)

	jobs := make([]string, 0, len(matchedIDs))
	for id := range matchedIDs {
		jobs = append(jobs, id)
	}

	wrMergeQuerier := WaitRecordingMergeQuerier{
		start:                     start,
		upstreamQueryWaitDuration: m.upstreamQueryWaitDuration,
		upstream:                  m.upstream,
	}

	return m.multiTenantSelectFunc(ctx, jobs, wrMergeQuerier, m.idLabelName, m.maxConcurrency, sortSeries, hints, filteredMatchers...)
}

// MultiTenantSelectFunc is an overrideable function that queries multiple tenants in parallel.
// By default, jobs would be the list of tenant IDs, idLabelName is used to add a label to the series
// to identify the tenant it belongs to, maxConcurrency is the maximum number of concurrent queries allowed,
// and ctx, sortSeries, hints, and matchers are the same as the Select method.
type MultiTenantSelectFunc func(ctx context.Context, jobs []string, wrMergeQuerier WaitRecordingMergeQuerier, idLabelName string, maxConcurrency int, sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet

func defaultMultiTenantSelectFunc(ctx context.Context, jobs []string, wrMergeQuerier WaitRecordingMergeQuerier, idLabelName string, maxConcurrency int, sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	seriesSets := make([]storage.SeriesSet, len(jobs))

	// We don't use the context passed to this function, since the context has to live longer
	// than the call to ForEachJob (i.e. as long as seriesSets)
	run := func(_ context.Context, idx int) error {
		id := jobs[idx]
		seriesSets[idx] = NewAddLabelsSeriesSet(
			wrMergeQuerier.Select(ctx, id, sortSeries, hints, matchers...),
			[]labels.Label{
				{
					Name:  idLabelName,
					Value: id,
				},
			},
		)
		return nil
	}

	if err := concurrency.ForEachJob(ctx, len(jobs), maxConcurrency, run); err != nil {
		return storage.ErrSeriesSet(err)
	}

	return storage.NewMergeSeriesSet(seriesSets, 0, storage.ChainedSeriesMerge)
}

// WaitRecordingMergeQuerier is a wrapper for MergeQuerierUpstream that records the time it took to start
// the upstream query.
type WaitRecordingMergeQuerier struct {
	start                     time.Time
	upstreamQueryWaitDuration prometheus.Histogram
	upstream                  MergeQuerierUpstream
}

func (q WaitRecordingMergeQuerier) Select(ctx context.Context, id string, sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	q.upstreamQueryWaitDuration.Observe(time.Since(q.start).Seconds())
	return q.upstream.Select(ctx, id, sortSeries, hints, matchers...)
}

type addLabelsSeriesSet struct {
	upstream   storage.SeriesSet
	labels     []labels.Label
	currSeries storage.Series
}

func NewAddLabelsSeriesSet(upstream storage.SeriesSet, labels []labels.Label) storage.SeriesSet {
	return &addLabelsSeriesSet{
		upstream: upstream,
		labels:   labels,
	}
}

func (m *addLabelsSeriesSet) Next() bool {
	m.currSeries = nil
	return m.upstream.Next()
}

// At returns full series. Returned series should be iteratable even after Next is called.
func (m *addLabelsSeriesSet) At() storage.Series {
	if m.currSeries == nil {
		upstream := m.upstream.At()
		m.currSeries = &addLabelsSeries{
			upstream: upstream,
			labels:   setLabelsRetainExisting(upstream.Labels(), m.labels...),
		}
	}
	return m.currSeries
}

// Err returns the error that iteration has failed with.
// When an error occurs, set cannot continue to iterate.
func (m *addLabelsSeriesSet) Err() error {
	return errors.Wrapf(m.upstream.Err(), "error querying %s", labelsToString(m.labels))
}

// Warnings returns a collection of warnings for the whole set.
// Warnings could be returned even if iteration has not failed with an error.
func (m *addLabelsSeriesSet) Warnings() annotations.Annotations {
	upstream := m.upstream.Warnings()
	warnings := make(annotations.Annotations, len(upstream))
	for _, w := range upstream {
		warnings.Add(errors.Wrapf(w, "warning querying %s", labelsToString(m.labels)))
	}
	return warnings
}

// rewrite label name to be more readable in error output
func rewriteLabelName(s string) string {
	return strings.TrimRight(strings.TrimLeft(s, "_"), "_")
}

// this outputs a more readable error format
func labelsToString(labels []labels.Label) string {
	parts := make([]string, len(labels))
	for pos, l := range labels {
		parts[pos] = rewriteLabelName(l.Name) + " " + l.Value
	}
	return strings.Join(parts, ", ")
}

type addLabelsSeries struct {
	upstream storage.Series
	labels   labels.Labels
}

// Labels returns the complete set of labels. For series it means all labels identifying the series.
func (a *addLabelsSeries) Labels() labels.Labels {
	return a.labels
}

// Iterator returns a new, independent iterator of the data of the series.
func (a *addLabelsSeries) Iterator(i chunkenc.Iterator) chunkenc.Iterator {
	return a.upstream.Iterator(i)
}
