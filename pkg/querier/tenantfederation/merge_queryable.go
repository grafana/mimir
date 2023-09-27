// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/tenantfederation/merge_queryable.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package tenantfederation

import (
	"context"
	"sort"
	"strings"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/concurrency"
	"github.com/grafana/dskit/tenant"
	"github.com/grafana/dskit/user"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/util/annotations"
	"golang.org/x/exp/slices"

	"github.com/grafana/mimir/pkg/util/spanlogger"
)

// NewQueryable returns a queryable that iterates through all the tenant IDs
// that are part of the request and aggregates the results from each tenant's
// Querier by sending of subsequent requests.
// By setting bypassWithSingleID to true the mergeQuerier gets bypassed
// and results for request with a single ID will not contain the
// "__tenant_id__" label. This allows a smoother transition, when enabling
// tenant federation in a cluster.
// The result contains a label "__tenant_id__" to identify the tenant ID that
// it originally resulted from.
// If the label "__tenant_id__" is already existing, its value is overwritten
// by the tenant ID and the previous value is exposed through a new label
// prefixed with "original_". This behaviour is not implemented recursively.
func NewQueryable(upstream storage.Queryable, bypassWithSingleID bool, maxConcurrency int, logger log.Logger) storage.Queryable {
	callbacks := MergeQuerierCallbacks{
		Querier: func(mint, maxt int64) (MergeQuerierUpstream, error) {
			q, err := upstream.Querier(mint, maxt)
			if err != nil {
				return nil, errors.Wrap(err, "construct querier")
			}

			return &tenantQuerier{
				upstream: q,
			}, nil
		},
		IDs: func(ctx context.Context) ([]string, error) {
			tenantIDs, err := tenant.TenantIDs(ctx)
			return tenantIDs, err
		},
	}
	return NewMergeQueryable(defaultTenantLabel, callbacks, bypassWithSingleID, maxConcurrency, logger)
}

type MergeQuerierCallbacks struct {
	Querier func(mint, maxt int64) (MergeQuerierUpstream, error)
	IDs     func(ctx context.Context) (ids []string, err error)
}

// MergeQuerierUpstream mirrors storage.Querier, except every query method also takes a federation ID.
type MergeQuerierUpstream interface {
	Select(ctx context.Context, id string, sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet
	LabelValues(ctx context.Context, id string, name string, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error)
	LabelNames(ctx context.Context, id string, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error)
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

func (q *tenantQuerier) LabelValues(ctx context.Context, id string, name string, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return q.upstream.LabelValues(user.InjectOrgID(ctx, id), name, matchers...)
}

func (q *tenantQuerier) LabelNames(ctx context.Context, id string, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	return q.upstream.LabelNames(user.InjectOrgID(ctx, id), matchers...)
}

func (q *tenantQuerier) Close() error {
	return q.upstream.Close()
}

// NewMergeQueryable returns a queryable that merges results from multiple
// underlying Queryables. The underlying queryables and its label values to be
// considered are returned by a MergeQuerierCallback.
// By setting bypassWithSingleID to true the mergeQuerier gets bypassed
// and results for requests with a single ID will not contain the ID label.
// This allows a smoother transition, when enabling tenant federation in a
// cluster.
// Results contain a label `idLabelName` to identify the underlying queryable
// that it originally resulted from.
// If the label `idLabelName` is already existing, its value is overwritten and
// the previous value is exposed through a new label prefixed with "original_".
// This behaviour is not implemented recursively.
func NewMergeQueryable(idLabelName string, callbacks MergeQuerierCallbacks, bypassWithSingleID bool, maxConcurrency int, logger log.Logger) storage.Queryable {
	return &mergeQueryable{
		logger:             logger,
		idLabelName:        idLabelName,
		callbacks:          callbacks,
		bypassWithSingleID: bypassWithSingleID,
		maxConcurrency:     maxConcurrency,
	}
}

type mergeQueryable struct {
	logger             log.Logger
	idLabelName        string
	bypassWithSingleID bool
	callbacks          MergeQuerierCallbacks
	maxConcurrency     int
}

// Querier returns a new mergeQuerier, which aggregates results from multiple
// underlying queriers into a single result.
func (m *mergeQueryable) Querier(mint int64, maxt int64) (storage.Querier, error) {
	// TODO: it's necessary to think of how to override context inside querier
	//  to mark spans created inside querier as child of a span created inside
	//  methods of merged querier.
	upstream, err := m.callbacks.Querier(mint, maxt)
	if err != nil {
		return nil, err
	}
	return &mergeQuerier{
		logger:             m.logger,
		idLabelName:        m.idLabelName,
		callbacks:          m.callbacks,
		upstream:           upstream,
		maxConcurrency:     m.maxConcurrency,
		bypassWithSingleID: m.bypassWithSingleID,
	}, nil
}

// mergeQuerier aggregates the results from underlying queriers and adds a
// label `idLabelName` to identify the queryable that the metric resulted
// from.
// If the label `idLabelName` is already existing, its value is overwritten and
// the previous value is exposed through a new label prefixed with "original_".
// This behaviour is not implemented recursively
type mergeQuerier struct {
	logger             log.Logger
	callbacks          MergeQuerierCallbacks
	upstream           MergeQuerierUpstream
	idLabelName        string
	maxConcurrency     int
	bypassWithSingleID bool
}

// LabelValues returns all potential values for a label name.  It is not safe
// to use the strings beyond the lifefime of the querier.
// For the label `idLabelName` it will return all the underlying ids available.
// For the label "original_" + `idLabelName it will return all the values
// of the underlying queriers for `idLabelName`.
func (m *mergeQuerier) LabelValues(ctx context.Context, name string, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	ids, err := m.callbacks.IDs(ctx)
	if err != nil {
		return nil, nil, err
	}

	if m.bypassWithSingleID && len(ids) == 1 {
		return m.upstream.LabelValues(ctx, ids[0], name, matchers...)
	}

	spanlog, ctx := spanlogger.NewWithLogger(ctx, m.logger, "mergeQuerier.LabelValues")
	defer spanlog.Finish()

	matchedTenants, filteredMatchers := filterValuesByMatchers(m.idLabelName, ids, matchers...)

	if name == m.idLabelName {
		var labelValues = make([]string, 0, len(matchedTenants))
		for _, id := range ids {
			if _, matched := matchedTenants[id]; matched {
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

	return m.mergeDistinctStringSliceWithTenants(ctx, ids, func(ctx context.Context, id string) ([]string, annotations.Annotations, error) {
		return m.upstream.LabelValues(ctx, id, name, filteredMatchers...)
	}, matchedTenants)
}

// LabelNames returns all the unique label names present in the underlying
// queriers. It also adds the `idLabelName` and if present in the original
// results the original `idLabelName`.
func (m *mergeQuerier) LabelNames(ctx context.Context, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	ids, err := m.callbacks.IDs(ctx)
	if err != nil {
		return nil, nil, err
	}

	if m.bypassWithSingleID && len(ids) == 1 {
		return m.upstream.LabelNames(ctx, ids[0], matchers...)
	}

	spanlog, ctx := spanlogger.NewWithLogger(ctx, m.logger, "mergeQuerier.LabelNames")
	defer spanlog.Finish()

	matchedTenants, filteredMatchers := filterValuesByMatchers(m.idLabelName, ids, matchers...)

	labelNames, warnings, err := m.mergeDistinctStringSliceWithTenants(ctx, ids, func(ctx context.Context, id string) ([]string, annotations.Annotations, error) {
		return m.upstream.LabelNames(ctx, id, filteredMatchers...)
	}, matchedTenants)
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
// results from queriers whose tenant ids match the tenants map. If a nil map is
// provided, all queriers are used. It removes duplicates and sorts the result.
// It doesn't require the output of the stringSliceFunc to be sorted, as results
// of LabelValues are not sorted.
func (m *mergeQuerier) mergeDistinctStringSliceWithTenants(ctx context.Context, ids []string, f stringSliceFunc, tenants map[string]struct{}) ([]string, annotations.Annotations, error) {
	jobs := make([]*stringSliceFuncJob, 0, len(ids))
	for _, id := range ids {
		if tenants != nil {
			if _, matched := tenants[id]; !matched {
				continue
			}
		}

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

	var result = make([]string, 0, len(resultMap))
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

// Select returns a set of series that matches the given label matchers. If the
// `idLabelName` is matched on, it only considers those queriers
// matching. The forwarded labelSelector is not containing those that operate
// on `idLabelName`.
func (m *mergeQuerier) Select(ctx context.Context, sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	ids, err := m.callbacks.IDs(ctx)
	if err != nil {
		return storage.ErrSeriesSet(err)
	}

	if m.bypassWithSingleID && len(ids) == 1 {
		return m.upstream.Select(ctx, ids[0], sortSeries, hints, matchers...)
	}

	spanlog, ctx := spanlogger.NewWithLogger(ctx, m.logger, "mergeQuerier.Select")
	defer spanlog.Finish()

	matchedValues, filteredMatchers := filterValuesByMatchers(m.idLabelName, ids, matchers...)

	var jobs = make([]string, 0, len(matchedValues))
	var seriesSets = make([]storage.SeriesSet, len(matchedValues))
	for _, id := range ids {
		if _, matched := matchedValues[id]; !matched {
			continue
		}
		jobs = append(jobs, id)
	}

	// We don't use the context passed to this function, since the context has to live longer
	// than the call to ForEachJob (i.e. as long as seriesSets)
	run := func(_ context.Context, idx int) error {
		id := jobs[idx]
		seriesSets[idx] = &addLabelsSeriesSet{
			upstream: m.upstream.Select(ctx, id, sortSeries, hints, filteredMatchers...),
			labels: []labels.Label{
				{
					Name:  m.idLabelName,
					Value: id,
				},
			},
		}
		return nil
	}

	if err := concurrency.ForEachJob(ctx, len(jobs), m.maxConcurrency, run); err != nil {
		return storage.ErrSeriesSet(err)
	}

	return storage.NewMergeSeriesSet(seriesSets, storage.ChainedSeriesMerge)
}

type addLabelsSeriesSet struct {
	upstream   storage.SeriesSet
	labels     []labels.Label
	currSeries storage.Series
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

// The error that iteration as failed with.
// When an error occurs, set cannot continue to iterate.
func (m *addLabelsSeriesSet) Err() error {
	return errors.Wrapf(m.upstream.Err(), "error querying %s", labelsToString(m.labels))
}

// A collection of warnings for the whole set.
// Warnings could be return even iteration has not failed with error.
func (m *addLabelsSeriesSet) Warnings() annotations.Annotations {
	upstream := m.upstream.Warnings()
	var warnings annotations.Annotations
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
