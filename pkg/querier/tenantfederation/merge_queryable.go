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
// By setting bypassWithSingleQuerier to true the mergeQuerier gets bypassed
// and results for request with a single querier will not contain the
// "__tenant_id__" label. This allows a smoother transition, when enabling
// tenant federation in a cluster.
// The result contains a label "__tenant_id__" to identify the tenant ID that
// it originally resulted from.
// If the label "__tenant_id__" is already existing, its value is overwritten
// by the tenant ID and the previous value is exposed through a new label
// prefixed with "original_". This behaviour is not implemented recursively.
func NewQueryable(upstream storage.Queryable, bypassWithSingleQuerier bool, maxConcurrency int, logger log.Logger) storage.Queryable {
	return newMergeQueryable(defaultTenantLabel, upstream, bypassWithSingleQuerier, maxConcurrency, logger)
}

// MergeQuerierCallback returns the underlying queriers and their IDs relevant
// for the query.
type MergeQuerierCallback func(ids []string, mint int64, maxt int64) (queriers []storage.Querier, err error)

// newMergeQueryable returns a queryable that merges results from multiple
// underlying Queryables. The underlying queryables and its label values to be
// considered are returned by a MergeQuerierCallback.
// By setting bypassWithSingleTenant to true the mergeQuerier gets bypassed
// and results for request with a single querier will not contain the id label.
// This allows a smoother transition, when enabling tenant federation in a
// cluster.
// Results contain a label `idLabelName` to identify the underlying queryable
// that it originally resulted from.
// If the label `idLabelName` is already existing, its value is overwritten and
// the previous value is exposed through a new label prefixed with "original_".
// This behaviour is not implemented recursively.
func newMergeQueryable(idLabelName string, upstream storage.Queryable, bypassWithSingleTenant bool, maxConcurrency int, logger log.Logger) storage.Queryable {
	return &mergeQueryable{
		logger:                 logger,
		idLabelName:            idLabelName,
		upstream:               upstream,
		bypassWithSingleTenant: bypassWithSingleTenant,
		maxConcurrency:         maxConcurrency,
	}
}

type mergeQueryable struct {
	logger                 log.Logger
	idLabelName            string
	bypassWithSingleTenant bool
	upstream               storage.Queryable
	maxConcurrency         int
}

// Querier returns a new mergeQuerier, which aggregates results from multiple
// underlying queriers into a single result.
func (m *mergeQueryable) Querier(mint int64, maxt int64) (storage.Querier, error) {
	q, err := m.upstream.Querier(mint, maxt)
	if err != nil {
		return nil, errors.Wrap(err, "create querier")
	}
	return &mergeQuerier{
		logger:                 m.logger,
		idLabelName:            m.idLabelName,
		maxConcurrency:         m.maxConcurrency,
		bypassWithSingleTenant: m.bypassWithSingleTenant,
		upstream:               q,
	}, nil
}

// mergeQuerier aggregates the results from underlying queriers and adds a
// label `idLabelName` to identify the queryable that the metric resulted
// from.
// If the label `idLabelName` is already existing, its value is overwritten and
// the previous value is exposed through a new label prefixed with "original_".
// This behaviour is not implemented recursively
type mergeQuerier struct {
	logger                 log.Logger
	idLabelName            string
	maxConcurrency         int
	bypassWithSingleTenant bool
	upstream               storage.Querier
}

// LabelValues returns all potential values for a label name.  It is not safe
// to use the strings beyond the lifefime of the querier.
// For the label `idLabelName` it will return all the underlying ids available.
// For the label "original_" + `idLabelName it will return all the values
// of the underlying queriers for `idLabelName`.
func (m *mergeQuerier) LabelValues(ctx context.Context, name string, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	tenantIDs, err := tenant.TenantIDs(ctx)
	if err != nil {
		return nil, nil, err
	}

	if m.bypassWithSingleTenant && len(tenantIDs) == 1 {
		return m.upstream.LabelValues(ctx, name, matchers...)
	}

	spanlog, ctx := spanlogger.NewWithLogger(ctx, m.logger, "mergeQuerier.LabelValues")
	defer spanlog.Finish()

	matchedTenants, filteredMatchers := filterValuesByMatchers(m.idLabelName, tenantIDs, matchers...)

	if name == m.idLabelName {
		var labelValues = make([]string, 0, len(matchedTenants))
		for _, id := range tenantIDs {
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

	return m.mergeDistinctStringSliceWithTenants(ctx, tenantIDs, func(ctx context.Context) ([]string, annotations.Annotations, error) {
		return m.upstream.LabelValues(ctx, name, filteredMatchers...)
	}, matchedTenants)
}

// LabelNames returns all the unique label names present in the underlying
// queriers. It also adds the `idLabelName` and if present in the original
// results the original `idLabelName`.
func (m *mergeQuerier) LabelNames(ctx context.Context, matchers ...*labels.Matcher) ([]string, annotations.Annotations, error) {
	tenantIDs, err := tenant.TenantIDs(ctx)
	if err != nil {
		return nil, nil, err
	}

	if m.bypassWithSingleTenant && len(tenantIDs) == 1 {
		return m.upstream.LabelNames(ctx, matchers...)
	}

	spanlog, ctx := spanlogger.NewWithLogger(ctx, m.logger, "mergeQuerier.LabelNames")
	defer spanlog.Finish()

	matchedTenants, filteredMatchers := filterValuesByMatchers(m.idLabelName, tenantIDs, matchers...)

	labelNames, warnings, err := m.mergeDistinctStringSliceWithTenants(ctx, tenantIDs, func(ctx context.Context) ([]string, annotations.Annotations, error) {
		return m.upstream.LabelNames(ctx, filteredMatchers...)
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

type stringSliceFunc func(context.Context) ([]string, annotations.Annotations, error)

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
func (m *mergeQuerier) mergeDistinctStringSliceWithTenants(ctx context.Context, tenantIDs []string, f stringSliceFunc, tenants map[string]struct{}) ([]string, annotations.Annotations, error) {
	jobs := make([]*stringSliceFuncJob, 0, len(tenantIDs))
	for _, id := range tenantIDs {
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
		ctx = user.InjectOrgID(ctx, job.id)
		job.result, job.warnings, err = f(ctx)
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
	return errors.Wrap(m.upstream.Close(), "failed to close upstream querier")
}

type selectJob struct {
	ctx context.Context
	id  string
}

// Select returns a set of series that matches the given label matchers. If the
// `idLabelName` is matched on, it only considers those queriers
// matching. The forwarded labelSelector is not containing those that operate
// on `idLabelName`.
func (m *mergeQuerier) Select(ctx context.Context, sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	tenantIDs, err := tenant.TenantIDs(ctx)
	if err != nil {
		return storage.ErrSeriesSet(err)
	}

	if m.bypassWithSingleTenant && len(tenantIDs) == 1 {
		return m.upstream.Select(ctx, sortSeries, hints, matchers...)
	}

	spanlog, ctx := spanlogger.NewWithLogger(ctx, m.logger, "mergeQuerier.Select")
	defer spanlog.Finish()

	matchedValues, filteredMatchers := filterValuesByMatchers(m.idLabelName, tenantIDs, matchers...)

	var jobs = make([]*selectJob, 0, len(matchedValues))
	var seriesSets = make([]storage.SeriesSet, len(matchedValues))
	for _, tenantID := range tenantIDs {
		if _, matched := matchedValues[tenantID]; !matched {
			continue
		}
		jobs = append(jobs, &selectJob{
			ctx: user.InjectOrgID(ctx, tenantID),
			id:  tenantID,
		})
	}

	run := func(ctx context.Context, idx int) error {
		job := jobs[idx]
		seriesSets[idx] = &addLabelsSeriesSet{
			upstream: m.upstream.Select(job.ctx, sortSeries, hints, filteredMatchers...),
			labels: []labels.Label{
				{
					Name:  m.idLabelName,
					Value: job.id,
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
