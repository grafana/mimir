package querier

import (
	"context"
	"sort"
	"sync"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/weaveworks/common/user"
	"golang.org/x/sync/errgroup"

	"github.com/grafana/mimir/pkg/tenant"
	"github.com/grafana/mimir/pkg/util/spanlogger"
	"github.com/grafana/mimir/pkg/util/validation"
)

func NewMultitenantQueryable(upstream storage.Queryable, lim *validation.Overrides) storage.Queryable {
	return &multitenantQueryable{upstream: upstream, limits: lim}
}

type multitenantQueryable struct {
	upstream storage.Queryable
	limits   *validation.Overrides
}

// Querier returns a new multitenantMergeQuerier, which aggregates results from multiple
// underlying queriers into a single result.
func (m *multitenantQueryable) Querier(ctx context.Context, mint int64, maxt int64) (storage.Querier, error) {
	tenantID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, err
	}

	pol := m.limits.MultitenantPolicy(tenantID)
	if pol == nil {
		return m.upstream.Querier(ctx, mint, maxt)
	}

	tenantsMap := map[string]bool{}
	// Always add *this* tenant ID for querying.
	tenantsMap[tenantID] = true
	for _, p := range pol {
		tenantsMap[p.Tenant] = true
	}

	var tenants []string
	for t := range tenantsMap {
		tenants = append(tenants, t)
	}

	return &multitenantMergeQuerier{
		ctx:      ctx,
		upstream: m.upstream,
		mint:     mint,
		maxt:     maxt,
		tenants:  tenants,
	}, nil
}

type multitenantMergeQuerier struct {
	ctx        context.Context
	upstream   storage.Queryable
	mint, maxt int64
	tenants    []string
}

func (m *multitenantMergeQuerier) LabelValues(name string, matchers ...*labels.Matcher) ([]string, storage.Warnings, error) {
	log, ctx := spanlogger.New(m.ctx, "multitenantMergeQuerier.LabelValues")
	defer log.Span.Finish()

	lock := sync.Mutex{}
	results := make([][]string, 0, len(m.tenants))
	warnings := make([]storage.Warnings, 0, len(m.tenants))

	g, ctx := errgroup.WithContext(ctx)
	for _, t := range m.tenants {
		t := t
		g.Go(func() error {
			ctx := user.InjectOrgID(ctx, t)
			q, err := m.upstream.Querier(ctx, m.mint, m.maxt)
			if err != nil {
				return err
			}

			vals, warns, err := q.LabelValues(name, matchers...)
			if err != nil {
				return err
			}

			sort.Strings(vals)

			lock.Lock()
			results = append(results, vals)
			warnings = append(warnings, warns)
			lock.Unlock()

			return nil
		})
	}

	err := g.Wait()
	if err != nil {
		return nil, nil, err
	}

	// merge results
	return mergeAndDeduplicateSortedStringSlices(results), mergeWarnings(warnings), nil
}

func (m *multitenantMergeQuerier) LabelNames(matchers ...*labels.Matcher) ([]string, storage.Warnings, error) {
	log, ctx := spanlogger.New(m.ctx, "multitenantMergeQuerier.LabelNames")
	defer log.Span.Finish()

	lock := sync.Mutex{}
	results := make([][]string, 0, len(m.tenants))
	warnings := make([]storage.Warnings, 0, len(m.tenants))

	g, ctx := errgroup.WithContext(ctx)
	for _, t := range m.tenants {
		t := t
		g.Go(func() error {
			ctx := user.InjectOrgID(ctx, t)
			q, err := m.upstream.Querier(ctx, m.mint, m.maxt)
			if err != nil {
				return err
			}

			names, warns, err := q.LabelNames(matchers...)
			if err != nil {
				return err
			}

			// sort.Strings(vals) // should already be sorted

			lock.Lock()
			results = append(results, names)
			warnings = append(warnings, warns)
			lock.Unlock()

			return nil
		})
	}

	err := g.Wait()
	if err != nil {
		return nil, nil, err
	}

	// merge results
	return mergeAndDeduplicateSortedStringSlices(results), mergeWarnings(warnings), nil
}

func (m *multitenantMergeQuerier) Close() error {
	return nil
}

func (m *multitenantMergeQuerier) Select(sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	log, ctx := spanlogger.New(m.ctx, "multitenantMergeQuerier.Select")
	defer log.Span.Finish()

	lock := sync.Mutex{}
	seriesSets := make([]storage.SeriesSet, 0, len(m.tenants))

	g, ctx := errgroup.WithContext(ctx)
	for _, t := range m.tenants {
		t := t
		g.Go(func() error {
			ctx := user.InjectOrgID(ctx, t)
			q, err := m.upstream.Querier(ctx, m.mint, m.maxt)
			if err != nil {
				return err
			}

			// We always need sorting for easy merging.
			res := q.Select(true, hints, matchers...)
			if err := res.Err(); err != nil {
				return err
			}

			// sort.Strings(vals) // should already be sorted

			lock.Lock()
			seriesSets = append(seriesSets, res)
			lock.Unlock()

			return nil
		})
	}

	err := g.Wait()
	if err != nil {
		return storage.ErrSeriesSet(err)
	}

	return storage.NewMergeSeriesSet(seriesSets, storage.ChainedSeriesMerge)
}

func mergeAndDeduplicateSortedStringSlices(ss [][]string) []string {
	switch len(ss) {
	case 0:
		return nil
	case 1:
		return ss[0]
	case 2:
		return mergeAndDeduplicateTwoSortedStringSlices(ss[0], ss[1])
	default:
		halfway := len(ss) / 2
		return mergeAndDeduplicateTwoSortedStringSlices(
			mergeAndDeduplicateSortedStringSlices(ss[:halfway]),
			mergeAndDeduplicateSortedStringSlices(ss[halfway:]),
		)
	}
}

func mergeAndDeduplicateTwoSortedStringSlices(a, b []string) []string {
	result := make([]string, 0, len(a)+len(b))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		if a[i] < b[j] {
			result = append(result, a[i])
			i++
		} else if a[i] > b[j] {
			result = append(result, b[j])
			j++
		} else {
			result = append(result, a[i])
			i++
			j++
		}
	}
	result = append(result, a[i:]...)
	result = append(result, b[j:]...)
	return result
}

func mergeWarnings(warnings []storage.Warnings) storage.Warnings {
	output := storage.Warnings{}

	for _, ws := range warnings {
		for _, w := range ws {
			output = append(output, w)
		}
	}
	return output
}
