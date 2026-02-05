// SPDX-License-Identifier: AGPL-3.0-only

package labelaccess

import (
	"context"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/user"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/spanlogger"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
)

// WrapExemplarQueryable wraps the provided queryable with a labelAccessExemplarQueryable. This
// queryable implementation allows access to exemplars to be restricted based on label matching
// policies set as part of an admin API access policy.
func WrapExemplarQueryable(next storage.ExemplarQueryable, logger log.Logger) storage.ExemplarQueryable {
	return &labelAccessExemplarQueryable{next, logger}
}

type labelAccessExemplarQueryable struct {
	next   storage.ExemplarQueryable
	logger log.Logger
}

// ExemplarQuerier returns a new storage.ExemplarQuerier on the storage.
func (l *labelAccessExemplarQueryable) ExemplarQuerier(ctx context.Context) (storage.ExemplarQuerier, error) {
	instancePolicyMap, err := ExtractLabelMatchersContext(ctx)
	if err != nil {
		// If there are no policies in the context, that's fine - just return the next querier without filtering.
		if err == errNoMatcherSource {
			return l.next.ExemplarQuerier(ctx)
		}
		level.Error(l.logger).Log("msg", "unable to find instance policy map", "err", err)
		return nil, err
	}

	instanceName, err := user.ExtractOrgID(ctx)
	if err != nil {
		level.Error(l.logger).Log("msg", "unable to find instance name", "err", err)
		return nil, err
	}

	nextQuerier, err := l.next.ExemplarQuerier(ctx)
	if err != nil {
		return nil, err
	}

	policies, exist := instancePolicyMap[instanceName]
	if !exist {
		return nextQuerier, nil
	}
	level.Debug(l.logger).Log("msg", "enforcing label policy on query", "instance_name", instanceName)

	selectors := labelPoliciesToPromSelectors(policies)

	return newLabelAccessExemplarQuerier(ctx, selectors, nextQuerier, l.logger), nil
}

type labelAccessExemplarQuerier struct {
	selectors   []promSelector
	stringers   util.MultiMatchersStringer
	nextQuerier storage.ExemplarQuerier
	logger      log.Logger
	ctx         context.Context
}

func newLabelAccessExemplarQuerier(ctx context.Context, selectors []promSelector, upstream storage.ExemplarQuerier, logger log.Logger) *labelAccessExemplarQuerier {
	var stringers util.MultiMatchersStringer
	for _, s := range selectors {
		stringers = append(stringers, s)
	}

	return &labelAccessExemplarQuerier{
		selectors:   selectors,
		stringers:   stringers,
		nextQuerier: upstream,
		logger:      logger,
		ctx:         ctx,
	}
}

func (l *labelAccessExemplarQuerier) Select(start, end int64, matchers ...[]*labels.Matcher) ([]exemplar.QueryResult, error) {
	spanlog, _ := spanlogger.New(l.ctx, l.logger, tracer, "labelAccessExemplarQuerier.Select")
	defer spanlog.Finish()

	// There are no LBAC selectors that we have to enforce, just delegate to upstream. This
	// shouldn't actually happen since the Queryable responsible for creating this querier
	// would have just returned the upstream querier to the caller instead, but it doesn't
	// hurt to spend a little extra effort to prevent misuse.
	if len(l.selectors) == 0 {
		level.Debug(spanlog).Log("msg", "no LBAC selectors, delegating to upstream")
		return l.nextQuerier.Select(start, end, matchers...)
	}

	// The Select method gets multiple slices of matchers. Each slice is unioned together while all
	// matchers within a slice are treated as an intersection. If we only have a single LBAC selector
	// we can add our matchers to each slice of matchers passed to the upstream querier to enforce
	// that anything returned also satisfies our matchers (intersection). This allows us to avoid
	// querying all exemplars and then filtering them in memory here.
	if len(l.selectors) == 1 {
		for i, m := range matchers {
			m = append(m, l.selectors[0]...)
			matchers[i] = m
		}

		level.Debug(spanlog).Log("msg", "exemplars filtered by single LBAC selector as upstream matchers", "selector", l.stringers)
		return l.nextQuerier.Select(start, end, matchers...)
	}

	exemplars, err := l.nextQuerier.Select(start, end, matchers...)
	if err != nil {
		return nil, err
	}

	var (
		numUnfiltered int
		numFiltered   int
		filtered      []exemplar.QueryResult
	)

	for _, e := range exemplars {
		numUnfiltered += len(e.Exemplars)

		for _, s := range l.selectors {
			// Each LBAC policy is OR'd together so stop evaluating them as soon as we find
			// one that this exemplar matches.
			if s.matches(e.SeriesLabels) {
				numFiltered += len(e.Exemplars)
				filtered = append(filtered, e)
				break
			}
		}
	}

	level.Debug(spanlog).Log("msg", "exemplars filtered by LBAC selectors", "unfiltered", numUnfiltered, "filtered", numFiltered, "selectors", l.stringers)
	return filtered, nil
}
