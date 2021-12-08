// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/queryrange/split_by_interval.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package queryrange

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/promql/parser"

	apierror "github.com/grafana/mimir/pkg/api/error"
)

type IntervalFn func(r Request) time.Duration

// SplitByIntervalMiddleware creates a new Middleware that splits requests by a given interval.
func SplitByIntervalMiddleware(interval IntervalFn, limits Limits, merger Merger, registerer prometheus.Registerer) Middleware {
	splitByCounter := promauto.With(registerer).NewCounter(prometheus.CounterOpts{
		Namespace: "cortex",
		Name:      "frontend_split_queries_total",
		Help:      "Total number of underlying query requests after the split by interval is applied",
	})
	return MiddlewareFunc(func(next Handler) Handler {
		return splitByInterval{
			next:           next,
			limits:         limits,
			merger:         merger,
			interval:       interval,
			splitByCounter: splitByCounter,
		}
	})
}

type splitByInterval struct {
	next     Handler
	limits   Limits
	merger   Merger
	interval IntervalFn

	// Metrics.
	splitByCounter prometheus.Counter
}

func (s splitByInterval) Do(ctx context.Context, r Request) (Response, error) {
	// First we're going to build new requests, one for each day, taking care
	// to line up the boundaries with step.
	reqs, err := splitQueryByInterval(r, s.interval(r))
	if err != nil {
		return nil, err
	}
	s.splitByCounter.Add(float64(len(reqs)))

	reqResps, err := DoRequests(ctx, s.next, reqs, s.limits, true)
	if err != nil {
		return nil, err
	}

	resps := make([]Response, 0, len(reqResps))
	for _, reqResp := range reqResps {
		resps = append(resps, reqResp.Response)
	}

	response, err := s.merger.MergeResponse(resps...)
	if err != nil {
		return nil, err
	}
	return response, nil
}

func splitQueryByInterval(r Request, interval time.Duration) ([]Request, error) {
	// Replace @ modifier function to their respective constant values in the query.
	// This way subqueries will be evaluated at the same time as the parent query.
	query, err := evaluateAtModifierFunction(r.GetQuery(), r.GetStart(), r.GetEnd())
	if err != nil {
		return nil, err
	}
	var reqs []Request
	for start := r.GetStart(); start <= r.GetEnd(); {
		end := nextIntervalBoundary(start, r.GetStep(), interval)
		if end > r.GetEnd() {
			end = r.GetEnd()
		}

		// If step isn't too big, and adding another step saves us one extra request,
		// then extend the current request to cover the extra step too.
		if end+r.GetStep() == r.GetEnd() && r.GetStep() <= 5*time.Minute.Milliseconds() {
			end = r.GetEnd()
		}

		reqs = append(reqs, r.WithQuery(query).WithStartEnd(start, end))

		start = end + r.GetStep()
	}
	return reqs, nil
}

// evaluateAtModifierFunction parse the query and evaluates the `start()` and `end()` at modifier functions into actual constant timestamps.
// For example given the start of the query is 10.00, `http_requests_total[1h] @ start()` query will be replaced with `http_requests_total[1h] @ 10.00`
// If the modifier is already a constant, it will be returned as is.
func evaluateAtModifierFunction(query string, start, end int64) (string, error) {
	expr, err := parser.ParseExpr(query)
	if err != nil {
		return "", apierror.New(apierror.TypeBadData, err.Error())
	}
	parser.Inspect(expr, func(n parser.Node, _ []parser.Node) error {
		if selector, ok := n.(*parser.VectorSelector); ok {
			switch selector.StartOrEnd {
			case parser.START:
				selector.Timestamp = &start
			case parser.END:
				selector.Timestamp = &end
			}
			selector.StartOrEnd = 0
		}
		return nil
	})
	return expr.String(), nil
}

// Round up to the step before the next interval boundary.
func nextIntervalBoundary(t, step int64, interval time.Duration) int64 {
	intervalMillis := interval.Milliseconds()
	startOfNextInterval := ((t / intervalMillis) + 1) * intervalMillis
	// ensure that target is a multiple of steps away from the start time
	target := startOfNextInterval - ((startOfNextInterval - t) % step)
	if target == startOfNextInterval {
		target -= step
	}
	return target
}
