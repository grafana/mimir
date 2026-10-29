// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"errors"

	"github.com/grafana/dskit/tenant"
	"github.com/prometheus/prometheus/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/storage/cost"
)

// SampleQueryCost measures a bounded set of native chunks without streaming samples.
func (i *Ingester) SampleQueryCost(ctx context.Context, req *cost.SamplingRequest) (res *cost.SamplingResponse, err error) {
	matchers, err := req.Validate()
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	defer func() { err = i.mapReadErrorToErrorWithStatus(err) }()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, err
	}
	if err := i.enforceReadConsistency(ctx, userID); err != nil {
		return nil, err
	}
	i.metrics.queries.Inc()
	res = &cost.SamplingResponse{}
	db := i.getTSDB(userID)
	if db == nil {
		return res, nil
	}
	// The native capability rejects merged and out-of-order readers that can
	// eagerly encode an entire window before an iterator budget can stop them.
	source, supported := db.db.CostSamplingQueryable()
	if !supported {
		res.Unsupported = true
		return res, nil
	}
	q, err := source.ChunkQuerier(req.Start, req.End)
	if errors.Is(err, storage.ErrCostSamplingUnsupported) {
		res.Unsupported = true
		return res, nil
	}
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, q.Close()) }()
	// Keep native chunk boundaries so clipping cannot re-encode a chunk before
	// its encoded size has been checked. Observe seeks within the requested window.
	set := q.Select(ctx, false, &storage.SelectHints{Start: req.Start, End: req.End, DisableTrimming: true}, matchers...)
	for res.SeriesExamined < cost.MaxSeries && res.HasBudget() && set.Next() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		res.SeriesExamined++
		it := set.At().Iterator(nil)
		for res.HasBudget() && it.Next() {
			ok, err := res.Observe(ctx, it.At(), req)
			if err != nil {
				return nil, err
			}
			if !ok {
				return res, nil
			}
		}
		if err := it.Err(); err != nil {
			return nil, err
		}
	}
	for _, warning := range set.Warnings() {
		res.Warnings = append(res.Warnings, warning.Error())
	}
	return res, errors.Join(ctx.Err(), set.Err())
}
