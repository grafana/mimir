// SPDX-License-Identifier: AGPL-3.0-only

package compat

import (
	"context"
	"time"

	"github.com/grafana/dskit/tenant"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"

	"github.com/grafana/mimir/pkg/util/validation"
)

type nameValidatingEngine struct {
	engine promql.QueryEngine
	limits *validation.Overrides
}

// NameValidatingEngine creates a new promql.QueryEngine that wraps engine and overrides query options
// with the name validation scheme from limits.
func NameValidatingEngine(engine promql.QueryEngine, limits *validation.Overrides) promql.QueryEngine {
	return &nameValidatingEngine{engine: engine, limits: limits}
}

type optsWithValidationScheme struct {
	promql.QueryOpts
	validationScheme model.ValidationScheme
}

func (o optsWithValidationScheme) EnablePerStepStats() bool {
	return o.QueryOpts.EnablePerStepStats()
}

func (o optsWithValidationScheme) LookbackDelta() time.Duration {
	return o.QueryOpts.LookbackDelta()
}

func (o optsWithValidationScheme) ValidationScheme() model.ValidationScheme {
	return o.validationScheme
}

func (e nameValidatingEngine) NewInstantQuery(ctx context.Context, q storage.Queryable, opts promql.QueryOpts, qs string, ts time.Time) (promql.Query, error) {
	validationScheme, err := e.getValidationScheme(ctx)
	if err != nil {
		return nil, err
	}
	if opts == nil {
		opts = promql.NewPrometheusQueryOpts(false, 0, model.UTF8Validation)
	}
	opts = &optsWithValidationScheme{
		QueryOpts:        opts,
		validationScheme: validationScheme,
	}
	return e.engine.NewInstantQuery(ctx, q, opts, qs, ts)
}

func (e nameValidatingEngine) NewRangeQuery(ctx context.Context, q storage.Queryable, opts promql.QueryOpts, qs string, start, end time.Time, interval time.Duration) (promql.Query, error) {
	validationScheme, err := e.getValidationScheme(ctx)
	if err != nil {
		return nil, err
	}
	if opts == nil {
		opts = promql.NewPrometheusQueryOpts(false, 0, model.UTF8Validation)
	}
	opts = &optsWithValidationScheme{
		QueryOpts:        opts,
		validationScheme: validationScheme,
	}
	return e.engine.NewRangeQuery(ctx, q, opts, qs, start, end, interval)
}

// getValidationScheme retrieves the name validation scheme to use from a context containing tenant IDs.
// Returns legacy validation scheme if at least one tenant uses legacy validation.
func (e nameValidatingEngine) getValidationScheme(ctx context.Context) (model.ValidationScheme, error) {
	tenantIDs, err := tenant.TenantIDs(ctx)
	if err != nil {
		return model.UnsetValidation, err
	}
	for _, tenantID := range tenantIDs {
		if e.limits.ValidationScheme(tenantID) == model.LegacyValidation {
			return model.LegacyValidation, nil
		}
	}
	return model.UTF8Validation, nil
}
