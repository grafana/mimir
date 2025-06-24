// SPDX-License-Identifier: AGPL-3.0-only

package compat

import (
	"context"
	"time"

	"github.com/grafana/dskit/tenant"
	prom_validation "github.com/prometheus/prometheus/model/validation"
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

type optsWithNamingScheme struct {
	promql.QueryOpts
	namingScheme prom_validation.NamingScheme
}

func (o optsWithNamingScheme) EnablePerStepStats() bool {
	return o.QueryOpts.EnablePerStepStats()
}

func (o optsWithNamingScheme) LookbackDelta() time.Duration {
	return o.QueryOpts.LookbackDelta()
}

func (o optsWithNamingScheme) NameValidationScheme() prom_validation.NamingScheme {
	return o.namingScheme
}

func (e nameValidatingEngine) NewInstantQuery(ctx context.Context, q storage.Queryable, opts promql.QueryOpts, qs string, ts time.Time) (promql.Query, error) {
	namingScheme, err := e.getNamingScheme(ctx)
	if err != nil {
		return nil, err
	}
	if opts == nil {
		opts = promql.NewPrometheusQueryOpts(false, 0, prom_validation.UTF8NamingScheme)
	}
	opts = &optsWithNamingScheme{
		QueryOpts:    opts,
		namingScheme: namingScheme,
	}
	return e.engine.NewInstantQuery(ctx, q, opts, qs, ts)
}

func (e nameValidatingEngine) NewRangeQuery(ctx context.Context, q storage.Queryable, opts promql.QueryOpts, qs string, start, end time.Time, interval time.Duration) (promql.Query, error) {
	namingScheme, err := e.getNamingScheme(ctx)
	if err != nil {
		return nil, err
	}
	if opts == nil {
		opts = promql.NewPrometheusQueryOpts(false, 0, prom_validation.UTF8NamingScheme)
	}
	opts = &optsWithNamingScheme{
		QueryOpts:    opts,
		namingScheme: namingScheme,
	}
	return e.engine.NewRangeQuery(ctx, q, opts, qs, start, end, interval)
}

// getNamingScheme retrieves the name validation scheme to use from a context containing tenant IDs.
// Returns legacy validation scheme if at least one tenant uses legacy validation.
func (e nameValidatingEngine) getNamingScheme(ctx context.Context) (prom_validation.NamingScheme, error) {
	tenantIDs, err := tenant.TenantIDs(ctx)
	if err != nil {
		return "", err
	}
	for _, tenantID := range tenantIDs {
		if e.limits.NameValidationScheme(tenantID) == prom_validation.LegacyNamingScheme {
			return prom_validation.LegacyNamingScheme, nil
		}
	}
	return prom_validation.UTF8NamingScheme, nil
}
