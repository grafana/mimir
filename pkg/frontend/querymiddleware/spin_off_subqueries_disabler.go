// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"
	"strings"

	"github.com/grafana/dskit/tenant"
	"github.com/prometheus/prometheus/model/labels"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/util/validation"
)

type spinOffDisablerContextKey int

const spinOffDisabledPatternKey spinOffDisablerContextKey = 0

// spinOffSubqueriesDisablerMiddleware records in the request context whether the original query
// matches a configured subquery_spin_off_disabled_queries pattern. It must run before any
// query-rewriting middleware so that matching uses the original query.
type spinOffSubqueriesDisablerMiddleware struct {
	next   MetricsQueryHandler
	limits Limits
}

func newSpinOffSubqueriesDisablerMiddleware(limits Limits) MetricsQueryMiddleware {
	return MetricsQueryMiddlewareFunc(func(next MetricsQueryHandler) MetricsQueryHandler {
		return &spinOffSubqueriesDisablerMiddleware{
			next:   next,
			limits: limits,
		}
	})
}

func (m *spinOffSubqueriesDisablerMiddleware) Do(ctx context.Context, req MetricsQueryRequest) (Response, error) {
	tenantIDs, err := tenant.TenantIDs(ctx)
	if err != nil {
		return nil, apierror.New(apierror.TypeBadData, err.Error())
	}

	if !validation.AllTrueBooleansPerTenant(tenantIDs, m.limits.SubquerySpinOffEnabled) {
		return m.next.Do(ctx, req)
	}

	if pattern, disabled := matchedDisabledSpinOffQuery(m.limits, tenantIDs, req.GetQuery()); disabled {
		ctx = contextWithSpinOffDisabledPattern(ctx, pattern)
	}
	return m.next.Do(ctx, req)
}

func contextWithSpinOffDisabledPattern(ctx context.Context, pattern string) context.Context {
	return context.WithValue(ctx, spinOffDisabledPatternKey, pattern)
}

// spinOffDisabledPatternFromContext returns the matched pattern, or an empty string if none matched.
func spinOffDisabledPatternFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(spinOffDisabledPatternKey).(string); ok {
		return v
	}
	return ""
}

// matchedDisabledSpinOffQuery returns the configured pattern matching the query for any of the given
// tenants. Matching mirrors the query blocker: literal comparison, plus regex when the rule sets it.
func matchedDisabledSpinOffQuery(limits Limits, tenantIDs []string, query string) (string, bool) {
	trimmedQuery := strings.TrimSpace(query)
	for _, tenantID := range tenantIDs {
		for _, disabled := range limits.SubquerySpinOffDisabledQueries(tenantID) {
			pattern := strings.TrimSpace(disabled.Pattern)
			if pattern == "" {
				continue // pattern is required and enforced during configuration load.
			}

			if pattern == trimmedQuery {
				return disabled.Pattern, true
			}

			if disabled.Regex {
				r, err := labels.NewFastRegexMatcher(disabled.Pattern)
				if err != nil {
					continue // regex patterns are validated during configuration load.
				}
				if r.MatchString(query) {
					return disabled.Pattern, true
				}
			}
		}
	}
	return "", false
}
