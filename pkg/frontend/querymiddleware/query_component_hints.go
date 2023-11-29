// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"
	"time"

	"github.com/grafana/dskit/tenant"

	"github.com/grafana/mimir/pkg/querier"
	"github.com/grafana/mimir/pkg/util/validation"
)

type queryComponentHints struct {
	cfg    Config
	limits Limits
	codec  Codec
	next   Handler
}

func newQueryComponentHintsMiddleware(cfg Config, limits Limits, codec Codec) Middleware {
	return MiddlewareFunc(func(next Handler) Handler {
		return &queryComponentHints{
			cfg, limits, codec, next,
		}
	})
}

func (h *queryComponentHints) Do(ctx context.Context, request Request) (Response, error) {
	now := time.Now()
	tenantIDs, err := tenant.TenantIDs(ctx)
	if err != nil {
		return h.next.Do(ctx, request)
	}

	latestQueryIngestersWithinWindow := validation.MinDurationPerTenant(tenantIDs, h.limits.QueryIngestersWithin)
	// more debuggable version of timestamps
	//requestEnd := request.GetEnd()
	//a := now.Add(-latestQueryIngestersWithinWindow)
	//fmt.Println(a)
	shouldQueryIngesters := querier.ShouldQueryIngesters(
		latestQueryIngestersWithinWindow, now, request.GetEnd(),
	)
	request = request.SetShouldQueryIngestersQueryComponentHint(shouldQueryIngesters)

	// more debuggable version of timestamps
	//requestStart := request.GetStart()
	//b := now.Add(-h.cfg.QueryStoreAfter)
	//fmt.Println(b)
	_ = querier.ShouldQueryBlockStore(h.cfg.QueryStoreAfter, now, request.GetStart())
	//request = request.SetShouldQueryBlockStoreQueryComponentHint(shouldQueryBlockstore)

	return h.next.Do(ctx, request)
}
