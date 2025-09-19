// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"strings"

	"github.com/grafana/mimir/pkg/util/propagation"
)

type (
	filterQueryablesCtxKeyT int
	filterQueryables        map[string]struct{}
)

const (
	FilterQueryablesHeader                         = "X-Filter-Queryables"
	filterQueryablesCtxKey filterQueryablesCtxKeyT = 0
)

func newFilterQueryables(asString string) filterQueryables {
	f := make(filterQueryables)
	for _, name := range strings.Split(asString, ",") {
		f[strings.Trim(name, " ")] = struct{}{}
	}
	return f
}

func (f filterQueryables) use(name string) bool {
	_, ok := f[name]
	return ok
}

type FilterQueryablesExtractor struct{}

func (f *FilterQueryablesExtractor) ExtractFromCarrier(ctx context.Context, carrier propagation.Carrier) (context.Context, error) {
	if filterQueryables := carrier.Get(FilterQueryablesHeader); len(filterQueryables) > 0 {
		ctx = addFilterQueryablesToContext(ctx, filterQueryables)
	}

	return ctx, nil
}

func addFilterQueryablesToContext(ctx context.Context, value string) context.Context {
	return context.WithValue(ctx, filterQueryablesCtxKey, newFilterQueryables(value))
}

func getFilterQueryablesFromContext(ctx context.Context) (filterQueryables, bool) {
	value, ok := ctx.Value(filterQueryablesCtxKey).(filterQueryables)
	if !ok {
		return nil, false
	}

	return value, true
}
