// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"net/http"
	"strings"

	"github.com/grafana/dskit/middleware"
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

func FilterQueryablesMiddleware() middleware.Interface {
	return middleware.Func(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			if filterQueryables := req.Header.Get(FilterQueryablesHeader); len(filterQueryables) > 0 {
				req = req.WithContext(addFilterQueryablesToContext(req.Context(), filterQueryables))
			}

			next.ServeHTTP(w, req)
		})
	})
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
