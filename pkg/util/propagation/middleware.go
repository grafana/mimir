// SPDX-License-Identifier: AGPL-3.0-only

package propagation

import (
	"net/http"

	"github.com/grafana/dskit/middleware"
)

func Middleware(extractor Extractor) middleware.Interface {
	return middleware.Func(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx, err := extractor.ExtractFromCarrier(r.Context(), HttpHeaderCarrier(r.Header))
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			r = r.WithContext(ctx)
			next.ServeHTTP(w, r)
		})
	})
}
