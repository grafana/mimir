// SPDX-License-Identifier: AGPL-3.0-only

package labelaccess

import (
	"net/http"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"

	"github.com/grafana/dskit/middleware"
)

type labelAccessMiddleware struct {
	logger log.Logger
}

// NewLabelAccessMiddleware returns an HTTP middleware that parses the X-Prom-Label-Policy
// header and injects the parsed label selectors into the request context.
// On parse error the middleware responds with HTTP 400 and does not call next.
func NewLabelAccessMiddleware(logger log.Logger) middleware.Interface {
	return &labelAccessMiddleware{logger: logger}
}

func (l *labelAccessMiddleware) Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		matchers, err := ExtractLabelMatchersHTTP(r)
		if err != nil {
			level.Debug(l.logger).Log("msg", "unable to parse X-Prom-Label-Policy header", "err", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if len(matchers) > 0 {
			level.Debug(l.logger).Log("msg", "extracted label selector policies from HTTP request", "num_tenants", len(matchers))
		}

		r = r.Clone(InjectLabelMatchersContext(r.Context(), matchers))
		next.ServeHTTP(w, r)
	})
}
