// SPDX-License-Identifier: AGPL-3.0-only

package labelaccess

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/middleware"
)

type labelAccessMiddleware struct {
	logger log.Logger
}

// NewMiddleware returns a HTTP middleware that parses the X-Prom-Label-Policy header
// and injects the parsed label selectors into the request context.
func NewMiddleware(logger log.Logger) middleware.Interface {
	return &labelAccessMiddleware{
		logger: logger,
	}
}

// Wrap implements the middleware interface.
func (l *labelAccessMiddleware) Wrap(next http.Handler) http.Handler {
	unsupportedEndpoints := []string{"/cardinality/label_values", "/cardinality/label_names", "/cardinality/active_series"}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		matchers, err := ExtractLabelMatchersHTTP(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if len(matchers) > 0 {
			level.Info(l.logger).Log("msg", "extracted label selector policies from HTTP request", "num_matchers", len(matchers), "path", r.URL.Path)
			for _, unsupportedEndpoint := range unsupportedEndpoints {
				if strings.HasSuffix(r.URL.Path, unsupportedEndpoint) {
					level.Info(l.logger).Log("msg", "blocking unsupported endpoint with LBAC", "path", r.URL.Path)
					http.Error(w, fmt.Sprintf("%v endpoint doesn't support access policies with label-based access control", r.URL.Path), http.StatusBadRequest)
					return
				}
			}
		}
		r = r.Clone(InjectLabelMatchersContext(r.Context(), matchers))

		next.ServeHTTP(w, r)
	})
}
