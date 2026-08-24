// SPDX-License-Identifier: AGPL-3.0-only

package labelaccess

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/middleware"

	shared "github.com/grafana/mimir/pkg/labelaccess"
	"github.com/grafana/mimir/pkg/util/propagation"
)

type labelAccessMiddleware struct {
	logger log.Logger
}

// NewLabelAccessMiddleware returns a HTTP middleware that parses the X-Prom-Label-Policy header
// and injects the parsed label selectors into the request context.
func NewLabelAccessMiddleware(logger log.Logger) middleware.Interface {
	return &labelAccessMiddleware{
		logger: logger,
	}
}

// Wrap implements the middleware interface
func (l *labelAccessMiddleware) Wrap(next http.Handler) http.Handler {
	unsupportedEndpoints := []string{"/cardinality/label_values", "/cardinality/label_names", "/cardinality/active_series"}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		matchers, err := shared.ExtractLabelMatchersHTTP(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if len(matchers) > 0 {
			level.Debug(l.logger).Log("msg", "extracted label selector policies from HTTP request", "num_matchers", len(matchers))
			for _, unsupportedEndpoint := range unsupportedEndpoints {
				if strings.HasSuffix(r.URL.Path, unsupportedEndpoint) {
					http.Error(w, fmt.Sprintf("%v endpoint doesn't support access policies with label-based access control", r.URL.Path), http.StatusBadRequest)
					return
				}
			}
		}
		r = r.Clone(shared.InjectLabelMatchersContext(r.Context(), matchers))

		next.ServeHTTP(w, r)
	})
}

type labelAccessExtractor struct{}

func NewExtractor() propagation.Extractor {
	return &labelAccessExtractor{}
}

func (l *labelAccessExtractor) ExtractFromCarrier(ctx context.Context, carrier propagation.Carrier) (context.Context, error) {
	matchers, err := shared.ExtractLabelMatchersSlice(carrier.GetAll(shared.HTTPHeaderKey))
	if err != nil {
		return nil, err
	}

	// Unlike labelAccessMiddleware above, this extractor does not need to check for unsupported endpoints as it's only used for
	// range queries and instant queries at the time of writing.

	return shared.InjectLabelMatchersContext(ctx, matchers), nil
}
