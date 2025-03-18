// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"net/http"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/tenant"
	"github.com/grafana/regexp"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/util/globalerror"
)

func newRequestBlockedError() error {
	return apierror.New(apierror.TypeBadData, globalerror.RequestBlocked.Message("the request has been blocked by the cluster administrator"))
}

type requestBlocker struct {
	limits                 Limits
	logger                 log.Logger
	blockedRequestsCounter *prometheus.CounterVec
}

func newRequestBlocker(
	limits Limits,
	logger log.Logger,
	registerer prometheus.Registerer,
) *requestBlocker {
	blockedRequestsCounter := promauto.With(registerer).NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_query_frontend_rejected_requests_total",
		Help: "Number of HTTP requests that were rejected by the cluster administrator.",
	}, []string{"user"})
	return &requestBlocker{
		limits:                 limits,
		logger:                 logger,
		blockedRequestsCounter: blockedRequestsCounter,
	}
}

func (rb *requestBlocker) isBlocked(r *http.Request) error {
	tenants, err := tenant.TenantIDs(r.Context())
	if err != nil {
		return nil
	}

	for _, tenant := range tenants {
		blockedRequests := rb.limits.BlockedRequests(tenant)

		for _, blockedRequest := range blockedRequests {
			if blockedPath := blockedRequest.Path; blockedPath != "" && blockedPath != r.URL.Path {
				continue
			}

			if blockedMethod := blockedRequest.Method; blockedMethod != "" && blockedMethod != r.Method {
				continue
			}

			if blockedParams := blockedRequest.QueryParams; len(blockedParams) > 0 {
				query := r.URL.Query()
				blockedByParams := false
				for key, blocked := range blockedParams {
					if blocked.IsRegexp {
						blockedRegexp, err := regexp.Compile(blocked.Value)
						if err != nil {
							level.Error(rb.logger).Log("msg", "failed to compile regexp. Not blocking", "regexp", blocked.Value, "err", err)
							continue
						}

						if blockedRegexp.MatchString(query.Get(key)) {
							blockedByParams = true
							break
						}
					} else if query.Get(key) == blocked.Value {
						blockedByParams = true
						break
					}
				}

				if !blockedByParams {
					continue
				}
			}

			level.Info(rb.logger).Log("msg", "request blocked", "user", tenant, "url", r.URL.String(), "method", r.Method)
			rb.blockedRequestsCounter.WithLabelValues(tenant).Inc()
			return newRequestBlockedError()
		}
	}
	return nil

}
