package middleware

import (
	"fmt"
	"net/http"
	"regexp"
	"strings"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/grafana/dskit/clusterutil"
)

// ClusterValidationMiddleware validates that requests have the correct cluster verification label.
func ClusterValidationMiddleware(cluster string, auxPaths []string, invalidClusters *prometheus.CounterVec, logger log.Logger) Interface {
	var reB strings.Builder
	// Allow for a potential path prefix being configured.
	reB.WriteString(".*/(metrics|debug/pprof.*|ready")
	for _, p := range auxPaths {
		reB.WriteString("|" + regexp.QuoteMeta(p))
	}
	reB.WriteString(")")
	reAuxPath := regexp.MustCompile(reB.String())

	return Func(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			reqCluster := r.Header.Get(clusterutil.ClusterVerificationLabelHeader)
			if reqCluster != cluster && !reAuxPath.MatchString(r.URL.Path) {
				level.Warn(logger).Log("msg", "rejecting request with wrong cluster verification label",
					"cluster_verification_label", cluster, "request_cluster_verification_label", reqCluster,
					"header", clusterutil.ClusterVerificationLabelHeader, "url", r.URL, "path", r.URL.Path)
				if invalidClusters != nil {
					invalidClusters.WithLabelValues("http", r.URL.Path, reqCluster).Inc()
				}
				http.Error(w, fmt.Sprintf("request has cluster verification label %q - it should be %q", reqCluster, cluster),
					http.StatusNetworkAuthenticationRequired)
				return
			}

			next.ServeHTTP(w, r)
		})
	})
}
