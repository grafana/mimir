package api

import (
	"fmt"
	"net/http"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/middleware"
)

// ClusterValidationMiddleware validates that requests are for the correct cluster.
func ClusterValidationMiddleware(cluster string, logger log.Logger) middleware.Interface {
	return middleware.Func(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			reqCluster := r.Header.Get(clusterHeader)
			if reqCluster != cluster {
				level.Warn(logger).Log("msg", "rejecting request intended for wrong cluster", "cluster", cluster, "request_cluster", reqCluster)
				http.Error(w, fmt.Sprintf("request intended for cluster %q - this is cluster %q", reqCluster, cluster),
					http.StatusBadRequest)
				return
			}

			next.ServeHTTP(w, r)
		})
	})
}

const (
	clusterHeader = "X-Cluster"
)
