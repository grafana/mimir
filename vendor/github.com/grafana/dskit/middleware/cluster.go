package middleware

import (
	"fmt"
	"net/http"
	"regexp"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"

	"github.com/grafana/dskit/clusterutil"
)

// ClusterValidationMiddleware validates that requests are for the correct cluster.
func ClusterValidationMiddleware(cluster string, logger log.Logger) Interface {
	return Func(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			reqCluster := r.Header.Get(clusterutil.ClusterHeader)
			if !auxilliaryPath(r.URL.Path) && reqCluster != cluster {
				level.Warn(logger).Log("msg", "rejecting request intended for wrong cluster",
					"cluster", cluster, "request_cluster", reqCluster, "header", clusterutil.ClusterHeader,
					"url", r.URL, "path", r.URL.Path)
				http.Error(w, fmt.Sprintf("request intended for cluster %q - this is cluster %q", reqCluster, cluster),
					http.StatusBadRequest)
				return
			}

			next.ServeHTTP(w, r)
		})
	})
}

// Allow for a potential path prefix being configured.
// TODO: Take /backlog_replay_complete out, and allow for it to be configured instead (it's part of a readiness probe).
var reAuxPath = regexp.MustCompile(".*/(metrics|debug/pprof.*|ready|backlog_replay_complete)")

func auxilliaryPath(pth string) bool {
	return reAuxPath.MatchString(pth)
}
