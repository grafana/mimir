// SPDX-License-Identifier: AGPL-3.0-only

package configdoc

import "fmt"

type Category int

const (
	// Basic is the basic field category, and the default if none is defined.
	Basic Category = iota
	// Advanced is the advanced field category.
	Advanced
	// Experimental is the experimental field category.
	Experimental
	// Deprecated is the deprecated field category.
	Deprecated
)

func (c Category) String() string {
	switch c {
	case Basic:
		return "basic"
	case Advanced:
		return "advanced"
	case Experimental:
		return "experimental"
	case Deprecated:
		return "deprecated"
	default:
		panic(fmt.Sprintf("Unknown field category: %d", c))
	}
}

// Fields are primarily categorized via struct tags, but this can be impossible when third party libraries are involved
// Only categorize fields here when you can't otherwise, since struct tags are less likely to become stale
var categoryOverrides = map[string]Category{
	// Defined in grafana/dskit/server.Config
	"server.graceful-shutdown-timeout":                  Advanced,
	"server.grpc-conn-limit":                            Advanced,
	"server.grpc-listen-network":                        Advanced,
	"server.grpc-max-concurrent-streams":                Advanced,
	"server.grpc-max-recv-msg-size-bytes":               Advanced,
	"server.grpc-max-send-msg-size-bytes":               Advanced,
	"server.grpc-tls-ca-path":                           Advanced,
	"server.grpc-tls-cert-path":                         Advanced,
	"server.grpc-tls-client-auth":                       Advanced,
	"server.grpc-tls-key-path":                          Advanced,
	"server.grpc.keepalive.max-connection-age":          Advanced,
	"server.grpc.keepalive.max-connection-age-grace":    Advanced,
	"server.grpc.keepalive.max-connection-idle":         Advanced,
	"server.grpc.keepalive.min-time-between-pings":      Advanced,
	"server.grpc.keepalive.ping-without-stream-allowed": Advanced,
	"server.grpc.keepalive.time":                        Advanced,
	"server.grpc.keepalive.timeout":                     Advanced,
	"server.grpc.num-workers":                           Advanced,
	"server.http-conn-limit":                            Advanced,
	"server.http-idle-timeout":                          Advanced,
	"server.http-listen-network":                        Advanced,
	"server.http-read-timeout":                          Advanced,
	"server.http-tls-ca-path":                           Advanced,
	"server.http-tls-cert-path":                         Advanced,
	"server.http-tls-client-auth":                       Advanced,
	"server.http-tls-key-path":                          Advanced,
	"server.http-write-timeout":                         Advanced,
	"server.log-source-ips-enabled":                     Advanced,
	"server.log-source-ips-header":                      Advanced,
	"server.log-source-ips-regex":                       Advanced,
	"server.path-prefix":                                Advanced,
	"server.register-instrumentation":                   Advanced,
	"server.log-request-at-info-level-enabled":          Advanced,
	"server.proxy-protocol-enabled":                     Experimental,

	// Defined in grafana/dskit/cache.RedisClientConfig. Deprecated in 2.14 and
	// scheduled to be removed in a future release.
	"blocks-storage.bucket-store.chunks-cache.redis.endpoint":                  Deprecated,
	"blocks-storage.bucket-store.chunks-cache.redis.username":                  Deprecated,
	"blocks-storage.bucket-store.chunks-cache.redis.password":                  Deprecated,
	"blocks-storage.bucket-store.chunks-cache.redis.db":                        Deprecated,
	"blocks-storage.bucket-store.chunks-cache.redis.master-name":               Deprecated,
	"blocks-storage.bucket-store.chunks-cache.redis.dial-timeout":              Deprecated,
	"blocks-storage.bucket-store.chunks-cache.redis.read-timeout":              Deprecated,
	"blocks-storage.bucket-store.chunks-cache.redis.write-timeout":             Deprecated,
	"blocks-storage.bucket-store.chunks-cache.redis.connection-pool-size":      Deprecated,
	"blocks-storage.bucket-store.chunks-cache.redis.connection-pool-timeout":   Deprecated,
	"blocks-storage.bucket-store.chunks-cache.redis.min-idle-connections":      Deprecated,
	"blocks-storage.bucket-store.chunks-cache.redis.idle-timeout":              Deprecated,
	"blocks-storage.bucket-store.chunks-cache.redis.max-connection-age":        Deprecated,
	"blocks-storage.bucket-store.chunks-cache.redis.max-item-size":             Deprecated,
	"blocks-storage.bucket-store.chunks-cache.redis.max-async-concurrency":     Deprecated,
	"blocks-storage.bucket-store.chunks-cache.redis.max-async-buffer-size":     Deprecated,
	"blocks-storage.bucket-store.chunks-cache.redis.max-get-multi-concurrency": Deprecated,
	"blocks-storage.bucket-store.chunks-cache.redis.max-get-multi-batch-size":  Deprecated,
	"blocks-storage.bucket-store.chunks-cache.redis.tls-enabled":               Deprecated,
	"blocks-storage.bucket-store.chunks-cache.redis.tls-cert-path":             Deprecated,
	"blocks-storage.bucket-store.chunks-cache.redis.tls-key-path":              Deprecated,
	"blocks-storage.bucket-store.chunks-cache.redis.tls-ca-path":               Deprecated,
	"blocks-storage.bucket-store.chunks-cache.redis.tls-server-name":           Deprecated,
	"blocks-storage.bucket-store.chunks-cache.redis.tls-insecure-skip-verify":  Deprecated,
	"blocks-storage.bucket-store.chunks-cache.redis.tls-cipher-suites":         Deprecated,
	"blocks-storage.bucket-store.chunks-cache.redis.tls-min-version":           Deprecated,

	"blocks-storage.bucket-store.index-cache.redis.endpoint":                  Deprecated,
	"blocks-storage.bucket-store.index-cache.redis.username":                  Deprecated,
	"blocks-storage.bucket-store.index-cache.redis.password":                  Deprecated,
	"blocks-storage.bucket-store.index-cache.redis.db":                        Deprecated,
	"blocks-storage.bucket-store.index-cache.redis.master-name":               Deprecated,
	"blocks-storage.bucket-store.index-cache.redis.dial-timeout":              Deprecated,
	"blocks-storage.bucket-store.index-cache.redis.read-timeout":              Deprecated,
	"blocks-storage.bucket-store.index-cache.redis.write-timeout":             Deprecated,
	"blocks-storage.bucket-store.index-cache.redis.connection-pool-size":      Deprecated,
	"blocks-storage.bucket-store.index-cache.redis.connection-pool-timeout":   Deprecated,
	"blocks-storage.bucket-store.index-cache.redis.min-idle-connections":      Deprecated,
	"blocks-storage.bucket-store.index-cache.redis.idle-timeout":              Deprecated,
	"blocks-storage.bucket-store.index-cache.redis.max-connection-age":        Deprecated,
	"blocks-storage.bucket-store.index-cache.redis.max-item-size":             Deprecated,
	"blocks-storage.bucket-store.index-cache.redis.max-async-concurrency":     Deprecated,
	"blocks-storage.bucket-store.index-cache.redis.max-async-buffer-size":     Deprecated,
	"blocks-storage.bucket-store.index-cache.redis.max-get-multi-concurrency": Deprecated,
	"blocks-storage.bucket-store.index-cache.redis.max-get-multi-batch-size":  Deprecated,
	"blocks-storage.bucket-store.index-cache.redis.tls-enabled":               Deprecated,
	"blocks-storage.bucket-store.index-cache.redis.tls-cert-path":             Deprecated,
	"blocks-storage.bucket-store.index-cache.redis.tls-key-path":              Deprecated,
	"blocks-storage.bucket-store.index-cache.redis.tls-ca-path":               Deprecated,
	"blocks-storage.bucket-store.index-cache.redis.tls-server-name":           Deprecated,
	"blocks-storage.bucket-store.index-cache.redis.tls-insecure-skip-verify":  Deprecated,
	"blocks-storage.bucket-store.index-cache.redis.tls-cipher-suites":         Deprecated,
	"blocks-storage.bucket-store.index-cache.redis.tls-min-version":           Deprecated,

	"blocks-storage.bucket-store.metadata-cache.redis.endpoint":                  Deprecated,
	"blocks-storage.bucket-store.metadata-cache.redis.username":                  Deprecated,
	"blocks-storage.bucket-store.metadata-cache.redis.password":                  Deprecated,
	"blocks-storage.bucket-store.metadata-cache.redis.db":                        Deprecated,
	"blocks-storage.bucket-store.metadata-cache.redis.master-name":               Deprecated,
	"blocks-storage.bucket-store.metadata-cache.redis.dial-timeout":              Deprecated,
	"blocks-storage.bucket-store.metadata-cache.redis.read-timeout":              Deprecated,
	"blocks-storage.bucket-store.metadata-cache.redis.write-timeout":             Deprecated,
	"blocks-storage.bucket-store.metadata-cache.redis.connection-pool-size":      Deprecated,
	"blocks-storage.bucket-store.metadata-cache.redis.connection-pool-timeout":   Deprecated,
	"blocks-storage.bucket-store.metadata-cache.redis.min-idle-connections":      Deprecated,
	"blocks-storage.bucket-store.metadata-cache.redis.idle-timeout":              Deprecated,
	"blocks-storage.bucket-store.metadata-cache.redis.max-connection-age":        Deprecated,
	"blocks-storage.bucket-store.metadata-cache.redis.max-item-size":             Deprecated,
	"blocks-storage.bucket-store.metadata-cache.redis.max-async-concurrency":     Deprecated,
	"blocks-storage.bucket-store.metadata-cache.redis.max-async-buffer-size":     Deprecated,
	"blocks-storage.bucket-store.metadata-cache.redis.max-get-multi-concurrency": Deprecated,
	"blocks-storage.bucket-store.metadata-cache.redis.max-get-multi-batch-size":  Deprecated,
	"blocks-storage.bucket-store.metadata-cache.redis.tls-enabled":               Deprecated,
	"blocks-storage.bucket-store.metadata-cache.redis.tls-cert-path":             Deprecated,
	"blocks-storage.bucket-store.metadata-cache.redis.tls-key-path":              Deprecated,
	"blocks-storage.bucket-store.metadata-cache.redis.tls-ca-path":               Deprecated,
	"blocks-storage.bucket-store.metadata-cache.redis.tls-server-name":           Deprecated,
	"blocks-storage.bucket-store.metadata-cache.redis.tls-insecure-skip-verify":  Deprecated,
	"blocks-storage.bucket-store.metadata-cache.redis.tls-cipher-suites":         Deprecated,
	"blocks-storage.bucket-store.metadata-cache.redis.tls-min-version":           Deprecated,

	"query-frontend.results-cache.redis.endpoint":                  Deprecated,
	"query-frontend.results-cache.redis.username":                  Deprecated,
	"query-frontend.results-cache.redis.password":                  Deprecated,
	"query-frontend.results-cache.redis.db":                        Deprecated,
	"query-frontend.results-cache.redis.master-name":               Deprecated,
	"query-frontend.results-cache.redis.dial-timeout":              Deprecated,
	"query-frontend.results-cache.redis.read-timeout":              Deprecated,
	"query-frontend.results-cache.redis.write-timeout":             Deprecated,
	"query-frontend.results-cache.redis.connection-pool-size":      Deprecated,
	"query-frontend.results-cache.redis.connection-pool-timeout":   Deprecated,
	"query-frontend.results-cache.redis.min-idle-connections":      Deprecated,
	"query-frontend.results-cache.redis.idle-timeout":              Deprecated,
	"query-frontend.results-cache.redis.max-connection-age":        Deprecated,
	"query-frontend.results-cache.redis.max-item-size":             Deprecated,
	"query-frontend.results-cache.redis.max-async-concurrency":     Deprecated,
	"query-frontend.results-cache.redis.max-async-buffer-size":     Deprecated,
	"query-frontend.results-cache.redis.max-get-multi-concurrency": Deprecated,
	"query-frontend.results-cache.redis.max-get-multi-batch-size":  Deprecated,
	"query-frontend.results-cache.redis.tls-enabled":               Deprecated,
	"query-frontend.results-cache.redis.tls-cert-path":             Deprecated,
	"query-frontend.results-cache.redis.tls-key-path":              Deprecated,
	"query-frontend.results-cache.redis.tls-ca-path":               Deprecated,
	"query-frontend.results-cache.redis.tls-server-name":           Deprecated,
	"query-frontend.results-cache.redis.tls-insecure-skip-verify":  Deprecated,
	"query-frontend.results-cache.redis.tls-cipher-suites":         Deprecated,
	"query-frontend.results-cache.redis.tls-min-version":           Deprecated,

	"ruler-storage.cache.redis.endpoint":                  Deprecated,
	"ruler-storage.cache.redis.username":                  Deprecated,
	"ruler-storage.cache.redis.password":                  Deprecated,
	"ruler-storage.cache.redis.db":                        Deprecated,
	"ruler-storage.cache.redis.master-name":               Deprecated,
	"ruler-storage.cache.redis.dial-timeout":              Deprecated,
	"ruler-storage.cache.redis.read-timeout":              Deprecated,
	"ruler-storage.cache.redis.write-timeout":             Deprecated,
	"ruler-storage.cache.redis.connection-pool-size":      Deprecated,
	"ruler-storage.cache.redis.connection-pool-timeout":   Deprecated,
	"ruler-storage.cache.redis.min-idle-connections":      Deprecated,
	"ruler-storage.cache.redis.idle-timeout":              Deprecated,
	"ruler-storage.cache.redis.max-connection-age":        Deprecated,
	"ruler-storage.cache.redis.max-item-size":             Deprecated,
	"ruler-storage.cache.redis.max-async-concurrency":     Deprecated,
	"ruler-storage.cache.redis.max-async-buffer-size":     Deprecated,
	"ruler-storage.cache.redis.max-get-multi-concurrency": Deprecated,
	"ruler-storage.cache.redis.max-get-multi-batch-size":  Deprecated,
	"ruler-storage.cache.redis.tls-enabled":               Deprecated,
	"ruler-storage.cache.redis.tls-cert-path":             Deprecated,
	"ruler-storage.cache.redis.tls-key-path":              Deprecated,
	"ruler-storage.cache.redis.tls-ca-path":               Deprecated,
	"ruler-storage.cache.redis.tls-server-name":           Deprecated,
	"ruler-storage.cache.redis.tls-insecure-skip-verify":  Deprecated,
	"ruler-storage.cache.redis.tls-cipher-suites":         Deprecated,
	"ruler-storage.cache.redis.tls-min-version":           Deprecated,

	// main.go global flags
	"config.file":       Basic,
	"config.expand-env": Basic,
}

func AddCategoryOverrides(o map[string]Category) {
	for n, c := range o {
		categoryOverrides[n] = c
	}
}

func GetCategoryOverride(fieldName string) (category Category, ok bool) {
	category, ok = categoryOverrides[fieldName]
	return
}

func VisitCategoryOverrides(f func(name string)) {
	for override := range categoryOverrides {
		f(override)
	}
}
