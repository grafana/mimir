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
