// SPDX-License-Identifier: AGPL-3.0-only

package fieldcategory

import "fmt"

type Category int

const (
	// Basic is the basic field category, and the default if none is defined.
	Basic Category = iota
	// Advanced is the advanced field category.
	Advanced
	// Experimental is the experimental field category.
	Experimental
)

func (c Category) String() string {
	switch c {
	case Basic:
		return "basic"
	case Advanced:
		return "advanced"
	case Experimental:
		return "experimental"
	default:
		panic(fmt.Sprintf("Unknown field category: %d", c))
	}
}

// Fields are primarily categorized via struct tags, but this can be impossible when third party libraries are involved
// Only categorize fields here when you can't otherwise, since struct tags are less likely to become stale
var overrides = map[string]Category{
	// cmd/mimir/main variables that aren't in structs
	"debug.block-profile-rate":     Advanced,
	"debug.mutex-profile-fraction": Advanced,
	"mem-ballast-size-bytes":       Advanced,
	// weaveworks/common/server in server.Config
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
}

func GetOverride(fieldName string) (category Category, ok bool) {
	category, ok = overrides[fieldName]
	return
}
