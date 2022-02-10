// SPDX-License-Identifier: AGPL-3.0-only

package integration

import "github.com/grafana/mimir/integration/e2emimir"

// DefaultPreviousVersionImages is used by `tools/pre-pull-images` so it needs
// to be in a non `_test.go` file.
var DefaultPreviousVersionImages = map[string]e2emimir.FlagMapper{
	"quay.io/cortexproject/cortex:v1.11.0": e2emimir.ChainFlagMappers(
		func(flags map[string]string) map[string]string {
			flags["-store.engine"] = "blocks"
			flags["-server.http-listen-port"] = "8080"
			flags["-store-gateway.sharding-enabled"] = "true"
			return flags
		},
		e2emimir.RenameFlagMapper(map[string]string{
			// Note that we're renaming flags back to their old version.
			"-query-frontend.scheduler-dns-lookup-period": "-frontend.scheduler-dns-lookup-period",
		}),
	),
}
