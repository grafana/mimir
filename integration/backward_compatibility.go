// SPDX-License-Identifier: AGPL-3.0-only

package integration

import "github.com/grafana/mimir/integration/e2emimir"

// DefaultPreviousVersionImages is used by `tools/pre-pull-images` so it needs
// to be in a non `_test.go` file.
var DefaultPreviousVersionImages = map[string]e2emimir.FlagMapper{
	"quay.io/cortexproject/cortex:v1.11.0": e2emimir.ChainFlagMappers(
		cortexFlagMapper,
	),
}

var (
	// cortexFlagMapper sets flags that are needed for cortex.
	cortexFlagMapper = e2emimir.SetFlagMapper(map[string]string{
		"-store.engine":                   "blocks",
		"-server.http-listen-port":        "8080",
		"-store-gateway.sharding-enabled": "true",
	})
)
