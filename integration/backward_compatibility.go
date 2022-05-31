// SPDX-License-Identifier: AGPL-3.0-only

package integration

import "github.com/grafana/mimir/integration/e2emimir"

// DefaultPreviousVersionImages is used by `tools/pre-pull-images` so it needs
// to be in a non `_test.go` file.
var DefaultPreviousVersionImages = map[string]e2emimir.FlagMapper{
	"grafana/mimir:2.1.0": e2emimir.NoopFlagMapper,
}

var (
	// cortexFlagMapper sets flags that are needed for cortex.
	cortexFlagMapper = e2emimir.SetFlagMapper(map[string]string{
		"-store.engine":                   "blocks",
		"-server.http-listen-port":        "8080",
		"-store-gateway.sharding-enabled": "true",
	})

	// revertRenameFrontendToQueryFrontendFlagMapper reverts the `-frontend` to `-query-frontend` flag renaming that happened in:
	// https://github.com/grafana/mimir/issues/859
	revertRenameFrontendToQueryFrontendFlagMapper = e2emimir.RenameFlagMapper(map[string]string{
		// Map new name to old name.
		"-query-frontend.scheduler-dns-lookup-period": "-frontend.scheduler-dns-lookup-period",
	})

	ingesterRingRename = e2emimir.RenameFlagMapper(map[string]string{
		"-ingester.ring.store":              "-ring.store",
		"-ingester.ring.consul.hostname":    "-consul.hostname",
		"-ingester.ring.min-ready-duration": "-ingester.min-ready-duration",
		"-ingester.ring.num-tokens":         "-ingester.num-tokens",
		"-ingester.ring.replication-factor": "-distributor.replication-factor",
	})

	ingesterRingNewFeatures = e2emimir.RemoveFlagMapper([]string{
		"-ingester.ring.readiness-check-ring-health",
	})
)
