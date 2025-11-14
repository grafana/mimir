// SPDX-License-Identifier: AGPL-3.0-only

package integration

import "github.com/grafana/mimir/integration/e2emimir"

// DefaultPreviousVersionImages is used by `tools/pre-pull-images` so it needs
// to be in a non `_test.go` file.
var DefaultPreviousVersionImages = map[string]e2emimir.FlagMapper{
	"grafana/mimir:2.16.2": e2emimir.ChainFlagMappers(
		removePartitionRingFlags,
		removeQuerierRingFlags,
	),
	"grafana/mimir:2.17.2": e2emimir.ChainFlagMappers(
		removePartitionRingFlags,
		removeQuerierRingFlags,
	),
	"grafana/mimir:3.0.0": e2emimir.ChainFlagMappers(
		removePartitionRingFlags,
		removeQuerierRingFlags,
	),
}

// Always remove this flag so that it's not a pain to run GEM integration tests.
// This temporarily flag will be removed soon from Mimir.
var defaultPreviousVersionGlobalOverrides = e2emimir.NoopFlagMapper

var removePartitionRingFlags = e2emimir.RemoveFlagMapper([]string{
	"-ingester.partition-ring.store",
	"-ingester.partition-ring.consul.hostname",
})

var removeQuerierRingFlags = e2emimir.RemoveFlagMapper([]string{
	"-querier.ring.store",
	"-querier.ring.consul.hostname",
})
