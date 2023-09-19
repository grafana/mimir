// SPDX-License-Identifier: AGPL-3.0-only

package integration

import "github.com/grafana/mimir/integration/e2emimir"

// DefaultPreviousVersionImages is used by `tools/pre-pull-images` so it needs
// to be in a non `_test.go` file.
var DefaultPreviousVersionImages = map[string]e2emimir.FlagMapper{
	"grafana/mimir:2.0.0": e2emimir.ChainFlagMappers(
		e2emimir.SetFlagMapper(map[string]string{"-ingester.ring.readiness-check-ring-health": "false"}),
		e2emimir.RemoveFlagMapper([]string{"-ingester.native-histograms-ingestion-enabled"}),
	),
	"grafana/mimir:2.1.0": e2emimir.ChainFlagMappers(
		e2emimir.SetFlagMapper(map[string]string{"-ingester.ring.readiness-check-ring-health": "false"}),
		e2emimir.RemoveFlagMapper([]string{"-ingester.native-histograms-ingestion-enabled"}),
	),
	"grafana/mimir:2.2.0": e2emimir.ChainFlagMappers(
		e2emimir.SetFlagMapper(map[string]string{"-ingester.ring.readiness-check-ring-health": "false"}),
		e2emimir.RemoveFlagMapper([]string{"-ingester.native-histograms-ingestion-enabled"}),
	),
	"grafana/mimir:2.3.1": e2emimir.ChainFlagMappers(
		e2emimir.SetFlagMapper(map[string]string{"-ingester.ring.readiness-check-ring-health": "false"}),
		e2emimir.RemoveFlagMapper([]string{"-ingester.native-histograms-ingestion-enabled"}),
	),
	"grafana/mimir:2.4.0":  e2emimir.RemoveFlagMapper([]string{"-ingester.native-histograms-ingestion-enabled"}),
	"grafana/mimir:2.5.0":  e2emimir.RemoveFlagMapper([]string{"-ingester.native-histograms-ingestion-enabled"}),
	"grafana/mimir:2.6.0":  e2emimir.RemoveFlagMapper([]string{"-ingester.native-histograms-ingestion-enabled"}),
	"grafana/mimir:2.7.1":  e2emimir.NoopFlagMapper,
	"grafana/mimir:2.8.0":  e2emimir.NoopFlagMapper,
	"grafana/mimir:2.9.1":  e2emimir.NoopFlagMapper,
	"grafana/mimir:2.10.0": e2emimir.NoopFlagMapper,
}
