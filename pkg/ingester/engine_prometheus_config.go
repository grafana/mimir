// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ingester/ingester.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ingester

import promcfg "github.com/prometheus/prometheus/config"

func (e prometheusEngine) Configure(settings engineSettings) error {
	// DB.ApplyConfig only uses the storage settings.
	return e.db.ApplyConfig(&promcfg.Config{
		StorageConfig: promcfg.StorageConfig{
			ExemplarsConfig: &promcfg.ExemplarsConfig{MaxExemplars: settings.MaxExemplars},
			TSDBConfig: &promcfg.TSDBConfig{
				OutOfOrderTimeWindow: settings.OutOfOrderTimeWindow,
				// The caller resolves the default so ApplyConfig can reset the encoding when the tenant clears its limit.
				ChunkEncoding: promcfg.ChunkEncodingConfig{Floats: settings.FloatChunkEncoding},
			},
		},
	})
}
