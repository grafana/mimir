// SPDX-License-Identifier: AGPL-3.0-only

package functions

import (
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

func InfoFactory() SeriesMetadataFunction {
	return func(seriesMetadata []types.SeriesMetadata, tracker *limiter.MemoryConsumptionTracker, enableDelayedNameRemoval bool) ([]types.SeriesMetadata, error) {
		return seriesMetadata, nil
	}
}
