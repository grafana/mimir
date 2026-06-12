// SPDX-License-Identifier: AGPL-3.0-only

package querierpb

import (
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

func EncodeSeriesMetadataSlice(s []types.SeriesMetadata) []SeriesMetadata {
	encoded := make([]SeriesMetadata, len(s))

	for i, sm := range s {
		encoded[i] = SeriesMetadata{
			Labels:   mimirpb.FromLabelsToLabelAdapters(sm.Labels),
			DropName: sm.DropName,
		}
	}

	return encoded
}
