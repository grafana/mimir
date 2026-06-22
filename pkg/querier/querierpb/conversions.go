// SPDX-License-Identifier: AGPL-3.0-only

package querierpb

import (
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

func EncodeSeriesMetadataSlice(s []types.SeriesMetadata) []SeriesMetadata {
	encoded := make([]SeriesMetadata, len(s))

	for i, sm := range s {
		encoded[i] = EncodeSeriesMetadata(sm)
	}

	return encoded
}

func EncodeSeriesMetadata(s types.SeriesMetadata) SeriesMetadata {
	// Note that we deliberately don't use the field names below, so that this breaks if the definition of SeriesMetadata changes.
	// If you're making a change below to add a new field, you likely also need to update DecodeSeriesMetadata below.
	return SeriesMetadata{
		mimirpb.FromLabelsToLabelAdapters(s.Labels),
		s.DropName,
	}
}

func DecodeSeriesMetadata(s SeriesMetadata) types.SeriesMetadata {
	// Note that we deliberately don't use the field names below, so that this breaks if the definition of SeriesMetadata changes.
	// If you're making a change below to add a new field, you likely also need to update EncodeSeriesMetadata above.
	return types.SeriesMetadata{ //nolint:govet
		mimirpb.FromLabelAdaptersToLabels(s.Labels),
		s.DropName,
	}
}
