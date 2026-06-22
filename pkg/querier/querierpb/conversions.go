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

// EncodeInstantVectorSeriesData encodes d into a InstantVectorSeriesData.
// The returned instance may reference data in d (ie. this is a shallow copy, not a deep copy).
func EncodeInstantVectorSeriesData(d types.InstantVectorSeriesData) InstantVectorSeriesData {
	// Note that we deliberately don't use the field names below, so that this breaks if the definition of InstantVectorSeriesData changes.
	// If you're making a change below to add a new field, you likely also need to update DecodeInstantVectorSeriesData below.
	return InstantVectorSeriesData{
		// The methods below do unsafe casts and do not copy the data from the slices.
		mimirpb.FromFPointsToSamples(d.Floats),
		mimirpb.FromHPointsToHistograms(d.Histograms),
	}
}

// DecodeInstantVectorSeriesData decodes d into a types.InstantVectorSeriesData.
// The returned instance may reference data in d (ie. this is a shallow copy, not a deep copy).
func DecodeInstantVectorSeriesData(d InstantVectorSeriesData) types.InstantVectorSeriesData {
	// Note that we deliberately don't use the field names below, so that this breaks if the definition of InstantVectorSeriesData changes.
	// If you're making a change below to add a new field, you likely also need to update EncodeInstantVectorSeriesData above.
	return types.InstantVectorSeriesData{ //nolint:govet
		// The methods below do unsafe casts and do not copy the data from the slices.
		mimirpb.FromSamplesToFPoints(d.Floats),
		mimirpb.FromHistogramsToHPoints(d.Histograms),
	}
}
