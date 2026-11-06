// SPDX-License-Identifier: AGPL-3.0-only

package querierpb

import (
	"testing"
	"unsafe"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
)

func TestDecodeSeriesMetadataOwnsLabels(t *testing.T) {
	name := []byte("label")
	value := []byte("value")
	encoded := SeriesMetadata{
		Labels: []mimirpb.LabelAdapter{{
			Name:  mimirpb.BorrowString(unsafe.String(unsafe.SliceData(name), len(name))),
			Value: mimirpb.BorrowString(unsafe.String(unsafe.SliceData(value), len(value))),
		}},
		DropName: true,
	}

	decoded := DecodeSeriesMetadata(encoded)
	for i := range name {
		name[i] = 'x'
	}
	for i := range value {
		value[i] = 'y'
	}
	encoded.Labels[0] = mimirpb.LabelAdapter{}

	require.Equal(t, labels.FromStrings("label", "value"), decoded.Labels)
	require.True(t, decoded.DropName)
}
