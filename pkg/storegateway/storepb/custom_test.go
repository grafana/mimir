// SPDX-License-Identifier: AGPL-3.0-only

package storepb

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
)

func TestSeries_CloneRefs(t *testing.T) {
	const (
		origLabelName  = "name"
		origLabelValue = "value"
	)
	labelNameBytes := []byte(origLabelName)
	labelValueBytes := []byte(origLabelValue)
	s := Series{
		Labels: []mimirpb.LabelAdapter{
			{
				Name:  yoloString(labelNameBytes),
				Value: yoloString(labelValueBytes),
			},
		},
		Chunks: []AggrChunk{
			{
				Raw: Chunk{
					Data: mimirpb.UnsafeByteSlice(labelValueBytes),
				},
			},
		},
	}

	s.CloneRefs()

	// Modify the referenced byte slices, to test whether s retains them (it shouldn't).
	labelNameBytes[len(labelNameBytes)-1] = 'x'
	labelValueBytes[len(labelValueBytes)-1] = 'x'

	for _, l := range s.Labels {
		require.Equal(t, origLabelName, l.Name)
		require.Equal(t, origLabelValue, l.Value)
	}
	for _, c := range s.Chunks {
		require.Equal(t, origLabelValue, string(c.Raw.Data))
	}
}

func yoloString(buf []byte) string {
	return unsafe.String(unsafe.SliceData(buf), len(buf)) // nolint:gosec
}
