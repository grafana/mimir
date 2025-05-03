// SPDX-License-Identifier: AGPL-3.0-only

package client

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
)

func TestTimeSeriesChunk_MakeReferencesSafeToRetain(t *testing.T) {
	const (
		origLabelName  = "name"
		origLabelValue = "value"
		origChunk      = "chunk"
	)
	labelNameBytes := []byte(origLabelName)
	labelValueBytes := []byte(origLabelValue)
	chunkBytes := []byte(origChunk)
	c := TimeSeriesChunk{
		Labels: []mimirpb.LabelAdapter{
			{
				Name:  yoloString(labelNameBytes),
				Value: yoloString(labelValueBytes),
			},
		},
		Chunks: []Chunk{
			{
				Data: mimirpb.UnsafeByteSlice(chunkBytes),
			},
		},
	}

	c.MakeReferencesSafeToRetain()

	// Modify the referenced byte slices, to test whether c retains them (it shouldn't).
	labelNameBytes[len(labelNameBytes)-1] = 'x'
	labelValueBytes[len(labelValueBytes)-1] = 'x'
	chunkBytes[len(chunkBytes)-1] = 'x'

	for _, l := range c.Labels {
		require.Equal(t, origLabelName, l.Name)
		require.Equal(t, origLabelValue, l.Value)
	}
	for _, cc := range c.Chunks {
		require.Equal(t, origChunk, string(cc.Data))
	}
}

func yoloString(buf []byte) string {
	return unsafe.String(unsafe.SliceData(buf), len(buf)) // nolint:gosec
}
