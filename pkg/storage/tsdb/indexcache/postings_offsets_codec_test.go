// SPDX-License-Identifier: AGPL-3.0-only

package indexcache

import (
	"math/rand"
	"strings"
	"testing"

	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/stretchr/testify/require"

	streamindex "github.com/grafana/mimir/pkg/storage/indexheader/index"
)

const letters = "abcdefghijklmnopqrstuvwxyz"

func TestHeaderCacheCodec_PostingsOffsets(t *testing.T) {
	// Build 100 strings with increasing lengths
	strLenFactor := 4
	numOffsets := 100

	postingsOffsets := make([]streamindex.PostingListOffset, numOffsets)
	sb := strings.Builder{}
	strLen := 0
	for i := 0; i < numOffsets; i++ {
		// Increase string len exponentially with some jitter in growth factor.
		strLen += i * (rand.Intn(strLenFactor) + 1)
		sb.Grow(strLen)

		char := letters[i%len(letters)]
		for j := 0; j < strLen; j++ {
			sb.WriteByte(char)
		}
		lv := sb.String()
		postingsOffsets[i] = streamindex.PostingListOffset{
			LabelValue: lv,
			Off: index.Range{
				Start: int64(i * 4),
				End:   int64((i + 1) * 4),
			},
		}
	}

	for i := 0; i <= len(postingsOffsets); i++ {
		offsets := postingsOffsets[:i] // Encode more entries each iter.
		encodedOffsets := encodePostingsOffsets(offsets)

		decodedOffsets, err := decodePostingsOffsets(encodedOffsets)
		require.NoError(t, err, offsets)
		require.Equal(t, offsets, decodedOffsets)
	}
}

func TestHeaderCacheCodec_SingleRange(t *testing.T) {
	ranges := []index.Range{
		{Start: 0, End: 0},
		{Start: 0, End: 4},
		{Start: 4, End: 8},
		// Test some large values too.
		{Start: 1 << 20, End: 1 << 30},
		{Start: 1 << 32, End: 1 << 40},
	}

	for _, rng := range ranges {
		encoded := encodeSingleRange(rng)

		decoded, err := decodeSingleRange(encoded)
		require.NoError(t, err, rng)
		require.Equal(t, rng, decoded)
	}
}
