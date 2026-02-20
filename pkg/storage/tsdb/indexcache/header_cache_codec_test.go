// SPDX-License-Identifier: AGPL-3.0-only

package indexcache

import (
	"strings"
	"testing"

	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/stretchr/testify/require"

	streamindex "github.com/grafana/mimir/pkg/storage/indexheader/index"
)

const letters = "abcdefghijklmnopqrstuvwxyz"

func TestHeaderCacheCodec_PostingsOffsets(t *testing.T) {
	// Build 101 strings with lengths 0, 8, 16, ... 800.
	strLenFactor := 8
	numOffsets := 101

	postingsOffsets := make([]streamindex.PostingListOffset, numOffsets)
	sb := strings.Builder{}
	for i := 0; i < numOffsets; i++ {
		strLen := i * strLenFactor
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

	codec := BigEndianPostingsOffsetCodec{}
	for i := 0; i <= len(postingsOffsets); i++ {
		offsets := postingsOffsets[:i] // Number of entries to encode grows each iteration
		encodedOffsets := codec.EncodePostingsOffsets(offsets)

		decodedOffsets, err := codec.DecodePostingsOffsets(encodedOffsets)
		require.NoError(t, err, offsets)
		require.Equal(t, offsets, decodedOffsets)
	}
}
