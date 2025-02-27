// SPDX-License-Identifier: AGPL-3.0-only

package index

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	streamencoding "github.com/grafana/mimir/pkg/storage/indexheader/encoding"
	"github.com/grafana/mimir/pkg/storage/indexheader/indexheaderpb"
)

func TestPostingValueOffsets(t *testing.T) {
	testCases := map[string]struct {
		existingOffsets []postingOffset
		prefix          string
		expectedFound   bool
		expectedStart   int
		expectedEnd     int
	}{
		"prefix not found": {
			existingOffsets: []postingOffset{
				{value: "010"},
				{value: "019"},
				{value: "030"},
				{value: "031"},
			},
			prefix:        "a",
			expectedFound: false,
		},
		"prefix matches only one sampled offset": {
			existingOffsets: []postingOffset{
				{value: "010"},
				{value: "019"},
				{value: "030"},
				{value: "031"},
			},
			prefix:        "02",
			expectedFound: true,
			expectedStart: 1,
			expectedEnd:   2,
		},
		"prefix matches all offsets": {
			existingOffsets: []postingOffset{
				{value: "010"},
				{value: "019"},
				{value: "030"},
				{value: "031"},
			},
			prefix:        "0",
			expectedFound: true,
			expectedStart: 0,
			expectedEnd:   4,
		},
		"prefix matches only last offset": {
			existingOffsets: []postingOffset{
				{value: "010"},
				{value: "019"},
				{value: "030"},
				{value: "031"},
			},
			prefix:        "031",
			expectedFound: true,
			expectedStart: 3,
			expectedEnd:   4,
		},
		"prefix matches multiple offsets": {
			existingOffsets: []postingOffset{
				{value: "010"},
				{value: "019"},
				{value: "020"},
				{value: "030"},
				{value: "031"},
			},
			prefix:        "02",
			expectedFound: true,
			expectedStart: 1,
			expectedEnd:   3,
		},
		"prefix matches only first offset": {
			existingOffsets: []postingOffset{
				{value: "010"},
				{value: "019"},
				{value: "020"},
				{value: "030"},
				{value: "031"},
			},
			prefix:        "015",
			expectedFound: true,
			expectedStart: 0,
			expectedEnd:   1,
		},
	}

	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			offsets := postingValueOffsets{offsets: testCase.existingOffsets}
			start, end, found := offsets.prefixOffsets(testCase.prefix)
			assert.Equal(t, testCase.expectedStart, start)
			assert.Equal(t, testCase.expectedEnd, end)
			assert.Equal(t, testCase.expectedFound, found)
		})
	}
}

func createPostingOffset(n int) []*indexheaderpb.PostingOffset {
	offsets := make([]*indexheaderpb.PostingOffset, n)
	for i := 0; i < n; i++ {
		offsets[i] = &indexheaderpb.PostingOffset{Value: fmt.Sprintf("%d", i), TableOff: int64(i)}
	}
	return offsets
}

func Test_NewPostingOffsetTableFromSparseHeader(t *testing.T) {

	testCases := map[string]struct {
		existingOffsetsLen              int
		postingOffsetsInMemSamplingRate int
		protoSamplingRate               int64
		expectedLen                     int
		expectErr                       bool
	}{
		"downsample_noop_proto_has_equal_sampling_rate":           {100, 32, 32, 100, false},
		"downsample_short_offsets":                                {2, 32, 16, 1, false},
		"downsample_noop_short_offsets":                           {1, 32, 16, 1, false},
		"downsample_proto_has_divisible_sampling_rate":            {100, 32, 16, 50, false},
		"cannot_downsample_proto_has_no_sampling_rate":            {100, 32, 0, 0, true},
		"cannot_upsample_proto_has_less_frequent_sampling_rate":   {100, 32, 64, 0, true},
		"cannot_downsample_proto_has_non_divisible_sampling_rate": {100, 32, 10, 0, true},
		"downsample_sampling_rates_ratio_does_not_divide_offsets": {33, 32, 16, 17, false},
		"downsample_sampling_rates_ratio_exceeds_offset_len":      {10, 1024, 8, 1, false},
		"downsample_sampling_rates_ratio_equals_offset_len":       {100, 100, 1, 1, false},
	}

	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			factory := streamencoding.DecbufFactory{}

			postingsMap := make(map[string]*indexheaderpb.PostingValueOffsets)
			postingsMap["__name__"] = &indexheaderpb.PostingValueOffsets{Offsets: createPostingOffset(testCase.existingOffsetsLen)}

			protoTbl := indexheaderpb.PostingOffsetTable{
				Postings:                      postingsMap,
				PostingOffsetInMemorySampling: testCase.protoSamplingRate,
			}

			tbl, err := NewPostingOffsetTableFromSparseHeader(&factory, &protoTbl, 0, testCase.postingOffsetsInMemSamplingRate)
			if testCase.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, testCase.expectedLen, len(tbl.postings["__name__"].offsets))
			}

		})
	}

}
