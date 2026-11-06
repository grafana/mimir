// SPDX-License-Identifier: AGPL-3.0-only

package index

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/indexheader/indexheaderpb"
)

func TestSparsePostingsOffsetsTableFromProto(t *testing.T) {
	testCases := map[string]struct {
		existingOffsetsLen              int
		postingOffsetsInMemSamplingRate int
		protoSamplingRate               int64
		expectedLen                     int
		expectErr                       bool
	}{
		"downsample_noop_proto_has_equal_sampling_rate": {
			existingOffsetsLen:              100,
			postingOffsetsInMemSamplingRate: 32,
			protoSamplingRate:               32,
			expectedLen:                     100,
		},
		"downsample_noop_preserve": {
			existingOffsetsLen:              1,
			postingOffsetsInMemSamplingRate: 32,
			protoSamplingRate:               16,
			expectedLen:                     1,
		},
		"downsample_noop_retain_first_and_last_posting": {
			existingOffsetsLen:              2,
			postingOffsetsInMemSamplingRate: 32,
			protoSamplingRate:               16,
			expectedLen:                     2,
		},
		"downsample_noop_retain_first_and_last_posting_larger_sampling_rates_ratio": {
			existingOffsetsLen:              2,
			postingOffsetsInMemSamplingRate: 32,
			protoSamplingRate:               8,
			expectedLen:                     2,
		},
		"downsample_short_offsets": {
			existingOffsetsLen:              2,
			postingOffsetsInMemSamplingRate: 32,
			protoSamplingRate:               16,
			expectedLen:                     2,
		},
		"downsample_noop_short_offsets": {
			existingOffsetsLen:              1,
			postingOffsetsInMemSamplingRate: 32,
			protoSamplingRate:               16,
			expectedLen:                     1,
		},
		"downsample_proto_has_divisible_sampling_rate": {
			existingOffsetsLen:              100,
			postingOffsetsInMemSamplingRate: 32,
			protoSamplingRate:               16,
			expectedLen:                     50,
		},
		"cannot_downsample_proto_has_no_sampling_rate": {
			existingOffsetsLen:              100,
			postingOffsetsInMemSamplingRate: 32,
			protoSamplingRate:               0,
			expectErr:                       true,
		},
		"cannot_upsample_proto_has_less_frequent_sampling_rate": {
			existingOffsetsLen:              100,
			postingOffsetsInMemSamplingRate: 32,
			protoSamplingRate:               64,
			expectErr:                       true,
		},
		"cannot_downsample_proto_has_non_divisible_sampling_rate": {
			existingOffsetsLen:              100,
			postingOffsetsInMemSamplingRate: 32,
			protoSamplingRate:               10,
			expectErr:                       true,
		},
		"downsample_sampling_rates_ratio_does_not_divide_offsets": {
			existingOffsetsLen:              33,
			postingOffsetsInMemSamplingRate: 32,
			protoSamplingRate:               16,
			expectedLen:                     17,
		},
		"downsample_sampling_rates_ratio_exceeds_offset_len": {
			existingOffsetsLen:              10,
			postingOffsetsInMemSamplingRate: 1024,
			protoSamplingRate:               8,
			expectedLen:                     2,
		},
		"downsample_sampling_rates_ratio_equals_offset_len": {
			existingOffsetsLen:              100,
			postingOffsetsInMemSamplingRate: 100,
			protoSamplingRate:               1,
			expectedLen:                     2,
		},
	}

	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {

			postingsMap := make(map[string]*indexheaderpb.PostingValueOffsets)
			postingsMap["__name__"] = &indexheaderpb.PostingValueOffsets{
				Offsets: createPostingsOffsetsProto(testCase.existingOffsetsLen),
			}

			protoTbl := indexheaderpb.PostingOffsetTable{
				Postings:                      postingsMap,
				PostingOffsetInMemorySampling: testCase.protoSamplingRate,
			}

			sparseTable, err := SparsePostingsOffsetsTableFromProto(&protoTbl, testCase.postingOffsetsInMemSamplingRate)

			if testCase.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, testCase.expectedLen, sparseTable["__name__"].numOffsets())
			}

		})
	}
}

func createPostingsOffsetsProto(n int) []*indexheaderpb.PostingOffset {
	offsets := make([]*indexheaderpb.PostingOffset, n)
	for i := 0; i < n; i++ {
		// Table offsets start after the table's length and entry count fields, like in a real file.
		offsets[i] = &indexheaderpb.PostingOffset{Value: fmt.Sprintf("%d", i), TableOff: int64(8 + i)}
	}
	return offsets
}

func TestSparsePostingsOffsetsForLabelValuePrefix(t *testing.T) {
	testCases := map[string]struct {
		existingValues []string
		prefix         string
		expectedFound  bool
		expectedStart  int
		expectedEnd    int
	}{
		"prefix not found": {
			existingValues: []string{"010", "019", "030", "031"},
			prefix:         "a",
			expectedFound:  false,
		},
		"prefix matches only one sampled offset": {
			existingValues: []string{"010", "019", "030", "031"},
			prefix:         "02",
			expectedFound:  true,
			expectedStart:  1,
			expectedEnd:    2,
		},
		"prefix matches all sampled offsets": {
			existingValues: []string{"010", "019", "030", "031"},
			prefix:         "0",
			expectedFound:  true,
			expectedStart:  0,
			expectedEnd:    4,
		},
		"prefix matches only last offset": {
			existingValues: []string{"010", "019", "030", "031"},
			prefix:         "031",
			expectedFound:  true,
			expectedStart:  3,
			expectedEnd:    4,
		},
		"prefix matches multiple sampled offsets": {
			existingValues: []string{"010", "019", "020", "030", "031"},
			prefix:         "02",
			expectedFound:  true,
			expectedStart:  1,
			expectedEnd:    3,
		},
		"prefix matches only first offset": {
			existingValues: []string{"010", "019", "020", "030", "031"},
			prefix:         "015",
			expectedFound:  true,
			expectedStart:  0,
			expectedEnd:    1,
		},
	}

	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			offsets := SparseTableOffsetsForLabel{}
			for _, value := range testCase.existingValues {
				// This test only cares about the values; any valid table offset will do.
				require.NoError(t, offsets.appendOffset(value, 8))
			}
			start, end, found := offsets.labelValuePrefixOffsets(testCase.prefix)
			assert.Equal(t, testCase.expectedStart, start)
			assert.Equal(t, testCase.expectedEnd, end)
			assert.Equal(t, testCase.expectedFound, found)
		})
	}
}
