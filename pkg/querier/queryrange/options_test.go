// SPDX-License-Identifier: AGPL-3.0-only
package queryrange

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_DecodeOptions(t *testing.T) {
	for _, tt := range []struct {
		name     string
		input    *http.Request
		expected *Options
	}{
		{
			name: "default",
			input: &http.Request{
				Header: http.Header{},
			},
			expected: &Options{},
		},
		{
			name: "disable cache",
			input: &http.Request{
				Header: http.Header{
					CacheControlHeader: []string{NoStoreValue},
				},
			},
			expected: &Options{
				CacheDisabled: true,
			},
		},
		{
			name: "custom sharding",
			input: &http.Request{
				Header: http.Header{
					TotalShardsControlHeader: []string{"64"},
				},
			},
			expected: &Options{
				TotalShards: 64,
			},
		},
		{
			name: "disable sharding",
			input: &http.Request{
				Header: http.Header{
					TotalShardsControlHeader: []string{"0"},
				},
			},
			expected: &Options{
				ShardingDisabled: true,
			},
		},
	} {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			actual := &Options{}
			DecodeOptions(tt.input, actual)
			require.Equal(t, tt.expected, actual)
		})
	}
}
