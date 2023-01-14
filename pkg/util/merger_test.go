// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/merger_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package util

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMergeSlices(t *testing.T) {
	tests := []struct {
		name string
		args [][]string
		want []string
	}{
		{
			name: "empty input",
			want: nil,
		},
		{
			name: "single input",
			args: [][]string{{"a", "b", "c"}},
			want: []string{"a", "b", "c"},
		},
		{
			name: "two inputs same",
			args: [][]string{{"a", "b", "c"}, {"a", "b", "c"}},
			want: []string{"a", "b", "c"},
		},
		{
			name: "two inputs interleaved",
			args: [][]string{{"a", "c", "e"}, {"b", "d", "f"}},
			want: []string{"a", "b", "c", "d", "e", "f"},
		},
		{
			name: "first input short",
			args: [][]string{{"a"}, {"b", "d", "f"}},
			want: []string{"a", "b", "d", "f"},
		},
		{
			name: "second input short",
			args: [][]string{{"a", "c", "e"}, {"b"}},
			want: []string{"a", "b", "c", "e"},
		},
		{
			name: "some duplicates",
			args: [][]string{{"a", "c", "e"}, {"b", "c"}},
			want: []string{"a", "b", "c", "e"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MergeSlices(tt.args...)
			require.Equal(t, tt.want, got)
		})
	}
}
