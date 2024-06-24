// SPDX-License-Identifier: AGPL-3.0-only

package alertmanager

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuildKeyvals(t *testing.T) {
	tests := []struct {
		name string
		msg  string
		ctx  []any
		want []any
	}{
		{
			name: "empty context slice",
			msg:  "test message",
			ctx:  []any{},
			want: []any{"msg", "test message"},
		},
		{
			name: "nil context slice",
			msg:  "test message",
			ctx:  nil,
			want: []any{"msg", "test message"},
		},
		{
			name: "context slice with one element",
			msg:  "test message",
			ctx:  []any{"key1"},
			want: []any{"msg", "test message", "key1"},
		},
		{
			name: "context slice with two elements",
			msg:  "test message",
			ctx:  []any{"key1", "value1"},
			want: []any{"msg", "test message", "key1", "value1"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := buildKeyvals(test.msg, test.ctx)
			require.Equal(t, test.want, got)
		})
	}
}
