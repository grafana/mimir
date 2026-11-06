// SPDX-License-Identifier: AGPL-3.0-only

package mimirpb

import (
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
)

func TestUnsafeFromExemplarProtosToExemplars(t *testing.T) {
	input := []Exemplar{{
		Labels:      []LabelAdapter{{Name: BorrowString("trace_id"), Value: BorrowString("abc123")}},
		Value:       1.5,
		TimestampMs: 1234,
	}}
	got := UnsafeFromExemplarProtosToExemplars(input)
	require.Len(t, got, 1)
	require.True(t, labels.Equal(labels.FromStrings("trace_id", "abc123"), got[0].Labels))
	require.Equal(t, 1.5, got[0].Value)
	require.Equal(t, int64(1234), got[0].Ts)
	require.False(t, got[0].HasTs) // Preserve the existing protobuf conversion semantics.
}
