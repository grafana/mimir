// SPDX-License-Identifier: AGPL-3.0-only

//go:build slicelabels

package mimirpb

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
)

func TestUnsafeExemplarConversionAliasesLabels(t *testing.T) {
	buf := []byte("abc123")
	input := []Exemplar{{Labels: []LabelAdapter{{
		Name:  BorrowString("trace_id"),
		Value: BorrowString(unsafe.String(unsafe.SliceData(buf), len(buf))),
	}}}}
	got := UnsafeFromExemplarProtosToExemplars(input)
	buf[0] = 'z'
	require.Equal(t, "zbc123", got[0].Labels.Get("trace_id"))
	input[0].Labels[0] = LabelAdapter{Name: BorrowString("span_id"), Value: BorrowString("def456")}
	require.Equal(t, "def456", got[0].Labels.Get("span_id"))
}
