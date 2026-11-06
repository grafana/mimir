// SPDX-License-Identifier: AGPL-3.0-only

//go:build slicelabels

package mimirpb

import (
	"testing"
	"unsafe"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
)

func TestUnsafeLabelConversionsAliasStorage(t *testing.T) {
	input := []LabelAdapter{{Name: BorrowString("label"), Value: BorrowString("value")}}
	converted := UnsafeFromLabelAdaptersToLabels(input)
	require.Equal(t, unsafe.Pointer(&input[0]), unsafe.Pointer(&converted[0]))
	input[0].Value = BorrowString("updated")
	require.Equal(t, "updated", converted.Get("label"))

	reversed := FromLabelsToLabelAdapters(converted)
	require.Equal(t, unsafe.Pointer(&converted[0]), unsafe.Pointer(&reversed[0]))
	converted[0].Value = "again"
	require.True(t, reversed[0].Value.EqualString("again"))
}

func TestCopyLabels(t *testing.T) {
	t.Parallel()

	expected := []labels.Label{
		{Name: "name1", Value: "value1"},
		{Name: "name2", Value: "value2"},
	}
	got := []labels.Label(CopyLabels(expected))
	require.Equal(t, expected, got)

	// Check that all strings share the same underlying buffer
	var base uintptr
	var offset int
	for i, label := range got {
		name := unsafe.StringData(label.Name)
		value := unsafe.StringData(label.Value)

		if i == 0 {
			base = uintptr(unsafe.Pointer(name))
		}

		require.Equal(t, base+uintptr(offset), uintptr(unsafe.Pointer(name)))
		offset += len(label.Name)
		require.Equal(t, base+uintptr(offset), uintptr(unsafe.Pointer(value)))
		offset += len(label.Value)
	}
}
