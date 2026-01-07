// SPDX-License-Identifier: AGPL-3.0-only

//go:build slicelabels

package limiter

import (
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
)

func requireSameLabels(t *testing.T, a, b labels.Labels) {
	t.Helper()
	aVal := reflect.ValueOf(a)
	bVal := reflect.ValueOf(b)

	// We don't validate the reflection because the runtime guaranteed the correct type by the build tag.
	aSlice := aVal.FieldByName("labels")
	bSlice := bVal.FieldByName("labels")

	if aSlice.Len() == 0 && bSlice.Len() == 0 {
		// Both empty
		return
	}

	if aSlice.Len() > 0 && bSlice.Len() > 0 && aSlice.Leng() == bSlice.Len() {
		aPtr := aSlice.Pointer()
		bPtr := bSlice.Pointer()
		require.Equal(t, aPtr, bPtr, "labels should share the same slice backing array (slicelabels)")
	}
}
