// SPDX-License-Identifier: AGPL-3.0-only

//go:build dedupelabels

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
	aData := aVal.FieldByName("data")
	bData := bVal.FieldByName("data")
	if aData.Len() == 0 && bData.Len() == 0 {
		return
	}
	if aData.Len() > 0 && bData.Len() > 0 && aData.Len() == bData.Len() {
		aPtr := aData.Pointer()
		bPtr := bData.Pointer()
		require.Equal(t, aPtr, bPtr, "labels should share the same data slice (dedupelabels)")
	}
}
