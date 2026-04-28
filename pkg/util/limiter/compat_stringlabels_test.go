// SPDX-License-Identifier: AGPL-3.0-only

//go:build !slicelabels && !dedupelabels

package limiter

import (
	"reflect"
	"testing"
	"unsafe"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
)

func requireSameLabels(t *testing.T, a, b labels.Labels) {
	t.Helper()
	aVal := reflect.ValueOf(a)
	bVal := reflect.ValueOf(b)

	// We don't validate the reflection because the runtime guaranteed the correct type by the build tag.
	aStr := aVal.FieldByName("data").String()
	bStr := bVal.FieldByName("data").String()

	require.Equal(t, len(aStr), len(bStr))

	if len(aStr) > 0 && len(bStr) > 0 && len(aStr) == len(bStr) {
		aPtr := unsafe.Pointer(unsafe.StringData(aStr))
		bPtr := unsafe.Pointer(unsafe.StringData(bStr))
		require.Equal(t, aPtr, bPtr, "labels should share the same internal data pointer (stringlabels)")
	}

}
