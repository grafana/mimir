// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/strings.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package util

import (
	"strings"
	"unsafe"
)

// StringsMap returns a map where keys are input values.
func StringsMap(values []string) map[string]bool {
	out := make(map[string]bool, len(values))
	for _, v := range values {
		out[v] = true
	}
	return out
}

// JoinStrings is like [strings.Join], but generic over the string type.
func JoinStrings[S interface{ ~string }](elems []S, sep string) string {
	return strings.Join(*(*[]string)(unsafe.Pointer(&elems)), sep)
}
