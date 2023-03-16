// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/math/math.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package math

import (
	"golang.org/x/exp/constraints"
)

// Max returns the maximum of two ordered arguments.
func Max[T constraints.Ordered](a, b T) T {
	if a > b {
		return a
	}
	return b
}

// Min returns the minimum of two ordered arguments.
func Min[T constraints.Ordered](a, b T) T {
	if a < b {
		return a
	}
	return b
}
