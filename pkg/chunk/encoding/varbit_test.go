// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors.

package encoding

import "testing"

func TestCountBits(t *testing.T) {
	for i := byte(0); i < 56; i++ {
		for j := byte(0); j <= 8; j++ {
			for k := byte(0); k < 8; k++ {
				p := uint64(bitMask[j][k]) << i
				gotLeading, gotSignificant := countBits(p)
				wantLeading := 56 - i + k
				wantSignificant := j
				if j+k > 8 {
					wantSignificant -= j + k - 8
				}
				if wantLeading > 31 {
					wantSignificant += wantLeading - 31
					wantLeading = 31
				}
				if p == 0 {
					wantLeading = 0
					wantSignificant = 0
				}
				if wantLeading != gotLeading {
					t.Errorf(
						"unexpected leading bit count for i=%d, j=%d, k=%d; want %d, got %d",
						i, j, k, wantLeading, gotLeading,
					)
				}
				if wantSignificant != gotSignificant {
					t.Errorf(
						"unexpected significant bit count for i=%d, j=%d, k=%d; want %d, got %d",
						i, j, k, wantSignificant, gotSignificant,
					)
				}
			}
		}
	}
}
