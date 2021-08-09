// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/hash_fp.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package util

import "github.com/prometheus/common/model"

// HashFP simply moves entropy from the most significant 48 bits of the
// fingerprint into the least significant 16 bits (by XORing) so that a simple
// MOD on the result can be used to pick a mutex while still making use of
// changes in more significant bits of the fingerprint. (The fast fingerprinting
// function we use is prone to only change a few bits for similar metrics. We
// really want to make use of every change in the fingerprint to vary mutex
// selection.)
func HashFP(fp model.Fingerprint) uint32 {
	return uint32(fp ^ (fp >> 32) ^ (fp >> 16))
}
