// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/common/model/fnv.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors.

package client

// Inline and byte-free variant of hash/fnv's fnv64a.

const (
	offset32 = 2166136261
	prime32  = 16777619
)

// HashNew32 initializies a new fnv32 hash value.
func HashNew32() uint32 {
	return offset32
}

// HashAdd32 adds a string to a fnv32 hash value, returning the updated hash.
// Note this is the same algorithm as Go stdlib `sum32.Write()`
func HashAdd32(h uint32, s string) uint32 {
	for i := 0; i < len(s); i++ {
		h *= prime32
		h ^= uint32(s[i])
	}
	return h
}

// HashAddByte32 adds a byte to a fnv32 hash value, returning the updated hash.
func HashAddByte32(h uint32, b byte) uint32 {
	h *= prime32
	h ^= uint32(b)
	return h
}

// HashNew32a initializies a new fnv32a hash value.
func HashNew32a() uint32 {
	return offset32
}

// HashAdd32a adds a string to a fnv32a hash value, returning the updated hash.
// Note this is the same algorithm as Go stdlib `sum32.Write()`
func HashAdd32a(h uint32, s string) uint32 {
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= prime32
	}
	return h
}

// HashAddByte32a adds a byte to a fnv32a hash value, returning the updated hash.
func HashAddByte32a(h uint32, b byte) uint32 {
	h ^= uint32(b)
	h *= prime32
	return h
}
