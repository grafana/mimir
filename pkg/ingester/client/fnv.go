// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/common/model/fnv.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors.

package client

// Inline and byte-free variant of hash/fnv's fnv64a.

const (
	offset64 = 14695981039346656037
	prime64  = 1099511628211
	offset32 = 2166136261
	prime32  = 16777619
)

// hashNew initializes a new fnv64a hash value.
func hashNew() uint64 {
	return offset64
}

// hashAdd adds a string to a fnv64a hash value, returning the updated hash.
// Note this is the same algorithm as Go stdlib `sum64a.Write()`
func hashAdd(h uint64, s string) uint64 {
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= prime64
	}
	return h
}

// hashAdd adds a string to a fnv64a hash value, returning the updated hash.
func hashAddString(h uint64, s string) uint64 {
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= prime64
	}
	return h
}

// hashAddByte adds a byte to a fnv64a hash value, returning the updated hash.
func hashAddByte(h uint64, b byte) uint64 {
	h ^= uint64(b)
	h *= prime64
	return h
}

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
