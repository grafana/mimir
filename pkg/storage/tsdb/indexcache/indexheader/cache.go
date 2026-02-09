// SPDX-License-Identifier: AGPL-3.0-only

package indexheader

import (
	"context"
	"encoding/base64"
	"fmt"
	"time"
	"unsafe"

	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/index"
	"golang.org/x/crypto/blake2b"
)

type PostingsOffsetTableCache interface {
	StorePostingsOffset(userID string, blockID ulid.ULID, lbl labels.Label, v index.Range, ttl time.Duration)
	FetchPostingsOffset(ctx context.Context, userID string, blockID ulid.ULID, lbl labels.Label) (index.Range, bool)
}

const (
	cacheKeySep             = ":"
	postingsOffsetKeyPrefix = "PO"
)

// InMemoryCacheKey defines behavior required for in-memory cache implementations.
// InMemoryCacheKey implementations do not need to pre-hash keys.
// Hashing & potential collision is handled internally by lru.LRU caches or other map-based types.
type InMemoryCacheKey interface {
	// Size represents the footprint of the cache key in memory
	// to track cache size and check whether eviction is required to add a new entry.
	Size() uint64
}

// RemoteCacheKey defines behavior required for remote (memcached, etc.) cache implementations.
// RemoteCacheKey implementations MUST pre-hash keys in a manner which avoids collision.
type RemoteCacheKey interface {
	Key() string
}

type PostingsOffsetCacheKey struct {
	tenantID string
	blockID  ulid.ULID
	lbl      labels.Label
}

// Key implements RemoteCacheKey with a crypto-hashed representation of tenant, block, and label pair.
func (k PostingsOffsetCacheKey) Key() string {
	return postingsOffsetCacheKey(
		postingsOffsetKeyPrefix, k.tenantID, k.blockID.String(), k.lbl,
	)
}

// Size implements InMemoryCacheKey
func (k PostingsOffsetCacheKey) Size() uint64 {
	return stringSize(k.tenantID) + ulidSize + stringSize(k.lbl.Name) + stringSize(k.lbl.Value)
}

func postingsOffsetCacheKey(prefix, tenantID, blockID string, lbl labels.Label) string {
	// Compute the label hash.
	lblHash, hashLen := postingsOffsetCacheKeyLabelID(lbl)

	// Preallocate the byte slice used to store the cache key.
	encodedHashLen := base64.RawURLEncoding.EncodedLen(hashLen)
	expectedLen := len(prefix) + len(tenantID) + 1 + ulid.EncodedSize + 1 + encodedHashLen
	key := make([]byte, expectedLen)
	offset := 0

	offset += copy(key[offset:], prefix)
	offset += copy(key[offset:], tenantID)
	offset += copy(key[offset:], cacheKeySep)
	offset += copy(key[offset:], blockID)
	offset += copy(key[offset:], cacheKeySep)
	base64.RawURLEncoding.Encode(key[offset:], lblHash[:hashLen])
	offset += encodedHashLen

	sizedKey := key[:offset]
	// Convert []byte to string with no extra allocation.
	return *(*string)(unsafe.Pointer(&sizedKey))
}

// postingsCacheKeyLabelID returns the hash of the input label or the label itself,
// if the label it is shorter than the size of the hash.
func postingsOffsetCacheKeyLabelID(lbl labels.Label) (out [blake2b.Size256]byte, outLen int) {
	// Compute the expected length.
	expectedLen := len(lbl.Name) + len(cacheKeySep) + len(lbl.Value)

	// If the whole label is smaller than the hash, then shortcut hashing and directly write out the result.
	if expectedLen <= blake2b.Size256 {
		offset := 0
		offset += copy(out[offset:], lbl.Name)
		offset += copy(out[offset:], cacheKeySep)
		offset += copy(out[offset:], lbl.Value)
		return out, offset
	}

	// Get a buffer from the pool and fill it with the label name/value pair to hash.
	bp := postingsOffsetCacheKeyLabelHashBufferPool.Get().(*[]byte)
	buf := *bp

	if cap(buf) < expectedLen {
		buf = make([]byte, expectedLen)
	} else {
		buf = buf[:expectedLen]
	}

	offset := 0
	offset += copy(buf[offset:], lbl.Name)
	offset += copy(buf[offset:], cacheKeySep)
	offset += copy(buf[offset:], lbl.Value)

	// This is expected to be always equal. If it's not, then it's a severe bug.
	if offset != expectedLen {
		panic(fmt.Sprintf("postingsOffsetCacheKeyLabelID() computed an invalid expected length (expected: %d, actual: %d)", expectedLen, offset))
	}

	// Use cryptographically hash functions to avoid hash collisions
	// which would end up in wrong query results.
	hash := blake2b.Sum256(buf)

	// Reuse the same pointer to put the buffer back into the pool.
	*bp = buf
	postingsOffsetCacheKeyLabelHashBufferPool.Put(bp)

	return hash, len(hash)
}
