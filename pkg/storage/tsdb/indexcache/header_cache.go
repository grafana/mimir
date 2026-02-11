// SPDX-License-Identifier: AGPL-3.0-only

package indexcache

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
	StorePostingsOffset(tenantID string, blockID ulid.ULID, lbl labels.Label, rng index.Range, ttl time.Duration)
	FetchPostingsOffset(ctx context.Context, tenantID string, blockID ulid.ULID, lbl labels.Label) (index.Range, bool)

	StorePostingsOffsetsForMatcher(tenantID string, blockID ulid.ULID, m *labels.Matcher, isSubtract bool, rngs []index.Range, ttl time.Duration)
	FetchPostingsOffsetsForMatcher(ctx context.Context, tenantID string, blockID ulid.ULID, m *labels.Matcher, isSubtract bool) ([]index.Range, bool)
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

type PostingsOffsetsForMatcherCacheKey struct {
	tenantID   string
	blockID    ulid.ULID
	matcherStr string
	isSubtract bool
}

// Key implements RemoteCacheKey with a crypto-hashed representation of tenant, block, and label pair.
func (k PostingsOffsetsForMatcherCacheKey) Key() string {
	return postingsOffsetsForMatcherCacheKey(
		postingsOffsetKeyPrefix, k.tenantID, k.blockID.String(), k.matcherStr, k.isSubtract,
	)
}

// Size implements InMemoryCacheKey
func (k PostingsOffsetsForMatcherCacheKey) Size() uint64 {
	return stringSize(k.tenantID) + ulidSize + stringSize(k.matcherStr) + 1 // add a byte for boolean isSubtract
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

func postingsOffsetsForMatcherCacheKey(prefix, tenantID, blockID string, matcherStr string, isSubtract bool) string {
	// Compute the matcher hash.
	lblHash, hashLen := postingsOffsetsForMatcherCacheKeyMatcherID(matcherStr, isSubtract)

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

func postingsOffsetsForMatcherCacheKeyMatcherID(matcherStr string, isSubtract bool) (out [blake2b.Size256]byte, outLen int) {
	if isSubtract {
		matcherStr = "not:" + matcherStr
	}
	matcherLen := len(matcherStr)

	// If the matcher string is smaller than the hash, then shortcut hashing and directly write out the result.
	if matcherLen <= blake2b.Size256 {
		offset := copy(out[:], matcherStr)
		return out, offset
	}
	// Get a buffer from the pool and fill it with the label name/value pair to hash.
	bp := postingsOffsetCacheKeyLabelHashBufferPool.Get().(*[]byte)
	buf := *bp
	if cap(buf) < matcherLen {
		buf = make([]byte, matcherLen)
	} else {
		buf = buf[:matcherLen]
	}

	//offset := 0 // TODO offset not actually used like in other impl; remove or see if should be used
	//offset += copy(buf[offset:], matcherStr)

	// Use cryptographically hash functions to avoid hash collisions
	// which would end up in wrong query results.
	hash := blake2b.Sum256([]byte(matcherStr))

	// Reuse the same pointer to put the buffer back into the pool.
	*bp = buf
	postingsOffsetCacheKeyLabelHashBufferPool.Put(bp)

	return hash, len(hash)
}

type NoopHeaderCache struct{}

func (n NoopHeaderCache) StorePostingsOffsetsForMatcher(tenantID string, blockID ulid.ULID, m *labels.Matcher, isSubtract bool, rngs []index.Range, ttl time.Duration) {
}

func (n NoopHeaderCache) FetchPostingsOffsetsForMatcher(ctx context.Context, tenantID string, blockID ulid.ULID, m *labels.Matcher, isSubtract bool) ([]index.Range, bool) {
	return nil, false
}

func (n NoopHeaderCache) StorePostingsOffset(string, ulid.ULID, labels.Label, index.Range, time.Duration) {

}

func (n NoopHeaderCache) FetchPostingsOffset(context.Context, string, ulid.ULID, labels.Label) (index.Range, bool) {
	return index.Range{}, false
}
