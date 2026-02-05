// SPDX-License-Identifier: AGPL-3.0-only

package index

import (
	"context"
	"encoding/binary"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/index"
)

//type CacheKey[K comparable] interface{
//	Key() K
//}

//// InMemoryCacheKey defines behavior required for in-memory cache implementations.
//// InMemoryCacheKey implementations do not need to pre-hash keys.
//// Hashing & potential collision is handled internally by lru.LRU caches or other map-based types.
//type InMemoryCacheKey[T comparable] interface {
//	Key() T
//
//	// Size represents the footprint of the cache key in memory
//	// to track cache size and check whether eviction is required to add a new entry.
//	Size() uint64
//}

//type InMemoryPostingsOffsetCacheKeyFunc func(tenantID string, blockID ulid.ULID, l labels.Label) InMemoryCacheKey

// RemoteCacheKey defines behavior required for remote (memcached, etc.) cache implementations.
// RemoteCacheKey implementations MUST pre-hash keys in a manner which avoids collision.
type RemoteCacheKey interface {
	Key() string
}

type RemotePostingsOffsetCacheKeyFunc func(userID string, blockID ulid.ULID, l labels.Label) RemoteCacheKey

type PostingsOffsetTableCache interface {
	StorePostingsOffset(userID string, blockID ulid.ULID, lbl labels.Label, v index.Range, ttl time.Duration)
	FetchPostingsOffset(ctx context.Context, userID string, blockID ulid.ULID, lbl labels.Label) (index.Range, bool)
}

// PostingsOffsetCacheCodec provides encoding and decoding for PostingsOffsetTableCache values.
//
// Encoding and decoding is strictly to store and retrieve int64 values for index.Range instances.
//
// Implementations must NOT be tied to specific TSDB layout assumptions
// such as the length of Postings entries or relation between consecutive Postings entries -
// This logic is left for the implementations of the PostingOffsetTable interface.
type PostingsOffsetCacheCodec interface {
	Encode(index.Range) []byte
	Decode([]byte) (index.Range, error)
}

type BigEndianPostingsOffsetCodec struct{}

func (c BigEndianPostingsOffsetCodec) Encode(rng index.Range) []byte {
	buf := make([]byte, 0, 2*binary.MaxVarintLen64)
	buf = binary.AppendUvarint(buf, uint64(rng.Start))
	buf = binary.AppendUvarint(buf, uint64(rng.End))
	return buf
}

func (c BigEndianPostingsOffsetCodec) Decode(b []byte) (index.Range, error) {
	rng := index.Range{}

	start, n := binary.Uvarint(b)
	if n <= 0 {
		return rng, errors.New("invalid postings offset encoding")
	}
	rng.Start = int64(start)

	end, n := binary.Uvarint(b)
	if n <= 0 {
		return rng, errors.New("invalid postings offset encoding")
	}
	rng.End = int64(end)

	return rng, nil
}
