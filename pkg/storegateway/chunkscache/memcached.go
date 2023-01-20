// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/store/cache/memcached.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package chunkscache

import (
	"context"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/efficientgo/core/errors"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/cache"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/grafana/mimir/pkg/storegateway/storepb"
	"github.com/grafana/mimir/pkg/util/pool"
)

const (
	memcachedDefaultTTL = 7 * 24 * time.Hour
)

// MemcachedChunksCache is a memcached-based index cache.
type MemcachedChunksCache struct {
	logger    log.Logger
	memcached cache.MemcachedClient

	// Metrics.
	requests prometheus.Counter
	hits     prometheus.Counter
}

// NewMemcachedChunksCache makes a new MemcachedChunksCache.
func NewMemcachedChunksCache(logger log.Logger, memcached cache.MemcachedClient, reg prometheus.Registerer) (*MemcachedChunksCache, error) {
	c := &MemcachedChunksCache{
		logger:    logger,
		memcached: memcached,
	}

	c.requests = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "cortex_bucket_store_chunks_cache_requests_total",
		Help: "Total number of items requests to the cache.",
	})

	c.hits = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "cortex_bucket_store_chunks_cache_hits_total",
		Help: "Total number of items requests to the cache that were a hit.",
	})

	level.Info(logger).Log("msg", "created memcached index cache")

	return c, nil
}

func (c *MemcachedChunksCache) FetchMultiChunks(ctx context.Context, userID string, keys []Key, chunksPool *pool.SafeSlabPool[storepb.AggrChunk], bytesPool *pool.SafeSlabPool[byte]) (hits map[Key][]storepb.AggrChunk) {
	c.requests.Add(float64(len(keys)))

	keysMap := make(map[string]Key, len(keys))
	strKeys := make([]string, 0, len(keys))
	for i, r := range keys {
		k := chunksKey(userID, r)
		keysMap[k] = keys[i]
		strKeys = append(strKeys, k)
	}

	hitBytes := c.memcached.GetMulti(ctx, strKeys, cache.WithAllocator(slabPoolAllocator{bytesPool}))
	if len(hitBytes) > 0 {
		hits = make(map[Key][]storepb.AggrChunk, len(hitBytes))
	}

	for key, b := range hitBytes {
		parsed, err := parseChunkSlice(b, chunksPool)
		if err != nil {
			level.Warn(c.logger).Log("msg", "couldn't parse cached chunk")
			continue
		}
		hits[keysMap[key]] = parsed
	}
	c.hits.Add(float64(len(hits)))
	return
}

func parseChunkSlice(buff []byte, chunksPool *pool.SafeSlabPool[storepb.AggrChunk]) ([]storepb.AggrChunk, error) {
	numChunks, read := binary.Uvarint(buff)
	parsed := chunksPool.Get(int(numChunks))

	for cIdx := range parsed {
		protoSize, uvarIntSize := binary.Uvarint(buff[read:])
		read += uvarIntSize

		err := parsed[cIdx].Unmarshal(buff[read : read+int(protoSize)])
		if err != nil {
			return nil, errors.Wrap(err, "parsing cached chunk")
		}
		read += int(protoSize)

		dataSize, uvarIntSize := binary.Uvarint(buff[read:])
		read += uvarIntSize

		parsed[cIdx].Raw.Data = buff[read : read+int(dataSize)]
		read += int(dataSize)
	}
	return parsed, nil
}

func chunksKey(userID string, r Key) string {
	return fmt.Sprintf("C:%s:%s:%d", userID, r.BlockID, r.FirstChunk)
}

func (c *MemcachedChunksCache) StoreChunks(ctx context.Context, userID string, r Key, v []storepb.AggrChunk) {
	encoded, err := encodeAggrgChunks(v)
	if err != nil {
		level.Warn(c.logger).Log("msg", "couldn't encode chunks to cache", "err", err)
		return
	}
	err = c.memcached.SetAsync(ctx, chunksKey(userID, r), encoded, memcachedDefaultTTL)
	if err != nil {
		level.Warn(c.logger).Log("msg", "storing chunks", "err", err)
	}
}

// Format:
// | num_chunks (uvarint) | chunk (chunk) ... |
// chunk: | proto_size (uvarint) | proto_marshalled (bytes) | data_size (uvarint) | data (bytes) |
func encodeAggrgChunks(v []storepb.AggrChunk) ([]byte, error) {
	// We have to allocate a new slice since we still don't use pooling with SETs,

	// Count the number of bytes we will encode
	tempVarIntBuf := make([]byte, 0, 10)
	totalEncodedSize := 0

	var swapData []byte
	for _, chk := range v {
		// Calculate protobuf encoding without encoding actual chunk data
		chk.Raw.Data, swapData = nil, chk.Raw.Data
		chkSize := chk.Size()
		totalEncodedSize += chkSize
		totalEncodedSize += uvarIntSize(uint64(chkSize), tempVarIntBuf)
		chk.Raw.Data = swapData

		totalEncodedSize += len(chk.Raw.Data)
		totalEncodedSize += uvarIntSize(uint64(len(chk.Raw.Data)), tempVarIntBuf)
	}
	totalEncodedSize += uvarIntSize(uint64(len(v)), tempVarIntBuf)

	encoded := make([]byte, totalEncodedSize)
	written := binary.PutUvarint(encoded, uint64(len(v)))

	for _, chk := range v {
		chk.Raw.Data, swapData = nil, chk.Raw.Data

		chkSize := chk.Size()
		written += binary.PutUvarint(encoded[written:], uint64(chkSize))

		protoWritten, err := chk.MarshalToSizedBuffer(encoded[written : written+chkSize])
		if err != nil {
			return nil, errors.Wrap(err, "encoding chunks cache item")
		}
		written += protoWritten

		written += binary.PutUvarint(encoded[written:], uint64(len(swapData)))
		written += copy(encoded[written:], swapData)
		chk.Raw.Data = swapData
	}

	if written != len(encoded) {
		panic("i have a bug")
	}
	return encoded, nil
}

func uvarIntSize(v uint64, buf []byte) int {
	buf = buf[:0]
	return len(binary.AppendUvarint(buf, v))
}

type slabPoolAllocator struct {
	p *pool.SafeSlabPool[byte]
}

func (s slabPoolAllocator) Get(sz int) *[]byte {
	b := s.p.Get(sz)
	return &b
}

func (s slabPoolAllocator) Put(*[]byte) {}
