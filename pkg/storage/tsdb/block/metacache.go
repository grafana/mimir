package block

import (
	"unsafe"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/tsdb"
)

type MetaCache struct {
	lru *lru.Cache[ulid.ULID, *Meta]
}

func NewMetaCache(size int) *MetaCache {
	l, err := lru.New[ulid.ULID, *Meta](size)
	// This can only happen if size < 0.
	if err != nil {
		panic(err.Error())
	}

	return &MetaCache{
		lru: l,
	}
}

func (mc *MetaCache) Put(id ulid.ULID, meta *Meta) {
	// We want to cache blocks that went through several compactions, because those blocks have higher chance of being reused over and over.
	if meta.Compaction.Level < 4 {
		return
	}

	// parsed *Meta with 64 sources takes about 1KB in memory. Most recent blocks have fewer sources.
	if len(meta.Compaction.Sources) < 128 {
		return
	}

	mc.lru.Add(id, meta)
}

func (mc *MetaCache) Get(id ulid.ULID) *Meta {
	val, ok := mc.lru.Get(id)
	if !ok {
		return nil
	}
	return val
}

func (mc *MetaCache) Stats() (items int, bytesSize int64) {
	for _, m := range mc.lru.Values() {
		items++
		bytesSize += sizeOfUlid // for a key
		bytesSize += MetaBytesSize(m)
	}
	return items, bytesSize
}

var sizeOfUlid = int64(unsafe.Sizeof(ulid.ULID{}))
var sizeOfBlockDesc = int64(unsafe.Sizeof(tsdb.BlockDesc{}))

func MetaBytesSize(m *Meta) int64 {
	size := int64(0)
	size += int64(unsafe.Sizeof(*m))
	size += int64(len(m.Compaction.Sources)) * sizeOfUlid
	size += int64(len(m.Compaction.Parents)) * sizeOfBlockDesc

	for _, h := range m.Compaction.Hints {
		size += int64(unsafe.Sizeof(h))
	}
	return size
}
