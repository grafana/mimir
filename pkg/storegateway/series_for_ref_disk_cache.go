// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"math"
	"os"
	"sync"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/storage"
)

// seriesForRefDiskCacheFilename is the basename used for the per-block on-disk
// SeriesForRef cache. The file lives alongside the block's index header under the
// per-block dir managed by BucketStore so that block-removal also removes the cache.
const seriesForRefDiskCacheFilename = "series-cache.dat"

const (
	// File-level header: 4-byte magic + 4-byte version. Anchored at offset 0 of any
	// non-empty cache file. An empty file (size 0) is also a valid empty cache; on
	// first use we write the header and start appending records past it.
	//
	// The header is the migration anchor for any future format change: a binary that
	// reads an unrecognised magic or version truncates the file and starts fresh
	// rather than silently misinterpreting records under the new layout.
	diskCacheMagicLen       = 4
	diskCacheVersionLen     = 4
	diskCacheFileHeaderSize = diskCacheMagicLen + diskCacheVersionLen

	// diskCacheVersion is the on-disk format version. Bump when the record layout
	// (or anything else replay relies on) changes; an old binary that reads a newer
	// file will fall back to "discard and start fresh".
	diskCacheVersion = uint32(1)

	// Each on-disk record begins with: 4 bytes CRC32 (IEEE) of (ref || length || data)
	// followed by 8 bytes ref (uint64 LE) and 4 bytes length (uint32 LE). The data
	// follows immediately. The CRC catches torn writes that span page boundaries
	// (where the kernel may flush some pages of a single WriteAt and not others) and
	// also detects bit-rot on disk. Replay drops any record whose CRC does not match.
	diskCacheRecordHeaderSize = 16
)

// diskCacheMagic identifies the file format. "sgsc" stands for "store-gateway
// series cache" — easy to grep for in a hex dump if an operator ever needs to
// inspect a cache file by hand.
var diskCacheMagic = [diskCacheMagicLen]byte{'s', 'g', 's', 'c'}

var (
	// errDiskCacheFull is returned by Set when appending the record would push the
	// file past its configured per-block budget. Treated as a soft drop by callers:
	// the in-pod L1 and memcached layers still hold the entry.
	errDiskCacheFull = errors.New("series-for-ref disk cache is full")

	// errDiskCacheValueTooLarge is returned for values that don't fit in a uint32
	// length field. SeriesForRef entries are bounded well below this in practice.
	errDiskCacheValueTooLarge = errors.New("series-for-ref disk cache value exceeds uint32 length")
)

// seriesForRefDiskCacheMetrics is the per-pod set of counters reused across all
// block-level disk cache instances. Holding a single set on the BucketStores avoids
// re-registering per block (which would explode metric cardinality with one series
// per block).
type seriesForRefDiskCacheMetrics struct {
	hits       prometheus.Counter
	misses     prometheus.Counter
	stores     prometheus.Counter
	storesFull prometheus.Counter
	storeErr   prometheus.Counter
	corruption prometheus.Counter
}

func newSeriesForRefDiskCacheMetrics(reg prometheus.Registerer) *seriesForRefDiskCacheMetrics {
	return &seriesForRefDiskCacheMetrics{
		hits: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_bucket_store_series_for_ref_disk_cache_hits_total",
			Help: "Total SeriesForRef on-disk cache hits.",
		}),
		misses: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_bucket_store_series_for_ref_disk_cache_misses_total",
			Help: "Total SeriesForRef on-disk cache misses.",
		}),
		stores: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_bucket_store_series_for_ref_disk_cache_stores_total",
			Help: "Total SeriesForRef on-disk cache successful Set calls.",
		}),
		storesFull: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_bucket_store_series_for_ref_disk_cache_full_total",
			Help: "Total SeriesForRef on-disk cache Set calls dropped because the per-block budget was exhausted.",
		}),
		storeErr: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_bucket_store_series_for_ref_disk_cache_store_errors_total",
			Help: "Total SeriesForRef on-disk cache Set calls that failed with an I/O error.",
		}),
		corruption: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_bucket_store_series_for_ref_disk_cache_corruption_total",
			Help: "Total SeriesForRef on-disk cache events where a file or record was discarded at open: unrecognised file header, torn file header, or per-record CRC mismatch.",
		}),
	}
}

// diskCacheLoc records the location of one cached SeriesForRef entry inside the
// block's cache file. offset points at the data (past the per-record header).
type diskCacheLoc struct {
	offset int64
	length uint32
}

// seriesForRefDiskCache is a per-block append-only on-disk cache for the byte form
// of SeriesForRef entries. It survives memcached evictions and network blips, and on
// a clean shutdown survives pod restarts; on an unclean shutdown it discards any
// trailing record that fails its CRC check rather than risk serving torn bytes.
//
// File format:
//
//	[4-byte magic "sgsc"][4-byte LE version=1]
//	[record 0][record 1]...[record N]
//
// Each record:
//
//	[4-byte LE CRC32(IEEE) of ref||length||data]
//	[8-byte LE ref][4-byte LE length][length bytes data]
//
// On Open we validate the file header and replay records sequentially, computing
// each record's CRC and dropping anything from the first CRC failure (or torn
// trailing record) to end-of-file. Reads use ReadAt and are safe to call
// concurrently.
//
// Bounding: a single per-block byte budget (maxBytes) caps file growth. Once full,
// Set returns errDiskCacheFull and the caller treats it as a soft drop. There is no
// in-place rewrite or LRU rotation in this version — the natural eviction unit is
// the block itself: when the BucketStore removes a block its dir (and this file) is
// deleted.
//
// Concurrency: a single mutex serialises (writeOff bump + WriteAt + index map
// insert). Reads only read-lock the index map and then issue ReadAt on the file
// without holding the mutex; ReadAt is safe under POSIX semantics on the same fd.
type seriesForRefDiskCache struct {
	file     *os.File
	maxBytes int64

	mu       sync.RWMutex
	index    map[storage.SeriesRef]diskCacheLoc
	writeOff int64 // current end of file; protected by mu

	logger log.Logger
	// errLogOnce gates the warn-level log emitted on the first non-budget Set
	// error, so a flaking volume doesn't spam the log. The scope is intentionally
	// per-instance, not per-pod: when a block is unloaded and later re-loaded by
	// the BucketStore, a fresh disk-cache instance gets a fresh once-flag, so a
	// recurring failure on the same volume can re-log on each block reload.
	// A pod-global once would silence the failure forever after the first line.
	errLogOnce sync.Once

	metrics *seriesForRefDiskCacheMetrics
}

// openSeriesForRefDiskCache opens (creating if needed) the per-block cache file at
// path. Existing records are scanned, indexed, and CRC-verified; any record whose
// CRC fails (or that is torn at the tail) causes the file to be truncated to the
// last consistent boundary. A file with an unrecognised header is discarded
// outright. Returns the cache or an error.
func openSeriesForRefDiskCache(path string, maxBytes int64, metrics *seriesForRefDiskCacheMetrics, logger log.Logger) (*seriesForRefDiskCache, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	c := &seriesForRefDiskCache{
		file:     f,
		maxBytes: maxBytes,
		index:    make(map[storage.SeriesRef]diskCacheLoc),
		logger:   logger,
		metrics:  metrics,
	}
	if err := c.replay(); err != nil {
		_ = f.Close()
		return nil, err
	}
	return c, nil
}

// resetFile truncates the file to zero, writes a fresh file header, and resets the
// in-memory index. Used when an existing file has an unrecognised header (different
// version, corrupt, pre-versioned binary) or was 0 < size < headerSize torn.
func (c *seriesForRefDiskCache) resetFile() error {
	if err := c.file.Truncate(0); err != nil {
		return err
	}
	var hdr [diskCacheFileHeaderSize]byte
	copy(hdr[0:diskCacheMagicLen], diskCacheMagic[:])
	binary.LittleEndian.PutUint32(hdr[diskCacheMagicLen:], diskCacheVersion)
	if _, err := c.file.WriteAt(hdr[:], 0); err != nil {
		return err
	}
	c.writeOff = diskCacheFileHeaderSize
	c.index = make(map[storage.SeriesRef]diskCacheLoc)
	return nil
}

// replay walks records from past the file header, rebuilding the in-memory index.
// On a torn write at the tail (header partially flushed, data shorter than declared
// length, or a CRC mismatch) the file is truncated to the last consistent boundary.
// An unrecognised file header (different magic or version) discards all contents
// and re-initialises the file.
func (c *seriesForRefDiskCache) replay() error {
	stat, err := c.file.Stat()
	if err != nil {
		return err
	}
	size := stat.Size()
	if size == 0 {
		return c.resetFile()
	}
	if size < diskCacheFileHeaderSize {
		c.metrics.corruption.Inc()
		level.Warn(c.logger).Log("msg", "series-for-ref disk cache file header is torn; discarding cache", "size", size)
		return c.resetFile()
	}

	var fhdr [diskCacheFileHeaderSize]byte
	if _, err := c.file.ReadAt(fhdr[:], 0); err != nil {
		return err
	}
	if !bytes.Equal(fhdr[0:diskCacheMagicLen], diskCacheMagic[:]) ||
		binary.LittleEndian.Uint32(fhdr[diskCacheMagicLen:]) != diskCacheVersion {
		c.metrics.corruption.Inc()
		level.Warn(c.logger).Log(
			"msg", "series-for-ref disk cache file header is unrecognised; discarding cache",
			"expected_magic", string(diskCacheMagic[:]),
			"expected_version", diskCacheVersion,
		)
		return c.resetFile()
	}

	var rhdr [diskCacheRecordHeaderSize]byte
	off := int64(diskCacheFileHeaderSize)
	for off < size {
		if size-off < diskCacheRecordHeaderSize {
			return c.file.Truncate(off)
		}
		if _, err := c.file.ReadAt(rhdr[:], off); err != nil {
			if errors.Is(err, io.EOF) {
				return c.file.Truncate(off)
			}
			return err
		}
		crc := binary.LittleEndian.Uint32(rhdr[0:4])
		ref := storage.SeriesRef(binary.LittleEndian.Uint64(rhdr[4:12]))
		length := binary.LittleEndian.Uint32(rhdr[12:16])
		dataOff := off + diskCacheRecordHeaderSize
		if dataOff+int64(length) > size {
			return c.file.Truncate(off)
		}
		body := make([]byte, length)
		if _, err := c.file.ReadAt(body, dataOff); err != nil {
			return c.file.Truncate(off)
		}
		// CRC covers ref + length + body, i.e. rhdr[4:16] || body. The CRC bytes
		// themselves (rhdr[0:4]) are excluded.
		h := crc32.NewIEEE()
		_, _ = h.Write(rhdr[4:16])
		_, _ = h.Write(body)
		if h.Sum32() != crc {
			c.metrics.corruption.Inc()
			level.Warn(c.logger).Log(
				"msg", "series-for-ref disk cache record failed CRC check; truncating remainder",
				"offset", off,
				"ref", uint64(ref),
				"length", length,
			)
			return c.file.Truncate(off)
		}
		c.index[ref] = diskCacheLoc{offset: dataOff, length: length}
		off = dataOff + int64(length)
	}
	c.writeOff = off
	return nil
}

// Get reads the cached entry for ref into dst (reusing dst's capacity when
// sufficient) and returns it. Returns nil, false on miss.
func (c *seriesForRefDiskCache) Get(ref storage.SeriesRef, dst []byte) ([]byte, bool) {
	c.mu.RLock()
	loc, ok := c.index[ref]
	c.mu.RUnlock()
	if !ok {
		c.metrics.misses.Inc()
		return nil, false
	}
	if cap(dst) < int(loc.length) {
		dst = make([]byte, loc.length)
	} else {
		dst = dst[:loc.length]
	}
	if _, err := c.file.ReadAt(dst, loc.offset); err != nil {
		// Read failure on the cache is non-fatal; treat as a miss so the caller falls
		// through to the upstream layer.
		c.metrics.misses.Inc()
		return nil, false
	}
	c.metrics.hits.Inc()
	return dst, true
}

// setBufferPoolMaxCap caps the buffer size returned to the pool so a single
// pathologically-large value can't permanently bloat per-block pool entries.
const setBufferPoolMaxCap = 64 * 1024

// setBufferPool keeps reusable buffers used by Set to coalesce header + value into a
// single WriteAt. Avoids a per-Set allocation in the hot path.
var setBufferPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 4*1024)
		return &b
	},
}

// Set appends a record for (ref, value). Returns errDiskCacheFull when the per-
// block budget is exhausted (the caller logs none and continues — the L1 / memcached
// layers still cover the entry); other I/O errors are returned to the caller and
// also logged once per block at warn level so a flaking volume doesn't go silent.
//
// The header and value are combined into a single contiguous buffer and written via
// a single os.File.WriteAt. This is required for crash-consistency, not just
// performance: page-cache writeback is unordered, so issuing two separate WriteAt
// calls can leave the on-disk file with a durable header pointing at undurable
// (zero or partial) value bytes after a crash. Coalescing into one syscall narrows
// the failure window from "any reorder" to "page-boundary tear within a record",
// and any such tear is detected at next-open time by the per-record CRC check.
func (c *seriesForRefDiskCache) Set(ref storage.SeriesRef, value []byte) error {
	if len(value) > math.MaxUint32 {
		return errDiskCacheValueTooLarge
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.index[ref]; exists {
		// Already cached. Avoid duplicate appends; the existing record is fine.
		return nil
	}
	recordSize := int64(diskCacheRecordHeaderSize) + int64(len(value))
	if c.writeOff+recordSize > c.maxBytes {
		c.metrics.storesFull.Inc()
		return errDiskCacheFull
	}

	bufPtr := setBufferPool.Get().(*[]byte)
	buf := (*bufPtr)[:0]
	if cap(buf) < int(recordSize) {
		buf = make([]byte, recordSize)
	} else {
		buf = buf[:recordSize]
	}
	binary.LittleEndian.PutUint64(buf[4:12], uint64(ref))
	binary.LittleEndian.PutUint32(buf[12:16], uint32(len(value)))
	copy(buf[diskCacheRecordHeaderSize:], value)
	binary.LittleEndian.PutUint32(buf[0:4], crc32.ChecksumIEEE(buf[4:]))

	_, err := c.file.WriteAt(buf, c.writeOff)
	if cap(buf) <= setBufferPoolMaxCap {
		*bufPtr = buf[:0]
		setBufferPool.Put(bufPtr)
	}
	if err != nil {
		c.metrics.storeErr.Inc()
		c.errLogOnce.Do(func() {
			level.Warn(c.logger).Log("msg", "series-for-ref disk cache write failed; further write errors for this block will not be logged", "err", err)
		})
		return err
	}
	c.index[ref] = diskCacheLoc{offset: c.writeOff + diskCacheRecordHeaderSize, length: uint32(len(value))}
	c.writeOff += recordSize
	c.metrics.stores.Inc()
	return nil
}

// Close releases the underlying file. Subsequent Get/Set calls are not safe.
func (c *seriesForRefDiskCache) Close() error {
	if c == nil || c.file == nil {
		return nil
	}
	return c.file.Close()
}
