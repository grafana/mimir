// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newDiskCacheForTest opens a fresh per-block disk cache backed by a fresh registry
// and returns the cache, the underlying file path, and the registry so tests can
// gather metrics via the standard pattern.
func newDiskCacheForTest(t *testing.T, maxBytes int64) (*seriesForRefDiskCache, string, *prometheus.Registry) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, seriesForRefDiskCacheFilename)
	reg := prometheus.NewPedanticRegistry()
	metrics := newSeriesForRefDiskCacheMetrics(reg)
	c, err := openSeriesForRefDiskCache(path, maxBytes, metrics, log.NewNopLogger())
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	return c, path, reg
}

// counterValue gathers the value of a counter metric (no labels) from reg.
func counterValue(t *testing.T, reg *prometheus.Registry, name string) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		var sum float64
		for _, m := range mf.GetMetric() {
			if c := m.GetCounter(); c != nil {
				sum += c.GetValue()
			}
		}
		return sum
	}
	return 0
}

func TestSeriesForRefDiskCache_EmptyOpen(t *testing.T) {
	c, path, _ := newDiskCacheForTest(t, 1<<20)

	require.Empty(t, c.index)
	// A brand-new file is initialised with the file-level header so future writes can
	// be distinguished from a torn-zero-length file.
	require.Equal(t, int64(diskCacheFileHeaderSize), c.writeOff)

	stat, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, int64(diskCacheFileHeaderSize), stat.Size())
}

func TestSeriesForRefDiskCache_SetGetRoundTrip(t *testing.T) {
	c, _, reg := newDiskCacheForTest(t, 1<<20)

	require.NoError(t, c.Set(7, []byte("alpha")))
	require.NoError(t, c.Set(99, []byte("beta-payload")))

	got, ok := c.Get(7, nil)
	require.True(t, ok)
	require.Equal(t, []byte("alpha"), got)

	got, ok = c.Get(99, nil)
	require.True(t, ok)
	require.Equal(t, []byte("beta-payload"), got)

	// Missing ref → miss.
	_, ok = c.Get(404, nil)
	require.False(t, ok)

	assert.Equal(t, 2.0, counterValue(t, reg, "cortex_bucket_store_series_for_ref_disk_cache_stores_total"))
	assert.Equal(t, 2.0, counterValue(t, reg, "cortex_bucket_store_series_for_ref_disk_cache_hits_total"))
	assert.Equal(t, 1.0, counterValue(t, reg, "cortex_bucket_store_series_for_ref_disk_cache_misses_total"))
}

func TestSeriesForRefDiskCache_WarmRestart(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, seriesForRefDiskCacheFilename)
	reg1 := prometheus.NewPedanticRegistry()

	// Phase 1: open empty, write a handful, close.
	c1, err := openSeriesForRefDiskCache(path, 1<<20, newSeriesForRefDiskCacheMetrics(reg1), log.NewNopLogger())
	require.NoError(t, err)
	for i := 0; i < 10; i++ {
		require.NoError(t, c1.Set(storage.SeriesRef(i), []byte{byte(i), 'x', 'y'}))
	}
	require.NoError(t, c1.Close())

	// Phase 2: re-open with a fresh metrics registry, every entry must be readable
	// from the rebuilt index.
	reg2 := prometheus.NewPedanticRegistry()
	c2, err := openSeriesForRefDiskCache(path, 1<<20, newSeriesForRefDiskCacheMetrics(reg2), log.NewNopLogger())
	require.NoError(t, err)
	defer c2.Close()
	for i := 0; i < 10; i++ {
		got, ok := c2.Get(storage.SeriesRef(i), nil)
		require.True(t, ok, "ref %d missing after warm restart", i)
		require.Equal(t, []byte{byte(i), 'x', 'y'}, got)
	}
	assert.Equal(t, 10.0, counterValue(t, reg2, "cortex_bucket_store_series_for_ref_disk_cache_hits_total"))
	// A clean reopen must not increment the corruption counter.
	assert.Equal(t, 0.0, counterValue(t, reg2, "cortex_bucket_store_series_for_ref_disk_cache_corruption_total"))
}

func TestSeriesForRefDiskCache_TruncateOnPartialHeader(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, seriesForRefDiskCacheFilename)

	// Write one good record, then a partial record header (8 bytes — half of the
	// 16-byte record header), simulating a crash during the next Set.
	c, err := openSeriesForRefDiskCache(path, 1<<20, newSeriesForRefDiskCacheMetrics(prometheus.NewPedanticRegistry()), log.NewNopLogger())
	require.NoError(t, err)
	require.NoError(t, c.Set(1, []byte("ok")))
	require.NoError(t, c.Close())

	f, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0o600)
	require.NoError(t, err)
	_, err = f.Write([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff})
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// Re-open: replay must truncate the partial header but keep the prior valid record.
	c2, err := openSeriesForRefDiskCache(path, 1<<20, newSeriesForRefDiskCacheMetrics(prometheus.NewPedanticRegistry()), log.NewNopLogger())
	require.NoError(t, err)
	defer c2.Close()

	got, ok := c2.Get(1, nil)
	require.True(t, ok)
	require.Equal(t, []byte("ok"), got)
	require.Len(t, c2.index, 1)

	// File is truncated back to the boundary of the valid record:
	// file header (8) + record header (16) + body ("ok" = 2) = 26.
	expectedSize := int64(diskCacheFileHeaderSize + diskCacheRecordHeaderSize + 2)
	stat, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, expectedSize, stat.Size())
}

func TestSeriesForRefDiskCache_TruncateOnPartialData(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, seriesForRefDiskCacheFilename)

	// Write one good record, then a header that claims 100 bytes of data but only
	// 5 bytes of (garbage) data — simulating a crash mid-write. The CRC field is
	// unread because the dataOff+length>size check fires first; we still write a
	// plausible value so we exercise the same code path Set produces.
	c, err := openSeriesForRefDiskCache(path, 1<<20, newSeriesForRefDiskCacheMetrics(prometheus.NewPedanticRegistry()), log.NewNopLogger())
	require.NoError(t, err)
	require.NoError(t, c.Set(1, []byte("ok")))
	require.NoError(t, c.Close())

	f, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0o600)
	require.NoError(t, err)
	var hdr [diskCacheRecordHeaderSize]byte
	binary.LittleEndian.PutUint32(hdr[0:4], 0xdeadbeef) // CRC, never validated on this path
	binary.LittleEndian.PutUint64(hdr[4:12], 9999)
	binary.LittleEndian.PutUint32(hdr[12:16], 100) // claim 100 bytes
	_, err = f.Write(hdr[:])
	require.NoError(t, err)
	_, err = f.Write([]byte{0xaa, 0xbb, 0xcc, 0xdd, 0xee}) // only 5 bytes
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// Re-open: replay must truncate the malformed record and keep ref 1.
	c2, err := openSeriesForRefDiskCache(path, 1<<20, newSeriesForRefDiskCacheMetrics(prometheus.NewPedanticRegistry()), log.NewNopLogger())
	require.NoError(t, err)
	defer c2.Close()

	got, ok := c2.Get(1, nil)
	require.True(t, ok)
	require.Equal(t, []byte("ok"), got)

	// Key 9999 must not be indexed.
	_, ok = c2.Get(9999, nil)
	require.False(t, ok)

	expectedSize := int64(diskCacheFileHeaderSize + diskCacheRecordHeaderSize + 2)
	stat, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, expectedSize, stat.Size())
}

func TestSeriesForRefDiskCache_TruncateOnCRCMismatch(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, seriesForRefDiskCacheFilename)

	// Write two valid records.
	c, err := openSeriesForRefDiskCache(path, 1<<20, newSeriesForRefDiskCacheMetrics(prometheus.NewPedanticRegistry()), log.NewNopLogger())
	require.NoError(t, err)
	require.NoError(t, c.Set(1, []byte("first")))
	require.NoError(t, c.Set(2, []byte("second-payload")))
	require.NoError(t, c.Close())

	// Layout:
	//   [0..8)        file header
	//   [8..29)       record 1: 16 hdr + 5 body ("first") = 21 bytes
	//   [29..59)      record 2: 16 hdr + 14 body ("second-payload") = 30 bytes
	// Flip a body byte of record 2 (offset = 8 + 21 + 16 = 45, the first body byte).
	corruptOffset := int64(diskCacheFileHeaderSize + diskCacheRecordHeaderSize + len("first") + diskCacheRecordHeaderSize)

	f, err := os.OpenFile(path, os.O_RDWR, 0o600)
	require.NoError(t, err)
	var b [1]byte
	_, err = f.ReadAt(b[:], corruptOffset)
	require.NoError(t, err)
	b[0] ^= 0xff
	_, err = f.WriteAt(b[:], corruptOffset)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// Re-open: replay must drop the corrupted 2nd record but keep the 1st.
	reg := prometheus.NewPedanticRegistry()
	c2, err := openSeriesForRefDiskCache(path, 1<<20, newSeriesForRefDiskCacheMetrics(reg), log.NewNopLogger())
	require.NoError(t, err)
	defer c2.Close()

	got, ok := c2.Get(1, nil)
	require.True(t, ok)
	require.Equal(t, []byte("first"), got)

	_, ok = c2.Get(2, nil)
	require.False(t, ok, "CRC-failed record must not be indexed")

	require.Len(t, c2.index, 1)
	assert.Equal(t, 1.0, counterValue(t, reg, "cortex_bucket_store_series_for_ref_disk_cache_corruption_total"))

	// File is truncated to the boundary of record 1 (i.e. just before record 2):
	// file header (8) + record 1 (16+5) = 29 bytes.
	expectedSize := int64(diskCacheFileHeaderSize + diskCacheRecordHeaderSize + len("first"))
	stat, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, expectedSize, stat.Size())
}

func TestSeriesForRefDiskCache_RejectsUnknownFileHeader(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, seriesForRefDiskCacheFilename)

	// Populate, close, then corrupt the magic bytes to simulate (a) a different
	// version, (b) a pre-versioned binary's file, or (c) an entirely unrelated file
	// at the same path.
	c, err := openSeriesForRefDiskCache(path, 1<<20, newSeriesForRefDiskCacheMetrics(prometheus.NewPedanticRegistry()), log.NewNopLogger())
	require.NoError(t, err)
	require.NoError(t, c.Set(1, []byte("payload-that-must-be-discarded")))
	require.NoError(t, c.Close())

	f, err := os.OpenFile(path, os.O_RDWR, 0o600)
	require.NoError(t, err)
	_, err = f.WriteAt([]byte("xxxx"), 0)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	reg := prometheus.NewPedanticRegistry()
	c2, err := openSeriesForRefDiskCache(path, 1<<20, newSeriesForRefDiskCacheMetrics(reg), log.NewNopLogger())
	require.NoError(t, err)
	defer c2.Close()

	_, ok := c2.Get(1, nil)
	require.False(t, ok, "after magic mismatch, all entries must be discarded")
	require.Empty(t, c2.index)
	require.Equal(t, int64(diskCacheFileHeaderSize), c2.writeOff)
	assert.Equal(t, 1.0, counterValue(t, reg, "cortex_bucket_store_series_for_ref_disk_cache_corruption_total"))

	// File has been reset to just the fresh header.
	stat, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, int64(diskCacheFileHeaderSize), stat.Size())

	// Subsequent writes succeed against the freshly-reset file.
	require.NoError(t, c2.Set(42, []byte("post-reset")))
	got, ok := c2.Get(42, nil)
	require.True(t, ok)
	require.Equal(t, []byte("post-reset"), got)
}

func TestSeriesForRefDiskCache_RejectsUnknownVersion(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, seriesForRefDiskCacheFilename)

	c, err := openSeriesForRefDiskCache(path, 1<<20, newSeriesForRefDiskCacheMetrics(prometheus.NewPedanticRegistry()), log.NewNopLogger())
	require.NoError(t, err)
	require.NoError(t, c.Set(1, []byte("v1-payload")))
	require.NoError(t, c.Close())

	// Stomp the version field with a value that the current binary does not know
	// about (simulating a downgrade after a forward-incompatible format bump).
	f, err := os.OpenFile(path, os.O_RDWR, 0o600)
	require.NoError(t, err)
	var future [4]byte
	binary.LittleEndian.PutUint32(future[:], 0xffffffff)
	_, err = f.WriteAt(future[:], int64(diskCacheMagicLen))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	reg := prometheus.NewPedanticRegistry()
	c2, err := openSeriesForRefDiskCache(path, 1<<20, newSeriesForRefDiskCacheMetrics(reg), log.NewNopLogger())
	require.NoError(t, err)
	defer c2.Close()

	require.Empty(t, c2.index)
	assert.Equal(t, 1.0, counterValue(t, reg, "cortex_bucket_store_series_for_ref_disk_cache_corruption_total"))
}

func TestSeriesForRefDiskCache_TornFileHeaderIsDiscarded(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, seriesForRefDiskCacheFilename)

	// Hand-craft a file that contains only half of the file header (4 bytes), as if
	// a crash happened during the very first Set on a brand-new cache.
	require.NoError(t, os.WriteFile(path, []byte{'s', 'g', 's', 'c'}, 0o600))

	reg := prometheus.NewPedanticRegistry()
	c, err := openSeriesForRefDiskCache(path, 1<<20, newSeriesForRefDiskCacheMetrics(reg), log.NewNopLogger())
	require.NoError(t, err)
	defer c.Close()

	require.Empty(t, c.index)
	require.Equal(t, int64(diskCacheFileHeaderSize), c.writeOff)
	assert.Equal(t, 1.0, counterValue(t, reg, "cortex_bucket_store_series_for_ref_disk_cache_corruption_total"))
}

// crc32IEEE recomputes the IEEE CRC32 of (ref || length || data) — the same
// formulation Set uses. Exposed here so a test that hand-builds a file ends up with
// records the production replay path treats as valid.
func crc32IEEE(ref storage.SeriesRef, value []byte) uint32 {
	var rl [12]byte
	binary.LittleEndian.PutUint64(rl[0:8], uint64(ref))
	binary.LittleEndian.PutUint32(rl[8:12], uint32(len(value)))
	h := crc32.NewIEEE()
	_, _ = h.Write(rl[:])
	_, _ = h.Write(value)
	return h.Sum32()
}

func TestSeriesForRefDiskCache_HandBuiltFileReplaysAsHits(t *testing.T) {
	// Hand-build a file (header + one record) and verify that openSeriesForRefDiskCache
	// indexes the record. This locks down the on-disk format: any future change to
	// record layout will fail this test in addition to TruncateOnCRCMismatch.
	dir := t.TempDir()
	path := filepath.Join(dir, seriesForRefDiskCacheFilename)

	body := []byte("frozen-format-payload")
	var fhdr [diskCacheFileHeaderSize]byte
	copy(fhdr[0:diskCacheMagicLen], diskCacheMagic[:])
	binary.LittleEndian.PutUint32(fhdr[diskCacheMagicLen:], diskCacheVersion)

	var rhdr [diskCacheRecordHeaderSize]byte
	binary.LittleEndian.PutUint32(rhdr[0:4], crc32IEEE(7, body))
	binary.LittleEndian.PutUint64(rhdr[4:12], 7)
	binary.LittleEndian.PutUint32(rhdr[12:16], uint32(len(body)))

	contents := append(append(fhdr[:], rhdr[:]...), body...)
	require.NoError(t, os.WriteFile(path, contents, 0o600))

	c, err := openSeriesForRefDiskCache(path, 1<<20, newSeriesForRefDiskCacheMetrics(prometheus.NewPedanticRegistry()), log.NewNopLogger())
	require.NoError(t, err)
	defer c.Close()

	got, ok := c.Get(7, nil)
	require.True(t, ok)
	require.Equal(t, body, got)
}

func TestSeriesForRefDiskCache_FullAtBudgetBoundary(t *testing.T) {
	// Budget set so exactly two records fit, accounting for the file header.
	const valueSize = 8
	budget := int64(diskCacheFileHeaderSize + 2*(diskCacheRecordHeaderSize+valueSize))

	c, _, reg := newDiskCacheForTest(t, budget)

	val := make([]byte, valueSize)
	require.NoError(t, c.Set(1, val))
	require.NoError(t, c.Set(2, val))
	err := c.Set(3, val)
	require.ErrorIs(t, err, errDiskCacheFull)

	assert.Equal(t, 1.0, counterValue(t, reg, "cortex_bucket_store_series_for_ref_disk_cache_full_total"))

	// The two committed entries are still readable.
	for _, ref := range []storage.SeriesRef{1, 2} {
		got, ok := c.Get(ref, nil)
		require.True(t, ok)
		require.Len(t, got, valueSize)
	}
}

func TestSeriesForRefDiskCache_GetMissIncrementsMisses(t *testing.T) {
	c, _, reg := newDiskCacheForTest(t, 1<<20)

	_, ok := c.Get(99, nil)
	require.False(t, ok)
	assert.Equal(t, 1.0, counterValue(t, reg, "cortex_bucket_store_series_for_ref_disk_cache_misses_total"))
}

func TestSeriesForRefDiskCache_DuplicateSetIsNoop(t *testing.T) {
	c, _, reg := newDiskCacheForTest(t, 1<<20)

	require.NoError(t, c.Set(1, []byte("first")))
	startStores := counterValue(t, reg, "cortex_bucket_store_series_for_ref_disk_cache_stores_total")
	require.NoError(t, c.Set(1, []byte("second-ignored")))

	got, ok := c.Get(1, nil)
	require.True(t, ok)
	require.Equal(t, []byte("first"), got, "duplicate Set must not overwrite the existing record")

	assert.Equal(t, startStores, counterValue(t, reg, "cortex_bucket_store_series_for_ref_disk_cache_stores_total"))
}
