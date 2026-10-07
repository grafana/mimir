// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
	"unicode/utf8"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/seriesstore/chunks"
	"github.com/grafana/mimir/pkg/storage/seriesstore/exemplars"
	"github.com/grafana/mimir/pkg/storage/seriesstore/labels"
)

// Head snapshot magics, one per layout version.
var (
	snapshotMagic = []byte("MIMIRHS6")
	// Without cold blocks.
	snapshotV5Magic = []byte("MIMIRHS5")
	// Without the emulated Go head's truncations and non-owned evictions.
	snapshotV4Magic = []byte("MIMIRHS4")
	// Without chunk out-of-order flags, the native histogram flag, or histograms in the open
	// out-of-order chunk.
	snapshotV3Magic = []byte("MIMIRHS3")
	// Also without the head max time and metadata timestamps, and with exemplars kept per series.
	snapshotLegacyMagic = []byte("MIMIRHS2")
)

const snapshotFileName = "snapshot"

// SnapshotOffset is the Kafka position a head snapshot covers for one cluster.
type SnapshotOffset struct {
	Offset      int64
	HasOffset   bool
	TimestampMs int64
}

// Restored is a store restored from a head snapshot, with the positions it covers.
type Restored struct {
	Store   *Store
	Offsets []SnapshotOffset
}

type snapshotWriter struct {
	w       *bufio.Writer
	hasher  hash.Hash32
	scratch [8]byte
}

func (w *snapshotWriter) write(bytes []byte) error {
	w.hasher.Write(bytes)
	_, err := w.w.Write(bytes)
	return err
}

func (w *snapshotWriter) u8(value uint8) error {
	w.scratch[0] = value
	return w.write(w.scratch[:1])
}

func (w *snapshotWriter) u32(value uint32) error {
	binary.LittleEndian.PutUint32(w.scratch[:4], value)
	return w.write(w.scratch[:4])
}

func (w *snapshotWriter) u64(value uint64) error {
	binary.LittleEndian.PutUint64(w.scratch[:8], value)
	return w.write(w.scratch[:8])
}

func (w *snapshotWriter) i64(value int64) error {
	return w.u64(uint64(value))
}

func (w *snapshotWriter) length(n int) error {
	if n > math.MaxUint32 {
		return errors.New("head snapshot collection exceeds u32")
	}
	return w.u32(uint32(n))
}

func (w *snapshotWriter) bytes(bytes []byte) error {
	if err := w.length(len(bytes)); err != nil {
		return err
	}
	return w.write(bytes)
}

func (w *snapshotWriter) str(s string) error {
	return w.bytes([]byte(s))
}

func (w *snapshotWriter) labels(stored labels.Labels) error {
	if err := w.length(stored.Len()); err != nil {
		return err
	}
	var err error
	stored.Range(func(name, value string) {
		if err == nil {
			err = w.str(name)
		}
		if err == nil {
			err = w.str(value)
		}
	})
	return err
}

// hashedWriter feeds the XOR appender's state through the snapshot checksum.
type hashedWriter struct{ w *snapshotWriter }

func (h hashedWriter) Write(bytes []byte) (int, error) {
	if err := h.w.write(bytes); err != nil {
		return 0, err
	}
	return len(bytes), nil
}

// WriteSnapshot persists every series' open chunks and chunk references next to the chunk files,
// like Prometheus's memory snapshot on shutdown, so the next start skips segment replay.
func (s *Store) WriteSnapshot(offsets []SnapshotOffset) error {
	for _, shard := range s.shards {
		shard.RLock()
	}
	defer func() {
		for _, shard := range s.shards {
			shard.RUnlock()
		}
	}()
	shardDirectory := s.shards[0].disk.Directory()
	if shardDirectory == "" {
		return errors.New("head snapshots need a chunk directory")
	}
	directory := filepath.Dir(shardDirectory)
	for _, shard := range s.shards {
		if err := shard.disk.Sync(); err != nil {
			return err
		}
	}
	temporary := filepath.Join(directory, snapshotFileName+".tmp")
	file, err := os.Create(temporary)
	if err != nil {
		return fmt.Errorf("create head snapshot %s: %w", temporary, err)
	}
	w := &snapshotWriter{w: bufio.NewWriterSize(file, 1<<20), hasher: crc32.NewIEEE()}
	if _, err := w.w.Write(snapshotMagic); err != nil {
		_ = file.Close()
		return err
	}
	err = s.writeSnapshotBody(w, offsets)
	if err == nil {
		s.exemplarsLock.Lock()
		err = writeExemplars(w, s.exemplars)
		s.exemplarsLock.Unlock()
	}
	if err == nil {
		binary.LittleEndian.PutUint32(w.scratch[:4], w.hasher.Sum32())
		_, err = w.w.Write(w.scratch[:4])
	}
	if err == nil {
		err = w.w.Flush()
	}
	if err == nil {
		err = file.Sync()
	}
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return fmt.Errorf("write head snapshot: %w", err)
	}
	if err := os.Rename(temporary, filepath.Join(directory, snapshotFileName)); err != nil {
		return err
	}
	return syncDirectory(directory)
}

func syncDirectory(directory string) error {
	dir, err := os.Open(directory)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func (s *Store) writeSnapshotBody(w *snapshotWriter, offsets []SnapshotOffset) error {
	if err := w.length(len(offsets)); err != nil {
		return err
	}
	for _, offset := range offsets {
		value := int64(math.MinInt64)
		if offset.HasOffset {
			value = offset.Offset
		}
		if err := w.i64(value); err != nil {
			return err
		}
		if err := w.i64(offset.TimestampMs); err != nil {
			return err
		}
	}
	if err := w.length(len(s.shards)); err != nil {
		return err
	}
	for _, shard := range s.shards {
		if err := writeShard(w, shard); err != nil {
			return err
		}
		// Cold blocks are already on disk; the snapshot names the ones the store keeps.
		if err := w.u64(shard.cold.nextID); err != nil {
			return err
		}
		if err := w.length(len(shard.cold.blocks)); err != nil {
			return err
		}
		for _, block := range shard.cold.blocks {
			if err := w.u64(block.id); err != nil {
				return err
			}
		}
	}
	return nil
}

func writeShard(w *snapshotWriter, shard *shardState) error {
	files, nextSequence := shard.disk.State()
	if err := w.u32(nextSequence); err != nil {
		return err
	}
	if err := w.length(len(files)); err != nil {
		return err
	}
	for _, file := range files {
		if err := errors.Join(w.u32(file.Sequence), w.u64(file.Written), w.i64(file.MaxTime)); err != nil {
			return err
		}
	}
	if err := w.length(len(shard.tenants)); err != nil {
		return err
	}
	for tenantID, t := range shard.tenants {
		if err := w.str(tenantID); err != nil {
			return err
		}
		// A restarted Go head resumes from its WAL with its min time and evictions.
		if err := errors.Join(w.i64(t.maxTime), w.i64(t.headMin), w.i64(t.truncatedTo), w.i64(t.minTime)); err != nil {
			return err
		}
		total := 0
		for _, set := range t.metadata {
			total += len(set)
		}
		if err := w.length(total); err != nil {
			return err
		}
		for _, name := range sortedFamilies(t) {
			set := t.metadata[name]
			keys := make([]metadataKey, 0, len(set))
			for key := range set {
				keys = append(keys, key)
			}
			slices.SortFunc(keys, compareMetadataKeys)
			for _, key := range keys {
				entry := set[key]
				encoded, err := entry.metadata.Marshal()
				if err != nil {
					return err
				}
				if err := errors.Join(w.bytes(encoded), w.i64(entry.seenMs)); err != nil {
					return err
				}
			}
		}
		if err := w.length(t.series.len); err != nil {
			return err
		}
		var err error
		t.series.forEach(func(entry *seriesEntry) {
			if err == nil {
				err = writeSeries(w, entry)
			}
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func sortedFamilies(t *tenant) []string {
	names := make([]string, 0, len(t.metadata))
	for name := range t.metadata {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

func boolByte(value bool) uint8 {
	if value {
		return 1
	}
	return 0
}

func writeSeries(w *snapshotWriter, entry *seriesEntry) error {
	series := &entry.series
	if err := w.labels(entry.labels); err != nil {
		return err
	}
	if err := errors.Join(w.i64(series.lastIngestedMs), w.u32(series.lastBucketCount),
		w.u8(boolByte(series.nativeHistogram)), w.u8(boolByte(series.headEvicted)), w.u32(series.nonOwnedSinceS)); err != nil {
		return err
	}
	metas := series.chunks.toSlice()
	if err := w.length(len(metas)); err != nil {
		return err
	}
	for _, chunk := range metas {
		if err := errors.Join(w.u64(chunk.Ref), w.i64(chunk.MinTime), w.i64(chunk.MaxTime), w.u32(chunk.Len),
			w.u8(chunk.Encoding), w.u8(boolByte(chunk.OutOfOrder))); err != nil {
			return err
		}
	}
	if fh := series.floatHead; fh != nil {
		if err := errors.Join(w.u8(1), w.i64(fh.minTime), w.i64(fh.nextAt)); err != nil {
			return err
		}
		if err := fh.appender.WriteState(hashedWriter{w}); err != nil {
			return err
		}
	} else if err := w.u8(0); err != nil {
		return err
	}
	histogramHead, err := series.histogramSamples()
	if err != nil {
		return err
	}
	if err := w.length(len(histogramHead)); err != nil {
		return err
	}
	for index := range histogramHead {
		encoded, err := histogramHead[index].Marshal()
		if err != nil {
			return err
		}
		if err := w.bytes(encoded); err != nil {
			return err
		}
	}
	if err := w.i64(series.histogramNextAtValue()); err != nil {
		return err
	}
	if err := w.length(len(series.ooo())); err != nil {
		return err
	}
	for _, sample := range series.ooo() {
		if err := w.i64(sample.T); err != nil {
			return err
		}
		if sample.H == nil {
			if err := errors.Join(w.u8(0), w.u64(math.Float64bits(sample.F))); err != nil {
				return err
			}
			continue
		}
		encoded, err := sample.H.Marshal()
		if err != nil {
			return err
		}
		if err := errors.Join(w.u8(1), w.bytes(encoded)); err != nil {
			return err
		}
	}
	return nil
}

// writeExemplars writes exemplars in insertion order, so a restore evicts in the same order.
func writeExemplars(w *snapshotWriter, all map[string]*exemplars.TenantExemplars[labels.Labels]) error {
	if err := w.length(len(all)); err != nil {
		return err
	}
	for tenantID, storage := range all {
		if err := errors.Join(w.str(tenantID), w.u64(uint64(storage.Capacity()))); err != nil {
			return err
		}
		entries := storage.InInsertionOrder()
		if err := w.length(len(entries)); err != nil {
			return err
		}
		for _, entry := range entries {
			if err := errors.Join(w.u64(entry.SeriesID), w.labels(entry.Labels)); err != nil {
				return err
			}
			exemplar := fromExemplar(&entry.Exemplar)
			encoded, err := exemplar.Marshal()
			if err != nil {
				return err
			}
			if err := w.bytes(encoded); err != nil {
				return err
			}
		}
	}
	return nil
}

type snapshotReader struct {
	r       *bufio.Reader
	hasher  hash.Hash32
	scratch [8]byte
}

func (r *snapshotReader) Read(bytes []byte) (int, error) {
	n, err := r.r.Read(bytes)
	r.hasher.Write(bytes[:n])
	return n, err
}

func (r *snapshotReader) array(n int) ([]byte, error) {
	if _, err := io.ReadFull(r, r.scratch[:n]); err != nil {
		return nil, err
	}
	return r.scratch[:n], nil
}

func (r *snapshotReader) u8() (uint8, error) {
	b, err := r.array(1)
	if err != nil {
		return 0, err
	}
	return b[0], nil
}

func (r *snapshotReader) u32() (uint32, error) {
	b, err := r.array(4)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(b), nil
}

func (r *snapshotReader) u64() (uint64, error) {
	b, err := r.array(8)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(b), nil
}

func (r *snapshotReader) i64() (int64, error) {
	value, err := r.u64()
	return int64(value), err
}

func (r *snapshotReader) count(maximum uint32) (int, error) {
	count, err := r.u32()
	if err != nil {
		return 0, err
	}
	if count > maximum {
		return 0, fmt.Errorf("head snapshot collection exceeds %d entries", maximum)
	}
	return int(count), nil
}

func (r *snapshotReader) readBytes() ([]byte, error) {
	n, err := r.count(64 * 1024 * 1024)
	if err != nil {
		return nil, err
	}
	bytes := make([]byte, n)
	if _, err := io.ReadFull(r, bytes); err != nil {
		return nil, err
	}
	return bytes, nil
}

func (r *snapshotReader) str() (string, error) {
	bytes, err := r.readBytes()
	if err != nil {
		return "", err
	}
	if !utf8.Valid(bytes) {
		return "", errors.New("head snapshot string is not UTF-8")
	}
	return string(bytes), nil
}

func (r *snapshotReader) labels() (labels.Labels, error) {
	count, err := r.count(10_000)
	if err != nil {
		return "", err
	}
	pairs := make([][2]string, count)
	for index := range pairs {
		if pairs[index][0], err = r.str(); err != nil {
			return "", err
		}
		if pairs[index][1], err = r.str(); err != nil {
			return "", err
		}
	}
	return labels.FromSorted(pairs), nil
}

// Restore loads and removes the head snapshot in chunkDir. It returns nil when there is none or it
// is unusable; the caller then rebuilds from segments, which clears the directory.
func Restore(activeWindowMs int64, retention Retention, chunkDir string, threads int) (*Restored, error) {
	path := filepath.Join(chunkDir, snapshotFileName)
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer file.Close()
	// Chunk files change as soon as ingestion resumes, so a snapshot is only valid once. The open
	// handle keeps it readable.
	if err := os.Remove(path); err != nil {
		return nil, fmt.Errorf("remove head snapshot %s: %w", path, err)
	}
	if err := syncDirectory(chunkDir); err != nil {
		return nil, err
	}
	restored, err := readSnapshot(file, activeWindowMs, retention, chunkDir, threads)
	if err != nil {
		fmt.Fprintf(os.Stderr, "phase=head_snapshot_invalid error=%v\n", err)
		return nil, nil
	}
	return restored, nil
}

type shardImage struct {
	files        []chunks.FileState
	nextSequence uint32
	tenants      map[string]*tenant
	legacy       []legacyExemplar
	nextColdID   uint64
	coldIDs      []uint64
}

type legacyExemplar struct {
	tenant   string
	seriesID uint64
	labels   labels.Labels
	exemplar mimirpb.Exemplar
}

func readSnapshot(file *os.File, activeWindowMs int64, retention Retention, chunkDir string, threads int) (restored *Restored, err error) {
	// A corrupt snapshot can hold impossible values; it is rejected like a bad checksum.
	defer func() {
		if recovered := recover(); recovered != nil {
			restored, err = nil, fmt.Errorf("corrupt head snapshot: %v", recovered)
		}
	}()
	r := &snapshotReader{r: bufio.NewReaderSize(file, 1<<20), hasher: crc32.NewIEEE()}
	var magic [8]byte
	if _, err := io.ReadFull(r.r, magic[:]); err != nil {
		return nil, err
	}
	var version int
	switch string(magic[:]) {
	case string(snapshotMagic):
		version = 6
	case string(snapshotV5Magic):
		version = 5
	case string(snapshotV4Magic):
		version = 4
	case string(snapshotV3Magic):
		version = 3
	case string(snapshotLegacyMagic):
		version = 2
	default:
		return nil, errors.New("head snapshot has invalid magic")
	}
	legacy := version == 2
	offsetCount, err := r.count(1_000)
	if err != nil {
		return nil, err
	}
	offsets := make([]SnapshotOffset, offsetCount)
	for index := range offsets {
		offset, err := r.i64()
		if err != nil {
			return nil, err
		}
		timestamp, err := r.i64()
		if err != nil {
			return nil, err
		}
		offsets[index] = SnapshotOffset{Offset: offset, HasOffset: offset != math.MinInt64, TimestampMs: timestamp}
		if !offsets[index].HasOffset {
			offsets[index].Offset = 0
		}
	}
	shardCount, err := r.count(4096)
	if err != nil {
		return nil, err
	}
	images := make([]shardImage, shardCount)
	for index := range images {
		image, err := readShard(r, version)
		if err != nil {
			return nil, err
		}
		// The shard's cold blocks: the next id, then the ids of the blocks to keep.
		if version >= 6 {
			if image.nextColdID, err = r.u64(); err != nil {
				return nil, err
			}
			count, err := r.count(1_000_000)
			if err != nil {
				return nil, err
			}
			image.coldIDs = make([]uint64, count)
			for id := range image.coldIDs {
				if image.coldIDs[id], err = r.u64(); err != nil {
					return nil, err
				}
			}
		}
		images[index] = image
	}
	var restoredExemplars map[string]*exemplars.TenantExemplars[labels.Labels]
	if !legacy {
		if restoredExemplars, err = readExemplars(r); err != nil {
			return nil, err
		}
	}
	expected := r.hasher.Sum32()
	var checksum [4]byte
	if _, err := io.ReadFull(r.r, checksum[:]); err != nil {
		return nil, err
	}
	if binary.LittleEndian.Uint32(checksum[:]) != expected {
		return nil, errors.New("head snapshot checksum mismatch")
	}
	if _, err := r.r.ReadByte(); err == nil {
		return nil, errors.New("trailing bytes in head snapshot")
	}
	if legacy {
		restoredExemplars = upgradeLegacy(images)
	}
	shards := make([]*shardState, len(images))
	for index, image := range images {
		disk, err := chunks.ReopenDiskMapper(shardDir(chunkDir, index), image.files, image.nextSequence)
		if err != nil {
			return nil, err
		}
		cold, err := reopenCold(coldDir(chunkDir, index), image.nextColdID, image.coldIDs)
		if err != nil {
			return nil, err
		}
		shards[index] = &shardState{tenants: image.tenants, disk: disk, cold: cold}
	}
	store := fromShards(shards, threads, activeWindowMs, retention)
	store.exemplars = restoredExemplars
	return &Restored{Store: store, Offsets: offsets}, nil
}

// reopenCold opens the cold blocks a snapshot lists and removes anything else, like the chunk
// files.
func reopenCold(directory string, nextID uint64, ids []uint64) (*coldState, error) {
	cold, err := newColdState(directory)
	if err != nil {
		return nil, err
	}
	cold.nextID = nextID
	kept := map[string]bool{}
	for _, id := range ids {
		kept[coldBlockPath(directory, id)] = true
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		path := filepath.Join(directory, entry.Name())
		if !kept[path] {
			if err := os.Remove(path); err != nil {
				return nil, fmt.Errorf("remove stale cold block %s: %w", path, err)
			}
		}
	}
	for _, id := range ids {
		block, err := openColdBlock(coldBlockPath(directory, id), id)
		if err != nil {
			return nil, err
		}
		cold.blocks = append(cold.blocks, block)
	}
	return cold, nil
}

func readExemplars(r *snapshotReader) (map[string]*exemplars.TenantExemplars[labels.Labels], error) {
	tenants, err := r.count(1_000_000)
	if err != nil {
		return nil, err
	}
	all := make(map[string]*exemplars.TenantExemplars[labels.Labels], tenants)
	for range tenants {
		tenantID, err := r.str()
		if err != nil {
			return nil, err
		}
		capacity, err := r.u64()
		if err != nil {
			return nil, err
		}
		storage := exemplars.New[labels.Labels](int(capacity))
		labelsBySeries := map[uint64]labels.Labels{}
		count, err := r.count(100_000_000)
		if err != nil {
			return nil, err
		}
		for range count {
			seriesID, err := r.u64()
			if err != nil {
				return nil, err
			}
			stored, err := r.labels()
			if err != nil {
				return nil, err
			}
			if existing, ok := labelsBySeries[seriesID]; ok {
				stored = existing
			} else {
				labelsBySeries[seriesID] = stored
			}
			encoded, err := r.readBytes()
			if err != nil {
				return nil, err
			}
			var exemplar mimirpb.Exemplar
			if err := exemplar.Unmarshal(encoded); err != nil {
				return nil, err
			}
			// Stored exemplars were valid when added; a window this wide re-adds them all.
			storage.Add(seriesID, func() labels.Labels { return stored }, toExemplar(&exemplar), math.MaxInt64)
		}
		all[tenantID] = storage
	}
	return all, nil
}

// upgradeLegacy derives what legacy snapshots lack: each tenant's head max time from its series,
// and tenant exemplar storage from the per-series exemplars in timestamp order.
func upgradeLegacy(images []shardImage) map[string]*exemplars.TenantExemplars[labels.Labels] {
	maxTimes := map[string]int64{}
	var all []legacyExemplar
	for index := range images {
		for tenantID, t := range images[index].tenants {
			newest := int64(math.MinInt64)
			t.series.forEach(func(entry *seriesEntry) {
				if last, ok := entry.series.maxTime(); ok {
					newest = max(newest, last)
				}
			})
			if current, ok := maxTimes[tenantID]; !ok || newest > current {
				maxTimes[tenantID] = newest
			}
		}
		all = append(all, images[index].legacy...)
	}
	if len(images) > 0 {
		for tenantID, maxTime := range maxTimes {
			tenantOf(images[0].tenants, tenantID).maxTime = maxTime
		}
	}
	slices.SortStableFunc(all, func(a, b legacyExemplar) int {
		switch {
		case a.exemplar.TimestampMs < b.exemplar.TimestampMs:
			return -1
		case a.exemplar.TimestampMs > b.exemplar.TimestampMs:
			return 1
		}
		return 0
	})
	storage := map[string]*exemplars.TenantExemplars[labels.Labels]{}
	for _, e := range all {
		tenantStorage, ok := storage[e.tenant]
		if !ok {
			// Sized to fit; the first ingest resizes to the tenant's limit.
			tenantStorage = exemplars.New[labels.Labels](math.MaxInt)
			storage[e.tenant] = tenantStorage
		}
		tenantStorage.Add(e.seriesID, func() labels.Labels { return e.labels }, toExemplar(&e.exemplar), math.MaxInt64)
	}
	return storage
}

// inferPreV4Flags recovers what older snapshots did not record: which chunks hold out-of-order
// samples, since a chunk that starts at or before the in-order data written before it, or ends in
// an open chunk's range, can only be one, as in-order chunks are only cut before the open ones.
// The native histogram flag follows the newest open chunk.
func inferPreV4Flags(series *Series) {
	openMin := int64(math.MaxInt64)
	if series.floatHead != nil {
		openMin = series.floatHead.minTime
	}
	if series.histogram() != nil {
		openMin = min(openMin, series.histogram().FirstTimestamp())
	}
	inOrderMax := int64(math.MinInt64)
	metas := series.chunks.toSlice()
	for index := range metas {
		chunk := &metas[index]
		chunk.OutOfOrder = chunk.MinTime <= inOrderMax || chunk.MaxTime >= openMin
		if !chunk.OutOfOrder {
			inOrderMax = chunk.MaxTime
		}
	}
	series.setChunks(chunkListFromMetas(metas))
	var (
		float    int64
		hasFloat bool
	)
	if series.floatHead != nil {
		float, hasFloat = series.floatHead.lastTimestamp(), true
	}
	series.nativeHistogram = series.histogram() != nil && (!hasFloat || series.histogram().Last().Timestamp > float)
}

func readShard(r *snapshotReader, version int) (shardImage, error) {
	legacy := version == 2
	var image shardImage
	var err error
	if image.nextSequence, err = r.u32(); err != nil {
		return image, err
	}
	fileCount, err := r.count(1_000_000)
	if err != nil {
		return image, err
	}
	image.files = make([]chunks.FileState, fileCount)
	for index := range image.files {
		file := &image.files[index]
		if file.Sequence, err = r.u32(); err != nil {
			return image, err
		}
		if file.Written, err = r.u64(); err != nil {
			return image, err
		}
		if file.MaxTime, err = r.i64(); err != nil {
			return image, err
		}
	}
	image.tenants = map[string]*tenant{}
	tenantCount, err := r.count(1_000_000)
	if err != nil {
		return image, err
	}
	for range tenantCount {
		tenantID, err := r.str()
		if err != nil {
			return image, err
		}
		t := newTenant()
		if !legacy {
			if t.maxTime, err = r.i64(); err != nil {
				return image, err
			}
		}
		if version >= 5 {
			if t.headMin, err = r.i64(); err != nil {
				return image, err
			}
			if t.truncatedTo, err = r.i64(); err != nil {
				return image, err
			}
		}
		if version >= 6 {
			if t.minTime, err = r.i64(); err != nil {
				return image, err
			}
		}
		metadataCount, err := r.count(10_000_000)
		if err != nil {
			return image, err
		}
		for range metadataCount {
			encoded, err := r.readBytes()
			if err != nil {
				return image, err
			}
			var metadata mimirpb.MetricMetadata
			if err := metadata.Unmarshal(encoded); err != nil {
				return image, err
			}
			seen := nowMs()
			if !legacy {
				if seen, err = r.i64(); err != nil {
					return image, err
				}
			}
			set, ok := t.metadata[metadata.MetricFamilyName]
			if !ok {
				set = map[metadataKey]metadataEntry{}
				t.metadata[metadata.MetricFamilyName] = set
			}
			set[metadataKey{int32(metadata.Type), metadata.Help, metadata.Unit}] = metadataEntry{metadata, seen}
		}
		seriesCount, err := r.count(100_000_000)
		if err != nil {
			return image, err
		}
		for range seriesCount {
			stored, err := r.labels()
			if err != nil {
				return image, err
			}
			series := Series{}
			if series.lastIngestedMs, err = r.i64(); err != nil {
				return image, err
			}
			if series.lastBucketCount, err = r.u32(); err != nil {
				return image, err
			}
			if version >= 4 {
				flag, err := r.u8()
				if err != nil {
					return image, err
				}
				series.nativeHistogram = flag == 1
			}
			if version >= 5 {
				flag, err := r.u8()
				if err != nil {
					return image, err
				}
				series.headEvicted = flag == 1
				if series.nonOwnedSinceS, err = r.u32(); err != nil {
					return image, err
				}
			}
			chunkCount, err := r.count(1_000_000)
			if err != nil {
				return image, err
			}
			metas := make([]ChunkMeta, chunkCount)
			for index := range metas {
				meta := &metas[index]
				if meta.Ref, err = r.u64(); err != nil {
					return image, err
				}
				if meta.MinTime, err = r.i64(); err != nil {
					return image, err
				}
				if meta.MaxTime, err = r.i64(); err != nil {
					return image, err
				}
				if meta.Len, err = r.u32(); err != nil {
					return image, err
				}
				if meta.Encoding, err = r.u8(); err != nil {
					return image, err
				}
				if version >= 4 {
					flag, err := r.u8()
					if err != nil {
						return image, err
					}
					meta.OutOfOrder = flag == 1
				}
			}
			series.setChunks(chunkListFromMetas(metas))
			hasFloat, err := r.u8()
			if err != nil {
				return image, err
			}
			if hasFloat == 1 {
				fh := &floatHead{}
				if fh.minTime, err = r.i64(); err != nil {
					return image, err
				}
				if fh.nextAt, err = r.i64(); err != nil {
					return image, err
				}
				if fh.appender, err = chunks.ReadXORAppenderState(r); err != nil {
					return image, err
				}
				series.floatHead = fh
			}
			histogramCount, err := r.count(1_000_000)
			if err != nil {
				return image, err
			}
			if histogramCount > 0 {
				samples := make([]mimirpb.Histogram, histogramCount)
				for index := range samples {
					encoded, err := r.readBytes()
					if err != nil {
						return image, err
					}
					if err := samples[index].Unmarshal(encoded); err != nil {
						return image, err
					}
				}
				head, err := chunks.NewHistogramAppenderFromSamples(samples)
				if err != nil {
					return image, err
				}
				series.setHistogram(head)
			}
			nextAt, err := r.i64()
			if err != nil {
				return image, err
			}
			series.setHistogramNextAt(nextAt)
			oooCount, err := r.count(1_000_000)
			if err != nil {
				return image, err
			}
			if oooCount > 0 {
				series.setOutOfOrder(make([]oooSample, oooCount))
			}
			for index := range series.ooo() {
				sample := &series.ooo()[index]
				if sample.T, err = r.i64(); err != nil {
					return image, err
				}
				kind := uint8(0)
				if version >= 4 {
					if kind, err = r.u8(); err != nil {
						return image, err
					}
				}
				if kind == 0 {
					bits, err := r.u64()
					if err != nil {
						return image, err
					}
					sample.F = math.Float64frombits(bits)
					continue
				}
				encoded, err := r.readBytes()
				if err != nil {
					return image, err
				}
				h := &mimirpb.Histogram{}
				if err := h.Unmarshal(encoded); err != nil {
					return image, err
				}
				sample.H = h
			}
			if version < 4 {
				inferPreV4Flags(&series)
			}
			series.ownedHash = ShardByAllLabels(tenantID, stored)
			series.shardHash = labels.StableHash(stored)
			seriesHash := stored.Hash()
			if legacy {
				exemplarCount, err := r.count(10_000_000)
				if err != nil {
					return image, err
				}
				for range exemplarCount {
					encoded, err := r.readBytes()
					if err != nil {
						return image, err
					}
					var exemplar mimirpb.Exemplar
					if err := exemplar.Unmarshal(encoded); err != nil {
						return image, err
					}
					image.legacy = append(image.legacy, legacyExemplar{tenantID, seriesHash, stored, exemplar})
				}
			}
			if !t.series.insert(seriesHash, stored, series) {
				return image, errors.New("duplicate series in head snapshot")
			}
		}
		if _, exists := image.tenants[tenantID]; exists {
			return image, errors.New("duplicate tenant in head snapshot")
		}
		image.tenants[tenantID] = t
	}
	return image, nil
}
