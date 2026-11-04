// SPDX-License-Identifier: AGPL-3.0-only
package store

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"slices"
	"sort"
	"testing"

	"github.com/grafana/mimir/pkg/storage/seriesstore/labels"
)

type benchColdWriter struct {
	file    *os.File
	writer  *bufio.Writer
	offset  int
	err     error
	scratch [10]byte
}

func (w *benchColdWriter) bytes(p []byte) {
	if w.err != nil {
		return
	}
	n, err := w.writer.Write(p)
	w.offset += n
	w.err = err
}
func (w *benchColdWriter) text(p string) {
	if w.err != nil {
		return
	}
	n, err := w.writer.WriteString(p)
	w.offset += n
	w.err = err
}
func (w *benchColdWriter) varint(v uint64) {
	n := binary.PutUvarint(w.scratch[:], v)
	w.bytes(w.scratch[:n])
}
func (w *benchColdWriter) uint64(v uint64) {
	binary.LittleEndian.PutUint64(w.scratch[:8], v)
	w.bytes(w.scratch[:8])
}
func (w *benchColdWriter) uint32(v uint32) {
	binary.LittleEndian.PutUint32(w.scratch[:4], v)
	w.bytes(w.scratch[:4])
}
func (w *benchColdWriter) patch64(at int, v uint64) {
	if w.err != nil {
		return
	}
	if w.err = w.writer.Flush(); w.err != nil {
		return
	}
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], v)
	_, w.err = w.file.WriteAt(b[:], int64(at))
}
func (w *benchColdWriter) finish() error {
	if w.err != nil {
		return w.err
	}
	if err := w.writer.Flush(); err != nil {
		return err
	}
	h := crc32.NewIEEE()
	if _, err := io.CopyBuffer(h, io.NewSectionReader(w.file, 0, int64(w.offset)), make([]byte, 64<<10)); err != nil {
		return err
	}
	w.uint32(h.Sum32())
	if w.err != nil {
		return w.err
	}
	if err := w.writer.Flush(); err != nil {
		return err
	}
	return w.file.Sync()
}
func benchFrozen(n, chunks int) []frozenSeries {
	out := make([]frozenSeries, n)
	for i := range out {
		pairs := [][2]string{{"__name__", fmt.Sprintf("metric_%02d", i%32)}, {"cluster", "dev-synthetic-central"}, {"container", "metrics-ingester"}, {"instance", fmt.Sprintf("10.0.%d.%d:9095", i/256, i%256)}, {"job", "metrics/ingester"}, {"namespace", "synthetic"}, {"pod", fmt.Sprintf("ingester-zone-a-%d", i)}, {"region", "us-central1"}, {"service", "metrics"}, {"team", "metrics-storage"}}
		metas := make([]ChunkMeta, chunks)
		for j := range metas {
			metas[j] = ChunkMeta{Ref: uint64(i*chunks+j) * 100, MinTime: int64(j) * 30000, MaxTime: int64(j+1)*30000 - 1, Len: 100, Encoding: xorEncoding}
		}
		out[i] = frozenSeries{tenant: "synthetic", labels: labels.FromSorted(pairs), chunks: chunkListFromMetas(metas)}
	}
	return out
}
func BenchmarkOptimizationColdWrite(b *testing.B) {
	for _, n := range []int{1000, 20000} {
		for _, stream := range []bool{false, true} {
			name := "baseline"
			if stream {
				name = "stream"
			}
			b.Run(fmt.Sprintf("%d/%s", n, name), func(b *testing.B) {
				rows := benchFrozen(n, 12)
				dir := b.TempDir()
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					var block *coldBlock
					var err error
					if stream {
						block, err = benchmarkStreamColdBlock(dir, uint64(i), rows)
					} else {
						block, err = buildMemoryColdBlock(dir, uint64(i), rows)
					}
					if err != nil {
						b.Fatal(err)
					}
					path := block.path
					if err = block.close(); err != nil {
						b.Fatal(err)
					}
					if err = os.Remove(path); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
func TestOptimizationColdWriterEquivalent(t *testing.T) {
	rows := benchFrozen(1000, 12)
	a, err := buildMemoryColdBlock(t.TempDir(), 1, slices.Clone(rows))
	if err != nil {
		t.Fatal(err)
	}
	defer a.close()
	b, err := benchmarkStreamColdBlock(t.TempDir(), 1, slices.Clone(rows))
	if err != nil {
		t.Fatal(err)
	}
	defer b.close()
	x, err := os.ReadFile(a.path)
	if err != nil {
		t.Fatal(err)
	}
	y, err := os.ReadFile(b.path)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(x, y) {
		t.Fatalf("blocks differ: baseline=%d streamed=%d", len(x), len(y))
	}
}
func benchmarkStreamColdBlock(directory string, id uint64, series []frozenSeries) (*coldBlock, error) {
	sort.SliceStable(series, func(a, b int) bool {
		if series[a].tenant != series[b].tenant {
			return series[a].tenant < series[b].tenant
		}
		return labels.Compare(series[a].labels, series[b].labels) < 0
	})
	var names []string
	local := map[string]uint32{}
	pairs := 0
	for index := range series {
		series[index].labels.Range(func(name, _ string) {
			pairs++
			if _, ok := local[name]; !ok {
				local[name] = uint32(len(names))
				names = append(names, name)
			}
		})
	}
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return nil, err
	}
	path := coldBlockPath(directory, id)
	file, err := os.Create(path + ".tmp")
	if err != nil {
		return nil, err
	}
	defer file.Close()
	out := benchColdWriter{file: file, writer: bufio.NewWriterSize(file, 64<<10)}
	out.bytes(coldMagic)
	out.varint(uint64(len(names)))
	for _, name := range names {
		out.varint(uint64(len(name)))
		out.text(name)
	}
	// Tenants: id, time range and where their tables are, patched once written.
	type tenantRange struct{ start, end int }
	var ranges []tenantRange
	for start := 0; start < len(series); {
		end := start
		for end < len(series) && series[end].tenant == series[start].tenant {
			end++
		}
		ranges = append(ranges, tenantRange{start, end})
		start = end
	}
	out.varint(uint64(len(ranges)))
	headers := make([]int, len(ranges))
	for index, r := range ranges {
		tenant := series[r.start].tenant
		out.varint(uint64(len(tenant)))
		out.text(tenant)
		headers[index] = out.offset
		// min, max, series table, series count, postings table, postings count, hashes table.
		out.bytes(make([]byte, 56))
	}
	blockMin, blockMax := int64(math.MaxInt64), int64(math.MinInt64)
	type posting struct {
		name  uint32
		hash  uint64
		index uint32
	}
	var labelBuffer []byte
	postings := make([]posting, 0, pairs)
	for rangeIndex, r := range ranges {
		offsets := make([]uint64, 0, r.end-r.start)
		postings = postings[:0]
		minTime, maxTime := int64(math.MaxInt64), int64(math.MinInt64)
		for index, frozen := range series[r.start:r.end] {
			offsets = append(offsets, uint64(out.offset))
			labelBuffer = labelBuffer[:0]
			frozen.labels.Range(func(name, value string) {
				labelBuffer = putUvarint(labelBuffer, uint64(local[name]))
				labelBuffer = putUvarint(labelBuffer, uint64(len(value)))
				labelBuffer = append(labelBuffer, value...)
				postings = append(postings, posting{local[name], labels.ValueHash(value), uint32(index)})
			})
			out.varint(uint64(len(labelBuffer)))
			out.bytes(labelBuffer)
			out.varint(uint64(len(frozen.chunks)))
			out.bytes(frozen.chunks)
			histogram := byte(0)
			if frozen.nativeHistogram {
				histogram = 1
			}
			out.scratch[0] = histogram
			out.bytes(out.scratch[:1])
			it := frozen.chunks.iter()
			for chunk, more := it.next(); more; chunk, more = it.next() {
				minTime = min(minTime, chunk.MinTime)
				maxTime = max(maxTime, chunk.MaxTime)
			}
		}
		seriesTable := out.offset
		for _, offset := range offsets {
			out.uint64(offset)
		}
		// Each series' labels hash and query shard hash, which queries need of every series they look at.
		hashesTable := out.offset
		for _, frozen := range series[r.start:r.end] {
			out.uint64(frozen.labels.Hash())
			out.uint64(labels.StableHash(frozen.labels))
		}
		slices.SortFunc(postings, func(a, b posting) int {
			switch {
			case a.name != b.name:
				if a.name < b.name {
					return -1
				}
				return 1
			case a.hash != b.hash:
				if a.hash < b.hash {
					return -1
				}
				return 1
			case a.index != b.index:
				if a.index < b.index {
					return -1
				}
				return 1
			}
			return 0
		})
		// Entries of (name, value hash, list offset), then the lists of series indexes: each entry is a run of the sorted
		// postings, its list their indexes.
		var entries []int
		for at, p := range postings {
			if at == 0 || postings[at-1].name != p.name || postings[at-1].hash != p.hash {
				entries = append(entries, at)
			}
		}
		runEnd := func(entry int) int {
			if entry+1 < len(entries) {
				return entries[entry+1]
			}
			return len(postings)
		}
		postingsTable := out.offset
		var listOffset uint32
		for entry, at := range entries {
			out.uint32(postings[at].name)
			out.uint64(postings[at].hash)
			out.uint32(listOffset)
			listOffset += 4 + 4*uint32(runEnd(entry)-at)
		}
		for entry, at := range entries {
			out.uint32(uint32(runEnd(entry) - at))
			for _, p := range postings[at:runEnd(entry)] {
				out.uint32(p.index)
			}
		}
		fields := [7]uint64{uint64(minTime), uint64(maxTime), uint64(seriesTable), uint64(len(offsets)), uint64(postingsTable), uint64(len(entries)), uint64(hashesTable)}
		for slot, field := range fields {
			out.patch64(headers[rangeIndex]+slot*8, field)
		}
		blockMin = min(blockMin, minTime)
		blockMax = max(blockMax, maxTime)
	}
	if err := out.finish(); err != nil {
		return nil, err
	}
	if err := os.Rename(path+".tmp", path); err != nil {
		return nil, err
	}
	block, err := openColdBlock(path, id, false)
	if err != nil {
		return nil, err
	}
	if block.minTime != blockMin || block.maxTime != blockMax {
		return nil, fmt.Errorf("cold block time mismatch")
	}
	return block, nil
}

func benchmarkStreamMergedBlock(directory string, id uint64, blocks []*coldBlock, kept *retained, shard int) (*coldBlock, error) {
	type key struct {
		tenant string
		labels labels.Labels
	}
	var frozen []frozenSeries
	positions := map[key]int{}
	var pairs [][2]string
	for _, block := range blocks {
		for tenant, table := range block.tenants {
			for index := range table.seriesCount {
				series := block.seriesIn(table, index)
				chunks := chunkList(slices.Clone(series.chunks))
				if kept != nil {
					hash := block.labelsHash(table, index)
					metas := chunks.toSlice()
					if retainedMetas := slices.DeleteFunc(slices.Clone(metas), func(meta ChunkMeta) bool { return !kept.keepsSeries(meta, shard, hash) }); len(retainedMetas) < len(metas) {
						if len(retainedMetas) == 0 {
							continue
						}
						chunks = chunkListFromMetas(retainedMetas)
					}
				}
				stored := series.labelsInto(&pairs)
				at, seen := positions[key{tenant, stored}]
				if !seen {
					positions[key{tenant, stored}] = len(frozen)
					frozen = append(frozen, frozenSeries{tenant: tenant, labels: stored, chunks: chunks, nativeHistogram: series.nativeHistogram})
					continue
				}
				// The same series left the head more than once: the chunks of the later block come after.
				known := map[uint64]struct{}{}
				metas := frozen[at].chunks.toSlice()
				for _, meta := range metas {
					known[uint64(meta.Ref)] = struct{}{}
				}
				for _, meta := range chunks.toSlice() {
					if _, ok := known[uint64(meta.Ref)]; !ok {
						metas = append(metas, meta)
					}
				}
				frozen[at].chunks = chunkListFromMetas(metas)
				frozen[at].nativeHistogram = series.nativeHistogram
			}
		}
	}
	if len(frozen) == 0 {
		return nil, nil
	}
	block, err := benchmarkStreamColdBlock(directory, id, frozen)
	if err == nil && kept != nil {
		block.retainedTo = kept.cutoff
	}
	return block, err
}

func BenchmarkOptimizationColdMerge(b *testing.B) {
	for _, variant := range []string{"baseline", "stream", "lean"} {
		name := variant
		b.Run(name, func(b *testing.B) {
			source := b.TempDir()
			out := b.TempDir()
			var blocks []*coldBlock
			for r := range 4 {
				rows := benchFrozen(5000, 12)
				for i := 2500; i < len(rows); i++ {
					rows[i].labels = labels.FromSorted([][2]string{{"__name__", "unique"}, {"pod", fmt.Sprintf("pod-%d-%d", r, i)}})
				}
				block, err := buildMemoryColdBlock(source, uint64(r), rows)
				if err != nil {
					b.Fatal(err)
				}
				blocks = append(blocks, block)
				b.Cleanup(func() { _ = block.close() })
			}
			kept := &retained{cutoff: math.MinInt64}
			b.ReportAllocs()
			b.ResetTimer()
			cpuStarted := processCPU()
			for i := 0; i < b.N; i++ {
				var block *coldBlock
				var err error
				switch variant {
				case "lean":
					block, err = benchmarkLeanMergedBlock(out, uint64(i), blocks, kept, 0)
				case "stream":
					block, err = benchmarkStreamMergedBlock(out, uint64(i), blocks, kept, 0)
				default:
					block, err = baselineMergedBlock(out, uint64(i), blocks, kept, 0)
				}
				if err != nil {
					b.Fatal(err)
				}
				path := block.path
				if err = block.close(); err != nil {
					b.Fatal(err)
				}
				if err = os.Remove(path); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric((processCPU()-cpuStarted)*1e9/float64(b.N), "cpu-ns/op")
		})
	}
}

func benchmarkLeanMergedBlock(directory string, id uint64, blocks []*coldBlock, kept *retained, shard int) (*coldBlock, error) {
	return buildMergedBlock(directory, id, blocks, kept, shard)
}

func TestOptimizationColdMergeEquivalent(t *testing.T) {
	var blocks []*coldBlock
	for r := range 4 {
		rows := benchFrozen(1000, 12)
		for i := range rows {
			metas := rows[i].chunks.toSlice()
			for j := range metas {
				metas[j].Ref += uint64(r * 500000)
				metas[j].MinTime += int64(r) * 360000
				metas[j].MaxTime += int64(r) * 360000
				metas[j].OutOfOrder = i%7 == 0
			}
			rows[i].chunks = chunkListFromMetas(metas)
		}
		block, err := buildMemoryColdBlock(t.TempDir(), uint64(r), rows)
		if err != nil {
			t.Fatal(err)
		}
		blocks = append(blocks, block)
		t.Cleanup(func() { _ = block.close() })
	}
	for _, cutoff := range []int64{math.MinInt64, 400000, 1000000, 2000000} {
		t.Run(fmt.Sprint(cutoff), func(t *testing.T) {
			kept := &retained{cutoff: cutoff, outOfOrderFrom: []uint64{500000}}
			a, err := baselineMergedBlock(t.TempDir(), 1, blocks, kept, 0)
			if err != nil {
				t.Fatal(err)
			}
			b, err := benchmarkLeanMergedBlock(t.TempDir(), 1, blocks, kept, 0)
			if err != nil {
				t.Fatal(err)
			}
			if a == nil || b == nil {
				if a != b {
					t.Fatal("different empty result")
				}
				return
			}
			defer a.close()
			defer b.close()
			x, err := os.ReadFile(a.path)
			if err != nil {
				t.Fatal(err)
			}
			y, err := os.ReadFile(b.path)
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(x, y) {
				t.Fatalf("merged blocks differ at cutoff %d", cutoff)
			}
		})
	}
}

func BenchmarkOptimizationColdMergeShapes(b *testing.B) {
	for _, shape := range []struct {
		name    string
		shared  bool
		expired bool
	}{{"disjoint", false, false}, {"revived", true, false}, {"retention", true, true}} {
		for _, lean := range []bool{false, true} {
			variant := "baseline"
			if lean {
				variant = "lean"
			}
			b.Run(shape.name+"/"+variant, func(b *testing.B) {
				source := b.TempDir()
				out := b.TempDir()
				var blocks []*coldBlock
				for r := range 4 {
					rows := benchFrozen(5000, 12)
					for i := range rows {
						if !shape.shared || i >= 2500 {
							var pairs [][2]string
							rows[i].labels.Range(func(name, value string) {
								if name == "pod" {
									value += fmt.Sprintf("-block-%d", r)
								}
								pairs = append(pairs, [2]string{name, value})
							})
							rows[i].labels = labels.FromSorted(pairs)
						}
						metas := rows[i].chunks.toSlice()
						for j := range metas {
							metas[j].Ref += uint64(r * 500000)
						}
						rows[i].chunks = chunkListFromMetas(metas)
					}
					block, err := buildMemoryColdBlock(source, uint64(r), rows)
					if err != nil {
						b.Fatal(err)
					}
					blocks = append(blocks, block)
					b.Cleanup(func() { _ = block.close() })
				}
				cutoff := int64(math.MinInt64)
				if shape.expired {
					cutoff = 180000
				}
				kept := &retained{cutoff: cutoff}
				b.ReportAllocs()
				b.ResetTimer()
				cpuStarted := processCPU()
				for i := 0; i < b.N; i++ {
					var block *coldBlock
					var err error
					if lean {
						block, err = benchmarkLeanMergedBlock(out, uint64(i), blocks, kept, 0)
					} else {
						block, err = baselineMergedBlock(out, uint64(i), blocks, kept, 0)
					}
					if err != nil {
						b.Fatal(err)
					}
					path := block.path
					if err = block.close(); err != nil {
						b.Fatal(err)
					}
					if err = os.Remove(path); err != nil {
						b.Fatal(err)
					}
				}
				b.ReportMetric((processCPU()-cpuStarted)*1e9/float64(b.N), "cpu-ns/op")
			})
		}
	}
}

func baselineMergedBlock(directory string, id uint64, blocks []*coldBlock, kept *retained, shard int) (*coldBlock, error) {
	type key struct {
		tenant string
		labels labels.Labels
	}
	var frozen []frozenSeries
	positions := map[key]int{}
	var pairs [][2]string
	for _, block := range blocks {
		for tenant, table := range block.tenants {
			for index := range table.seriesCount {
				series := block.seriesIn(table, index)
				chunks := chunkList(slices.Clone(series.chunks))
				if kept != nil {
					hash := block.labelsHash(table, index)
					metas := chunks.toSlice()
					if retainedMetas := slices.DeleteFunc(slices.Clone(metas), func(meta ChunkMeta) bool { return !kept.keepsSeries(meta, shard, hash) }); len(retainedMetas) < len(metas) {
						if len(retainedMetas) == 0 {
							continue
						}
						chunks = chunkListFromMetas(retainedMetas)
					}
				}
				stored := series.labelsInto(&pairs)
				at, seen := positions[key{tenant, stored}]
				if !seen {
					positions[key{tenant, stored}] = len(frozen)
					frozen = append(frozen, frozenSeries{tenant: tenant, labels: stored, chunks: chunks, nativeHistogram: series.nativeHistogram})
					continue
				}
				// The same series left the head more than once: the chunks of the later block come after.
				known := map[uint64]struct{}{}
				metas := frozen[at].chunks.toSlice()
				for _, meta := range metas {
					known[uint64(meta.Ref)] = struct{}{}
				}
				for _, meta := range chunks.toSlice() {
					if _, ok := known[uint64(meta.Ref)]; !ok {
						metas = append(metas, meta)
					}
				}
				frozen[at].chunks = chunkListFromMetas(metas)
				frozen[at].nativeHistogram = series.nativeHistogram
			}
		}
	}
	if len(frozen) == 0 {
		return nil, nil
	}
	block, err := buildMemoryColdBlock(directory, id, frozen)
	if err == nil && kept != nil {
		block.retainedTo = kept.cutoff
	}
	return block, err
}
