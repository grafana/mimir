// SPDX-License-Identifier: AGPL-3.0-only
package store

import (
	"bufio"
	"cmp"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"slices"
	"sort"

	"github.com/grafana/mimir/pkg/storage/seriesstore/labels"
)

type coldBlockWriter struct {
	file    *os.File
	writer  *bufio.Writer
	offset  int
	err     error
	scratch [10]byte
}

func (w *coldBlockWriter) bytes(p []byte) {
	if w.err != nil {
		return
	}
	n, err := w.writer.Write(p)
	w.offset += n
	w.err = err
}
func (w *coldBlockWriter) text(p string) {
	if w.err != nil {
		return
	}
	n, err := w.writer.WriteString(p)
	w.offset += n
	w.err = err
}
func (w *coldBlockWriter) varint(v uint64) {
	n := binary.PutUvarint(w.scratch[:], v)
	w.bytes(w.scratch[:n])
}
func (w *coldBlockWriter) uint64(v uint64) {
	binary.LittleEndian.PutUint64(w.scratch[:8], v)
	w.bytes(w.scratch[:8])
}
func (w *coldBlockWriter) uint32(v uint32) {
	binary.LittleEndian.PutUint32(w.scratch[:4], v)
	w.bytes(w.scratch[:4])
}
func (w *coldBlockWriter) patchHeader(at int, fields [7]uint64) {
	if w.err != nil {
		return
	}
	if w.err = w.writer.Flush(); w.err != nil {
		return
	}
	var header [56]byte
	for slot, value := range fields {
		binary.LittleEndian.PutUint64(header[slot*8:], value)
	}
	_, w.err = w.file.WriteAt(header[:], int64(at))
}

func (w *coldBlockWriter) finish() error {
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
func writeColdBlock(directory string, id uint64, series []frozenSeries) (*coldBlock, error) {
	sort.SliceStable(series, func(a, b int) bool {
		if series[a].tenant != series[b].tenant {
			return series[a].tenant < series[b].tenant
		}
		return labels.Compare(series[a].labels, series[b].labels) < 0
	})
	var names []string
	local := map[string]uint32{}
	type postingRange struct {
		name uint32
		end  int
	}
	type tenantRange struct {
		start, end int
		postings   []postingRange
	}
	var ranges []tenantRange
	var counts []int
	var usedNames []uint32
	maxPairs := 0
	for start := 0; start < len(series); {
		end := start + 1
		for end < len(series) && series[end].tenant == series[start].tenant {
			end++
		}
		usedNames = usedNames[:0]
		for index := start; index < end; index++ {
			series[index].labels.Range(func(name, _ string) {
				id, ok := local[name]
				if !ok {
					id = uint32(len(names))
					local[name] = id
					names = append(names, name)
					counts = append(counts, 0)
				}
				if counts[id] == 0 {
					usedNames = append(usedNames, id)
				}
				counts[id]++
			})
		}
		slices.Sort(usedNames)
		groups := make([]postingRange, 0, len(usedNames))
		total := 0
		for _, name := range usedNames {
			total += counts[name]
			groups = append(groups, postingRange{name, total})
			counts[name] = 0
		}
		ranges = append(ranges, tenantRange{start, end, groups})
		maxPairs = max(maxPairs, total)
		start = end
	}
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return nil, err
	}
	path := coldBlockPath(directory, id)
	file, err := os.Create(path + ".tmp")
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close(); _ = os.Remove(path + ".tmp") }()
	out := coldBlockWriter{file: file, writer: bufio.NewWriterSize(file, 64<<10)}
	out.bytes(coldMagic)
	out.varint(uint64(len(names)))
	for _, name := range names {
		out.varint(uint64(len(name)))
		out.text(name)
	}
	// Tenants: id, time range and where their tables are, patched once written.
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
		name, index uint32
		hash        uint64
	}
	var labelBuffer []byte
	postingRoom := make([]posting, maxPairs)
	cursors := make([]int, len(names))
	for rangeIndex, r := range ranges {
		offsets := make([]uint64, 0, r.end-r.start)
		total := 0
		for _, group := range r.postings {
			cursors[group.name] = total
			total = group.end
		}
		postings := postingRoom[:total]
		minTime, maxTime := int64(math.MaxInt64), int64(math.MinInt64)
		for index, frozen := range series[r.start:r.end] {
			offsets = append(offsets, uint64(out.offset))
			labelBuffer = labelBuffer[:0]
			frozen.labels.Range(func(name, value string) {
				nameID := local[name]
				labelBuffer = putUvarint(labelBuffer, uint64(nameID))
				labelBuffer = putUvarint(labelBuffer, uint64(len(value)))
				labelBuffer = append(labelBuffer, value...)
				postings[cursors[nameID]] = posting{nameID, uint32(index), labels.ValueHash(value)}
				cursors[nameID]++
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
		// Grouping by name during the write leaves only each name's value hashes to sort.
		begin := 0
		for _, group := range r.postings {
			end := group.end
			slices.SortFunc(postings[begin:end], func(a, b posting) int {
				if order := cmp.Compare(a.hash, b.hash); order != 0 {
					return order
				}
				return cmp.Compare(a.index, b.index)
			})
			begin = end
		}
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
		out.patchHeader(headers[rangeIndex], fields)
		blockMin = min(blockMin, minTime)
		blockMax = max(blockMax, maxTime)
	}
	if err := out.finish(); err != nil {
		return nil, err
	}
	if err := file.Close(); err != nil {
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
