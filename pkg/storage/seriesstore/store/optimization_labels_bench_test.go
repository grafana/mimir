// SPDX-License-Identifier: AGPL-3.0-only
package store

import (
	"encoding/binary"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"unsafe"

	"github.com/grafana/mimir/pkg/storage/seriesstore/labels"
)

type benchmarkValueDict struct {
	ids    map[uint32]map[string]uint32
	values map[uint32][]string
}
type benchmarkLabelTable struct {
	rows []string
	dict *benchmarkValueDict
}

func benchmarkLabelPairs(n int, unique bool) [][][2]string {
	out := make([][][2]string, n)
	for i := range out {
		out[i] = [][2]string{{"__name__", "synthetic_metric"}, {"cluster", "dev-synthetic-central"}, {"container", "metrics-ingester"}, {"instance", fmt.Sprintf("10.0.%d.%d:9095", i/256, i%256)}, {"job", "metrics/ingester"}, {"namespace", "synthetic"}, {"pod", fmt.Sprintf("ingester-zone-a-%d", i)}, {"region", "us-central1"}, {"service", "metrics"}, {"team", "metrics-storage"}}
		if unique {
			for j := range out[i] {
				out[i][j][1] = fmt.Sprintf("unique-value-%d-%d-abcdefghijklmnop", i, j)
			}
		}
	}
	return out
}
func benchmarkBuildLabelTable(pairs [][][2]string, dictionary bool) *benchmarkLabelTable {
	table := &benchmarkLabelTable{rows: make([]string, len(pairs))}
	if !dictionary {
		for i, row := range pairs {
			var ids [32]uint32
			size := 0
			for j, p := range row {
				ids[j] = labels.Intern(p[0])
				size += uvarintLen(uint64(ids[j])) + uvarintLen(uint64(len(p[1]))<<1) + len(p[1])
			}
			encoded := make([]byte, 0, size)
			for j, p := range row {
				encoded = labels.PutVarint(encoded, uint64(ids[j]))
				encoded = labels.PutVarint(encoded, uint64(len(p[1]))<<1)
				encoded = append(encoded, p[1]...)
			}
			table.rows[i] = unsafe.String(unsafe.SliceData(encoded), len(encoded))
		}
		return table
	}
	d := &benchmarkValueDict{ids: map[uint32]map[string]uint32{}, values: map[uint32][]string{}}
	sampled := min(2048, len(pairs))
	counts := map[uint32]map[string]int{}
	for _, row := range pairs[:sampled] {
		for _, p := range row {
			id := labels.Intern(p[0])
			if counts[id] == nil {
				counts[id] = map[string]int{}
			}
			counts[id][p[1]]++
		}
	}
	for id, vs := range counts {
		if len(vs) > sampled/8 {
			continue
		}
		d.ids[id] = map[string]uint32{}
		for value, n := range vs {
			if n < 8 {
				continue
			}
			at := uint32(len(d.values[id]))
			owned := strings.Clone(value)
			d.ids[id][owned] = at
			d.values[id] = append(d.values[id], owned)
		}
	}
	table.dict = d

	var ids [32]uint32
	var tags [32]uint64
	for i, row := range pairs {
		if len(row) > len(ids) {
			panic("benchmark fixture has more than 32 labels")
		}
		size := 0
		for j, p := range row {
			id := labels.Intern(p[0])
			ids[j] = id
			at, shared := d.ids[id][p[1]]
			size += uvarintLen(uint64(id))
			if shared {
				tags[j] = uint64(at)<<1 | 1
				size += uvarintLen(tags[j])
			} else {
				tags[j] = uint64(len(p[1])) << 1
				size += uvarintLen(tags[j]) + len(p[1])
			}
		}
		out := make([]byte, 0, size)
		for j, p := range row {
			out = binary.AppendUvarint(out, uint64(ids[j]))
			out = binary.AppendUvarint(out, tags[j])
			if tags[j]&1 == 0 {
				out = append(out, p[1]...)
			}
		}
		table.rows[i] = unsafe.String(unsafe.SliceData(out), len(out))
	}
	return table
}
func (t *benchmarkLabelTable) get(row int, name string) string {
	if t.dict == nil {
		return labels.Labels(t.rows[row]).Get(name)
	}
	wanted, ok := labels.Lookup(name)
	if !ok {
		return ""
	}
	s := t.rows[row]
	for i := 0; i < len(s); {
		var id, tag uint64
		if c := s[i]; c < 0x80 {
			id = uint64(c)
			i++
		} else {
			rest := s[i:]
			id = takeUvarintString(&rest)
			i = len(s) - len(rest)
		}
		if c := s[i]; c < 0x80 {
			tag = uint64(c)
			i++
		} else {
			rest := s[i:]
			tag = takeUvarintString(&rest)
			i = len(s) - len(rest)
		}
		if tag&1 != 0 {
			if uint32(id) == wanted {
				return t.dict.values[uint32(id)][tag>>1]
			}
		} else {
			size := int(tag >> 1)
			if uint32(id) == wanted {
				return s[i : i+size]
			}
			i += size
		}
	}
	return ""
}

func BenchmarkOptimizationLabelBuild(b *testing.B) {
	for _, unique := range []bool{false, true} {
		shape := "repeated"
		if unique {
			shape = "unique"
		}
		pairs := benchmarkLabelPairs(100000, unique)
		for _, dict := range []bool{false, true} {
			name := "baseline"
			if dict {
				name = "dictionary"
			}
			b.Run(shape+"/"+name, func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					table := benchmarkBuildLabelTable(pairs, dict)
					runtime.KeepAlive(table)
				}
			})
		}
	}
}

var benchmarkLabelValue string

func BenchmarkOptimizationLabelLookup(b *testing.B) {
	pairs := benchmarkLabelPairs(100000, false)
	for _, dict := range []bool{false, true} {
		name := "baseline"
		if dict {
			name = "dictionary"
		}
		table := benchmarkBuildLabelTable(pairs, dict)
		for _, label := range []string{"namespace", "pod"} {
			b.Run(name+"/"+label, func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					benchmarkLabelValue = table.get(i%len(table.rows), label)
				}
			})
		}
	}
}
func TestOptimizationLabelDictionary(t *testing.T) {
	for _, unique := range []bool{false, true} {
		pairs := benchmarkLabelPairs(4096, unique)
		table := benchmarkBuildLabelTable(pairs, true)
		for i, row := range pairs {
			for _, p := range row {
				if got := table.get(i, p[0]); got != p[1] {
					t.Fatalf("row %d label %s got %q want %q", i, p[0], got, p[1])
				}
			}
		}
	}
}
func TestOptimizationLabelFootprint(t *testing.T) {
	for _, unique := range []bool{false, true} {
		pairs := benchmarkLabelPairs(100000, unique)
		for _, dict := range []bool{false, true} {
			runtime.GC()
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			table := benchmarkBuildLabelTable(pairs, dict)
			runtime.GC()
			runtime.ReadMemStats(&after)
			t.Logf("unique=%t dictionary=%t retained=%d bytes (%.1f B/series)", unique, dict, int64(after.HeapAlloc)-int64(before.HeapAlloc), float64(int64(after.HeapAlloc)-int64(before.HeapAlloc))/float64(len(pairs)))
			runtime.KeepAlive(table)
			runtime.KeepAlive(pairs)
		}
	}
}
