// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/seriesstore/labels"
)

// A prototype of what a schema-and-dictionary layout of the series' labels would cost and save, to decide whether to
// build it: the labels of 19 names, most of them shared by many series, as a Kubernetes tenant's are. Run with
// GO_INGESTER_BENCH=1.

type protoLabel struct {
	name string
	// How many distinct values it has, per series that share the pod it belongs to: 0 is one for every series.
	values int
	// Whether the value follows the pod, which 25 series share, or the series itself.
	perPod bool
}

var protoLabels = []protoLabel{
	{"__name__", 5000, false},
	{"app", 600, true},
	{"cluster", 20, true},
	{"code", 6, false},
	{"container", 1000, true},
	{"env", 3, true},
	{"id", 0, false},
	{"instance", 0, true},
	{"job", 200, true},
	{"le", 12, false},
	{"method", 8, false},
	{"namespace", 300, true},
	{"node", 2000, true},
	{"pod", 0, true},
	{"region", 4, true},
	{"service", 500, true},
	{"team", 30, true},
	{"tier", 5, true},
	{"version", 40, true},
}

func protoSeries(n int) [][2]string {
	pod := n / 25
	pairs := make([][2]string, 0, len(protoLabels))
	for _, label := range protoLabels {
		key := n
		if label.perPod {
			key = pod
		}
		var value string
		switch {
		case label.name == "__name__":
			// Metrics have skewed series counts.
			value = fmt.Sprintf("metric_%d", (n*2654435761)%label.values)
		case label.values == 0 && label.name == "id":
			value = fmt.Sprintf("%016x", uint64(n)*0x9e3779b97f4a7c15)
		case label.values == 0:
			value = fmt.Sprintf("%s-%d-%x", label.name, key, uint32(key)*2654435761)
		default:
			value = fmt.Sprintf("%s-%d", label.name, (key*2654435761)%label.values)
		}
		pairs = append(pairs, [2]string{label.name, value})
	}
	slices.SortFunc(pairs, comparePairs)
	return pairs
}

// protoColumns stores each label as a column: a dictionary of its values and a code for each series when it has few
// values, or the values themselves in one array when it has many.
type protoColumns struct {
	rows    int
	names   []string
	columns []protoColumn
}

type protoColumn struct {
	// Dictionary columns.
	dictionary []string
	codes8     []uint8
	codes16    []uint16
	codes32    []uint32
	// Raw columns: the values back to back and where each ends.
	arena []byte
	ends  []uint32
}

func buildProtoColumns(series [][][2]string) *protoColumns {
	c := &protoColumns{rows: len(series)}
	for _, pair := range series[0] {
		c.names = append(c.names, pair[0])
	}
	c.columns = make([]protoColumn, len(c.names))
	for column := range c.columns {
		distinct := map[string]uint32{}
		for _, s := range series {
			if _, ok := distinct[s[column][1]]; !ok {
				distinct[s[column][1]] = uint32(len(distinct))
			}
		}
		col := &c.columns[column]
		if len(distinct)*8 <= len(series) {
			col.dictionary = make([]string, len(distinct))
			for value, code := range distinct {
				col.dictionary[code] = value
			}
			switch {
			case len(distinct) == 1:
				// Every series has the value: a column of no bytes.
			case len(distinct) <= 256:
				col.codes8 = make([]uint8, len(series))
			case len(distinct) <= 65536:
				col.codes16 = make([]uint16, len(series))
			default:
				col.codes32 = make([]uint32, len(series))
			}
			for row, s := range series {
				code := distinct[s[column][1]]
				switch {
				case col.codes8 != nil:
					col.codes8[row] = uint8(code)
				case col.codes16 != nil:
					col.codes16[row] = uint16(code)
				case col.codes32 != nil:
					col.codes32[row] = code
				}
			}
			continue
		}
		col.ends = make([]uint32, 0, len(series))
		for _, s := range series {
			col.arena = append(col.arena, s[column][1]...)
			col.ends = append(col.ends, uint32(len(col.arena)))
		}
	}
	return c
}

func (c *protoColumn) code(row int) (uint32, bool) {
	switch {
	case c.codes8 != nil:
		return uint32(c.codes8[row]), true
	case c.codes16 != nil:
		return uint32(c.codes16[row]), true
	case c.codes32 != nil:
		return c.codes32[row], true
	case c.dictionary != nil:
		return 0, true
	}
	return 0, false
}

func (c *protoColumn) value(row int) string {
	if code, ok := c.code(row); ok {
		return c.dictionary[code]
	}
	start := uint32(0)
	if row > 0 {
		start = c.ends[row-1]
	}
	return string(c.arena[start:c.ends[row]])
}

// labelsOf assembles the labels of a row, which the stored encoding needs nothing for.
func (c *protoColumns) labelsOf(row int, pairs *[][2]string) labels.Labels {
	*pairs = (*pairs)[:0]
	for column := range c.columns {
		*pairs = append(*pairs, [2]string{c.names[column], c.columns[column].value(row)})
	}
	return labels.FromSorted(*pairs)
}

func (c *protoColumns) bytes() (dictionaries, codes, raw int) {
	for _, col := range c.columns {
		for _, v := range col.dictionary {
			dictionaries += 16 + len(v)
		}
		codes += len(col.codes8) + 2*len(col.codes16) + 4*len(col.codes32)
		raw += len(col.arena) + 4*len(col.ends)
	}
	return
}

func TestColumnarLabelsPrototype(t *testing.T) {
	requireBench(t)
	const series = 500_000
	pairs := make([][][2]string, series)
	for n := range pairs {
		pairs[n] = protoSeries(n)
	}

	// What the store keeps: each series' labels as one encoded string.
	before := liveHeapBytes()
	stored := make([]labels.Labels, series)
	for n := range stored {
		stored[n] = labels.FromSorted(pairs[n])
	}
	encoded := liveHeapBytes() - before
	var encodedBytes int
	for _, l := range stored {
		encodedBytes += l.HeapSize()
	}

	before = liveHeapBytes()
	columns := buildProtoColumns(pairs)
	columnar := liveHeapBytes() - before
	dictionaries, codes, raw := columns.bytes()
	fmt.Printf("labels per series: encoded=%dB (heap %dB), columnar=%dB (heap %dB): dictionaries=%dB codes=%dB raw=%dB\n",
		encodedBytes/series, int(encoded)/series, (dictionaries+codes+raw)/series, int(columnar)/series, dictionaries/series, codes/series, raw/series)
	for column, col := range columns.columns {
		kind := "raw"
		size := len(col.arena) + 4*len(col.ends)
		if col.dictionary != nil {
			kind = fmt.Sprintf("dictionary of %d", len(col.dictionary))
			size = len(col.codes8) + 2*len(col.codes16) + 4*len(col.codes32)
		}
		fmt.Printf("  %-10s %-22s %6.1f B/series\n", columns.names[column], kind, float64(size)/series)
	}

	// Assembling a row back to labels: the encoded form needs nothing, a query that returns series pays this for each.
	var scratch [][2]string
	start := time.Now()
	for n := range series {
		got := columns.labelsOf(n, &scratch)
		if n%100_000 == 0 {
			require.Equal(t, stored[n], got)
		}
	}
	fmt.Printf("assemble: %dns/series (encoded: 0, shared)\n", time.Since(start).Nanoseconds()/series)

	// A matcher on the namespace over all series: read each series' labels, or its code.
	namespace := slices.Index(columns.names, "namespace")
	id := labels.Intern("namespace")
	matches := func(value string) bool { return value == "namespace-7" || value == "namespace-11" }
	start = time.Now()
	count := 0
	for _, l := range stored {
		if matches(l.ValueOf(id)) {
			count++
		}
	}
	scanEncoded := time.Since(start)
	start = time.Now()
	col := &columns.columns[namespace]
	accepted := make([]bool, len(col.dictionary))
	for code, value := range col.dictionary {
		accepted[code] = matches(value)
	}
	other := 0
	for _, code := range col.codes16 {
		if accepted[code] {
			other++
		}
	}
	for _, code := range col.codes8 {
		if accepted[code] {
			other++
		}
	}
	scanColumnar := time.Since(start)
	require.Equal(t, count, other)
	fmt.Printf("scan of the namespace over %d series: encoded=%dns/series columnar=%dns/series\n", series, scanEncoded.Nanoseconds()/series, scanColumnar.Nanoseconds()/series)
	runtimeKeepAlive(stored, columns, pairs)
}

func runtimeKeepAlive(values ...any) { _ = rand.IntN(len(values)) }
