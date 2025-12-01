package hll

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"compress/lzw"
	"compress/zlib"
	"fmt"
	"math"
	"math/rand"
	"slices"
	"strings"
	"testing"

	"github.com/DataDog/hyperloglog"
	"github.com/DmitriyVTitov/size"
	"github.com/stretchr/testify/require"
)

type enc struct {
	val  uint8
	bits uint8
}

const tableOffset = 2
const fullValueMark = 0b111
const fullValueMarkBits = 3
const fullValueBits = 6

var table = [5]enc{
	-2 + tableOffset: {val: 0b110, bits: 3},
	-1 + tableOffset: {val: 0b011, bits: 3},
	0 + tableOffset:  {val: 0b00, bits: 2},
	1 + tableOffset:  {val: 0b010, bits: 3},
	2 + tableOffset:  {val: 0b101, bits: 3},
}

func TestNewSeriesHLL(t *testing.T) {
	const partitions = 5000
	const seriesPerPartition = 10
	for _, seriesPerPartition := range []int{1, 10, 100} {
		hlls := make([]*hyperloglog.HyperLogLog, partitions)
		for i := range hlls {
			var err error
			hlls[i], err = hyperloglog.New(2048)
			require.NoError(t, err)
		}

		for i := range hlls {
			for s := 0; s < seriesPerPartition; s++ {
				hlls[i].Add64(rand.Uint64())
			}
		}

		var totalSize int
		{
			var buf bytes.Buffer
			z, _ := gzip.NewWriterLevel(&buf, gzip.BestCompression)
			for _, hll := range hlls {
				_, err := z.Write(hll.Registers)
				require.NoError(t, err)
				totalSize += size.Of(hll.Registers)
			}
			require.NoError(t, z.Flush())
			require.NoError(t, z.Close())
			t.Logf("Total new series: %d (%d per partition, %d partitions), registers gzipped size: %d, ratio: %2.2f%%",
				seriesPerPartition*partitions, seriesPerPartition, partitions,
				len(buf.Bytes()),
				100*float64(len(buf.Bytes()))/float64(totalSize))
		}
	}
}

func TestHLL(t *testing.T) {
	const total = 3e6 / 20

	var maxErrorPercentage float64
	var globalMaxRegister uint8
	var globalMaxDelta int
	for n := 0; n < 100; n++ {
		hll, err := hyperloglog.New(2048)
		require.NoError(t, err)
		for i := 0; i < total; i++ {
			hll.Add64(rand.Uint64())
		}

		count := hll.Count()
		pct := 100 * math.Abs(float64(count)-total) / total
		maxErrorPercentage = math.Max(maxErrorPercentage, pct)
		sz := size.Of(hll)
		t.Logf("Total: %.0f, size: %d, counted: %d, error: %2.2f%%", total, sz, count, pct)

		freq := map[int]int{}
		for _, r := range hll.Registers {
			freq[int(r)]++
		}
		var vals []int
		for k := range freq {
			vals = append(vals, k)
		}
		slices.SortFunc(vals, func(i, j int) int {
			return freq[j] - freq[i]
		})
		sb := &strings.Builder{}
		for i, v := range vals[:5] {
			if i > 0 {
				sb.WriteString(", ")
			}
			fmt.Fprintf(sb, "%d:%dx", v, freq[v])
		}
		t.Logf("Reg values: %d, freq: %s", len(vals), sb.String())
		bs := &bstream{}
		prev := hll.Registers[0]
		bs.writeBits(uint64(prev), fullValueBits)
		maxDelta := 0
		dfreq := map[int]int{}

		for i, r := range hll.Registers[1:] {
			if i > 0 {
				sb.WriteString(",")
			}
			delta := int(r) - int(prev)
			if delta < -2 || delta > 2 {
				bs.writeBits(fullValueMark, fullValueMarkBits)
				bs.writeBits(uint64(r), fullValueBits)
			} else {
				e := table[delta+tableOffset]
				bs.writeBits(uint64(e.val), int(e.bits))
			}
			dfreq[delta]++
			maxDelta = max(maxDelta, int(math.Abs(float64(delta))))
			globalMaxDelta = max(globalMaxDelta, maxDelta)
			prev = r
		}
		var deltas []int
		for k := range dfreq {
			deltas = append(deltas, k)
		}
		slices.SortFunc(deltas, func(i, j int) int {
			return dfreq[j] - dfreq[i]
		})
		sb.Reset()
		cnt := 0
		const top = 5
		for i, d := range deltas[:top] {
			if i > 0 {
				sb.WriteString(", ")
			}
			fmt.Fprintf(sb, "%d:%dx", d, dfreq[d])
			cnt += dfreq[d]
		}

		toppcnt := 100 * float64(cnt) / float64(len(hll.Registers))
		t.Logf("Deltas: max: %d, count: %d, top(%d) pct: %2f, freq: %s", maxDelta, len(vals), top, toppcnt, sb.String())
		t.Logf("Bstream encoded size: %d, ratio: %2.2f%%", len(bs.bytes()), 100*float64(len(bs.bytes()))/float64(sz))
		{
			var buf bytes.Buffer
			z, _ := gzip.NewWriterLevel(&buf, gzip.BestCompression)
			_, err = z.Write(hll.Registers)
			require.NoError(t, err)
			require.NoError(t, z.Flush())
			require.NoError(t, z.Close())
			t.Logf("Registers gzipped size: %d, ratio: %2.2f%%", len(buf.Bytes()), 100*float64(len(buf.Bytes()))/float64(sz))
		}
		{
			var buf bytes.Buffer
			z, _ := gzip.NewWriterLevel(&buf, gzip.BestCompression)
			_, err = z.Write(bs.bytes())
			require.NoError(t, err)
			require.NoError(t, z.Flush())
			require.NoError(t, z.Close())
			t.Logf("Bstream gzipped size: %d, ratio: %2.2f%%", len(buf.Bytes()), 100*float64(len(buf.Bytes()))/float64(sz))
		}

		{
			var buf bytes.Buffer
			z, _ := zlib.NewWriterLevel(&buf, zlib.BestCompression)
			_, err = z.Write(hll.Registers)
			require.NoError(t, err)
			require.NoError(t, z.Flush())
			require.NoError(t, z.Close())
			t.Logf("Registers zlib size: %d, ratio: %2.2f%%", len(buf.Bytes()), 100*float64(len(buf.Bytes()))/float64(sz))
		}

		{
			var buf bytes.Buffer
			z := lzw.NewWriter(&buf, lzw.LSB, 6)
			_, err = z.Write(hll.Registers)
			require.NoError(t, err)
			require.NoError(t, z.Close())
			t.Logf("Registers lzw size: %d, ratio: %2.2f%%", len(buf.Bytes()), 100*float64(len(buf.Bytes()))/float64(sz))

			// check back
			read := make([]uint8, len(hll.Registers))
			zr := lzw.NewReader(&buf, lzw.LSB, 6)
			_, err = zr.Read(read)
			require.NoError(t, err)
			require.NoError(t, zr.Close())
			require.Equal(t, hll.Registers, read)
		}
		{
			var buf bytes.Buffer
			z, _ := flate.NewWriter(&buf, flate.BestCompression)
			_, err = z.Write(hll.Registers)
			require.NoError(t, err)
			require.NoError(t, z.Flush())
			require.NoError(t, z.Close())
			t.Logf("Registers flate size: %d, ratio: %2.2f%%", len(buf.Bytes()), 100*float64(len(buf.Bytes()))/float64(sz))
		}

	}
	t.Logf("Max error: %.2f%%", maxErrorPercentage)
	t.Logf("Max register: %d", globalMaxRegister)
	t.Logf("Max delta: %d", globalMaxDelta)
}
