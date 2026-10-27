// SPDX-License-Identifier: AGPL-3.0-only

package exemplars

import (
	"strconv"
	"strings"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
)

func TestStoredExemplarsDoNotKeepTheRecordBuffer(t *testing.T) {
	record := "trace_id0123456789abcdef"
	storage := New[uint64](3)
	received := Exemplar{Labels: []Label{{Name: record[:8], Value: record[8:]}}, Value: 1, TimestampMs: 10}
	_, rejection := storage.Add(1, func() uint64 { return 1 }, received, 0)
	require.Equal(t, Accepted, rejection)
	stored := storage.Select(-1<<63, 1<<63-1, func(*uint64) bool { return true })[0].Exemplars[0]
	require.True(t, stored.Equal(&received))
	start := uintptr(unsafe.Pointer(unsafe.StringData(record)))
	end := start + uintptr(len(record))
	for _, label := range stored.Labels {
		for _, s := range []string{label.Name, label.Value} {
			p := uintptr(unsafe.Pointer(unsafe.StringData(s)))
			require.False(t, p >= start && p < end, "label shares the record buffer")
		}
	}
}

func exemplar(timestampMs int64, value float64) Exemplar {
	return Exemplar{Labels: []Label{{Name: "trace_id", Value: strconv.FormatInt(timestampMs, 10)}}, Value: value, TimestampMs: timestampMs}
}

func timestamps(storage *TenantExemplars[uint64], series uint64) []int64 {
	var out []int64
	for _, selected := range storage.Select(-1<<63, 1<<63-1, func(l *uint64) bool { return *l == series }) {
		for _, e := range selected.Exemplars {
			out = append(out, e.TimestampMs)
		}
	}
	return out
}

func add(storage *TenantExemplars[uint64], series uint64, e Exemplar, window int64) Rejection {
	_, rejection := storage.Add(series, func() uint64 { return series }, e, window)
	return rejection
}

func TestEvictsTheOldestInsertedExemplarAcrossSeries(t *testing.T) {
	storage := New[uint64](3)
	require.Equal(t, Accepted, add(storage, 1, exemplar(10, 1), 0))
	require.Equal(t, Accepted, add(storage, 2, exemplar(11, 1), 0))
	require.Equal(t, Accepted, add(storage, 1, exemplar(12, 1), 0))
	require.Equal(t, Accepted, add(storage, 2, exemplar(13, 1), 0))
	require.Equal(t, 3, storage.Len())
	require.Equal(t, []int64{12}, timestamps(storage, 1))
	require.Equal(t, []int64{11, 13}, timestamps(storage, 2))
	storage.Resize(1)
	require.Empty(t, timestamps(storage, 1))
	require.Equal(t, []int64{13}, timestamps(storage, 2))
	storage.Resize(0)
	require.Equal(t, Disabled, add(storage, 1, exemplar(20, 1), 0))
}

func TestRejectsOutOfOrderExemplarsOutsideTheWindow(t *testing.T) {
	storage := New[uint64](10)
	require.Equal(t, Accepted, add(storage, 1, exemplar(100, 1), 0))
	// Duplicates of the newest exemplar are accepted and not stored twice.
	require.Equal(t, Accepted, add(storage, 1, exemplar(100, 1), 0))
	require.Equal(t, 1, storage.Len())
	require.Equal(t, OutOfOrder, add(storage, 1, exemplar(90, 1), 0))
	require.Equal(t, OutOfOrder, add(storage, 1, exemplar(100, 0.5), 0))
	// With a window, older exemplars within it are stored in timestamp order.
	require.Equal(t, Accepted, add(storage, 1, exemplar(90, 1), 20))
	require.Equal(t, Accepted, add(storage, 1, exemplar(95, 1), 20))
	require.Equal(t, OutOfOrder, add(storage, 1, exemplar(80, 1), 20))
	require.Equal(t, []int64{90, 95, 100}, timestamps(storage, 1))
	// A timestamp already stored is skipped.
	require.Equal(t, Accepted, add(storage, 1, exemplar(95, 7), 20))
	require.Equal(t, 3, storage.Len())
	require.Equal(t, Accepted, add(storage, 1, exemplar(110, 1), 20))
	require.Equal(t, []int64{90, 95, 100, 110}, timestamps(storage, 1))
	var inserted []int64
	for _, i := range storage.InInsertionOrder() {
		inserted = append(inserted, i.Exemplar.TimestampMs)
	}
	require.Equal(t, []int64{100, 90, 95, 110}, inserted)
}

func TestRejectsLongLabelSetsAndSelectsByTime(t *testing.T) {
	storage := New[uint64](10)
	long := exemplar(1, 1)
	long.Labels[0].Value = strings.Repeat("x", MaxLabelSetLength)
	require.Equal(t, LabelLength, add(storage, 1, long, 0))
	for _, ts := range []int64{10, 20, 30} {
		require.Equal(t, Accepted, add(storage, 1, exemplar(ts, 1), 0))
	}
	selected := storage.Select(15, 30, func(*uint64) bool { return true })
	var got []int64
	for _, e := range selected[0].Exemplars {
		got = append(got, e.TimestampMs)
	}
	require.Equal(t, []int64{20, 30}, got)
	require.Empty(t, storage.Select(31, 40, func(*uint64) bool { return true }))
}

// Eviction keeps its queue bounded by the capacity however many exemplars pass through it.
func TestEvictionQueueStaysBounded(t *testing.T) {
	storage := New[uint64](10)
	for ts := int64(0); ts < 10_000; ts++ {
		require.Equal(t, Accepted, add(storage, uint64(ts%3), exemplar(ts, 1), 0))
	}
	require.Equal(t, 10, storage.Len())
	require.LessOrEqual(t, cap(storage.order), 1024)
}
