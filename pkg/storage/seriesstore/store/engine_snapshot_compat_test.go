// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/prometheus/prometheus/model/histogram"
	promlabels "github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/require"
)

// Written by the version-8 writer with tenant maps, including empty shards and exemplar storage.
func TestEngineRestoresPreviousSnapshotLayout(t *testing.T) {
	dir := t.TempDir()
	data, err := os.ReadFile(filepath.Join("testdata", "snapshot-v8.bin"))
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(filepath.Join(dir, storeDirName), 0755))
	for shard := range 4 {
		require.NoError(t, os.MkdirAll(shardDir(filepath.Join(dir, storeDirName), shard), 0755))
		require.NoError(t, os.MkdirAll(coldDir(filepath.Join(dir, storeDirName), shard), 0755))
	}
	require.NoError(t, os.WriteFile(filepath.Join(dir, storeDirName, snapshotFileName), data, 0644))
	opts := EngineOptions{Shards: 4, OutOfOrderTimeWindowMs: 3600000, MaxExemplars: 10, Checkpoints: true}
	e, err := OpenEngine(dir, "tenant", opts)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, e.Close()) })
	for round := range 2 {
		require.True(t, e.Restored())
		if round == 0 {
			require.Equal(t, []byte("single-tenant-compatibility"), e.RestoredCheckpoint())
		} else {
			require.Empty(t, e.RestoredCheckpoint())
		}
		require.Equal(t, uint64(2), e.NumSeries())
		counter := querySamples(t, e, 0, 30000, nil, eq("__name__", "counter"))
		require.Len(t, counter, 1)
		require.Equal(t, []decodedSample{
			{t: 10000, f: math.Float64bits(1), typ: chunkenc.ValFloat},
			{t: 15000, f: math.Float64bits(2), typ: chunkenc.ValFloat},
			{t: 20000, f: math.Float64bits(3), typ: chunkenc.ValFloat},
		}, counter[0].samples)
		h := &histogram.Histogram{Count: 3, Sum: 3, Schema: 0, PositiveSpans: []histogram.Span{{Offset: 0, Length: 1}}, PositiveBuckets: []int64{3}}
		hist := querySamples(t, e, 0, 30000, nil, eq("__name__", "histogram"))
		require.Len(t, hist, 1)
		require.Equal(t, []decodedSample{{t: 20000, h: h.String(), typ: chunkenc.ValHistogram}}, hist[0].samples)
		q, err := e.ExemplarQuerier(t.Context())
		require.NoError(t, err)
		exemplars, err := q.Select(0, 30000, []*promlabels.Matcher{eq("__name__", "counter")})
		require.NoError(t, err)
		require.Len(t, exemplars, 1)
		require.Len(t, exemplars[0].Exemplars, 1)
		require.Equal(t, "abc", exemplars[0].Exemplars[0].Labels.Get("trace_id"))
		if round == 0 {
			require.NoError(t, e.Close())
			e, err = OpenEngine(dir, "tenant", opts)
			require.NoError(t, err)
		}
	}
}

func TestSnapshotRejectsAnotherTenant(t *testing.T) {
	file, err := os.Open(filepath.Join("testdata", "snapshot-v8.bin"))
	require.NoError(t, err)
	defer func() { require.NoError(t, file.Close()) }()
	restored, err := readSnapshot(file, t.TempDir(), "other")
	require.EqualError(t, err, `snapshot tenant "tenant" does not match "other"`)
	require.Nil(t, restored)
}
