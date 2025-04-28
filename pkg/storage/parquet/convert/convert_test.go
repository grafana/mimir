// Copyright The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package convert

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/prometheus-community/parquet-common/schema"
	"github.com/prometheus-community/parquet-common/util"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/util/teststorage"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/filesystem"
)

func Test_Convert_TSDB(t *testing.T) {
	ctx := context.Background()

	tc := []struct {
		dataColDuration        time.Duration
		step                   time.Duration
		numberOfSamples        int
		expectedNumberOfChunks int
		expectedPointsPerChunk int
	}{
		{
			dataColDuration:        time.Hour,
			step:                   time.Hour,
			numberOfSamples:        3,
			expectedNumberOfChunks: 3,
			expectedPointsPerChunk: 1,
		},
		{
			dataColDuration:        time.Hour,
			step:                   time.Hour,
			numberOfSamples:        48,
			expectedNumberOfChunks: 48,
			expectedPointsPerChunk: 1,
		},
		{
			dataColDuration:        8 * time.Hour,
			step:                   time.Hour / 2,
			numberOfSamples:        10,
			expectedNumberOfChunks: 1,
			expectedPointsPerChunk: 10,
		},
		{
			dataColDuration:        8 * time.Hour,
			step:                   time.Hour / 2,
			numberOfSamples:        32,
			expectedNumberOfChunks: 2,
			expectedPointsPerChunk: 16,
		},
	}

	for _, tt := range tc {
		t.Run(fmt.Sprintf("dataColDurationMs:%v,step:%v,numberOfSamples:%v", tt.dataColDuration.Hours(), tt.step.Seconds(), tt.numberOfSamples), func(t *testing.T) {
			st := teststorage.New(t)
			t.Cleanup(func() { _ = st.Close() })

			bkt, err := filesystem.NewBucket(t.TempDir())
			require.NoError(t, err)
			t.Cleanup(func() { _ = bkt.Close() })

			app := st.Appender(ctx)
			seriesHash := make(map[uint64]struct{})
			for i := 0; i != 1_000; i++ {
				for j := 0; j < tt.numberOfSamples; j++ {
					lbls := labels.FromStrings("__name__", "foo", "bar", fmt.Sprintf("%d", 2*i))
					seriesHash[lbls.Hash()] = struct{}{}
					_, err := app.Append(0, lbls, (tt.step * time.Duration(j)).Milliseconds(), float64(i))
					require.NoError(t, err)
				}
			}

			require.NoError(t, app.Commit())

			h := st.Head()
			shards, err := ConvertTSDBBlock(ctx, bkt, h.MinTime(), h.MaxTime(), []Convertible{h}, WithColDuration(tt.dataColDuration), WithSortBy(labels.MetricName))
			require.NoError(t, err)
			require.Equal(t, 1, shards)

			labelsFileName := schema.LabelsPfileNameForShard(DefaultConvertOpts.name, 0)
			chunksFileName := schema.ChunksPfileNameForShard(DefaultConvertOpts.name, 0)
			lf, cf, err := openParquetFiles(ctx, bkt, labelsFileName, chunksFileName)
			require.NoError(t, err)
			series, chunks, err := readSeries(t, lf, cf)
			require.NoError(t, err)
			require.Equal(t, st.DB.Head().NumSeries(), uint64(len(series)))
			require.Equal(t, st.DB.Head().NumSeries(), uint64(len(chunks)))

			// Make sure the chunk page bounds are empty
			for _, ci := range cf.ColumnIndexes() {
				for _, value := range append(ci.MinValues, ci.MaxValues...) {
					require.Empty(t, value)
				}
			}

			// Make sure labels pages bounds are populated
			for _, ci := range lf.ColumnIndexes() {
				for _, value := range append(ci.MinValues, ci.MaxValues...) {
					require.NotEmpty(t, value)
				}
			}

			for i, s := range series {
				require.Contains(t, seriesHash, s.Hash())
				require.Len(t, chunks[i], tt.expectedNumberOfChunks)
				totalSamples := 0
				for _, c := range chunks[i] {
					require.Equal(t, tt.expectedPointsPerChunk, c.Chunk.NumSamples())
					totalSamples += c.Chunk.NumSamples()
				}
				require.Equal(t, tt.numberOfSamples, totalSamples)
			}
		})
	}
}

func Test_CreateParquetWithReducedTimestampSamples(t *testing.T) {
	ctx := context.Background()
	st := teststorage.New(t)
	t.Cleanup(func() { _ = st.Close() })

	bkt, err := filesystem.NewBucket(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = bkt.Close() })

	app := st.Appender(ctx)

	// 240 samples * 30 seconds = 2 hours
	step := (30 * time.Second).Milliseconds()
	for i := 0; i < 240; i++ {
		_, err := app.Append(0, labels.FromStrings("__name__", "test", "foo", "bar"), int64(i)*step, float64(i))
		require.NoError(t, err)
	}

	require.NoError(t, app.Commit())

	h := st.Head()
	mint, maxt := (time.Minute * 30).Milliseconds(), (time.Minute*90).Milliseconds()-1

	datColDuration := time.Minute * 10
	shards, err := ConvertTSDBBlock(
		ctx, bkt, mint, maxt,
		[]Convertible{h},
		WithColDuration(datColDuration),
		WithSortBy(labels.MetricName),
		WithColumnPageBuffers(parquet.NewFileBufferPool(t.TempDir(), "buffers.*")),
	)

	require.NoError(t, err)
	require.Equal(t, 1, shards)

	labelsFileName := schema.LabelsPfileNameForShard(DefaultConvertOpts.name, 0)
	chunksFileName := schema.ChunksPfileNameForShard(DefaultConvertOpts.name, 0)
	lf, cf, err := openParquetFiles(ctx, bkt, labelsFileName, chunksFileName)
	require.NoError(t, err)

	// Check metadatas
	for _, file := range []*parquet.File{lf, cf} {
		require.Equal(t, schema.MetadataToMap(file.Metadata().KeyValueMetadata)[schema.MinTMd], strconv.FormatInt(mint, 10))
		require.Equal(t, schema.MetadataToMap(file.Metadata().KeyValueMetadata)[schema.MaxTMd], strconv.FormatInt(maxt, 10))
		require.Equal(t, schema.MetadataToMap(file.Metadata().KeyValueMetadata)[schema.DataColSizeMd], strconv.FormatInt(datColDuration.Milliseconds(), 10))
	}

	// 2 labels + col indexes
	require.Len(t, lf.Schema().Columns(), 3)
	// 6 data cols with 10 min duration
	require.Len(t, cf.Schema().Columns(), 6)
	series, chunks, err := readSeries(t, lf, cf)

	require.NoError(t, err)
	require.Len(t, series, 1)
	require.Len(t, chunks, 1)
	require.Equal(t, labels.FromStrings("__name__", "test", "foo", "bar").Hash(), series[0].Hash())

	totalSamples := 0
	for _, c := range chunks[0] {
		totalSamples += c.Chunk.NumSamples()
		require.LessOrEqual(t, c.MaxTime, maxt)
		require.GreaterOrEqual(t, c.MinTime, mint)
	}
	require.Equal(t, 120, totalSamples)
}

func Test_BlockHasOnlySomeSeriesInConvertTime(t *testing.T) {
	ctx := context.Background()
	st := teststorage.New(t)
	t.Cleanup(func() { _ = st.Close() })

	bkt, err := filesystem.NewBucket(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = bkt.Close() })

	app := st.Appender(ctx)

	// one series before convert time
	_, err = app.Append(0, labels.FromStrings(
		labels.MetricName, fmt.Sprintf("metric_%d", 0),
		"i", fmt.Sprintf("%v", 0),
	), 0, float64(0))
	require.NoError(t, err)

	// one series inside convert time
	_, err = app.Append(0, labels.FromStrings(
		labels.MetricName, fmt.Sprintf("metric_%d", 1),
		"i", fmt.Sprintf("%v", 1),
	), 11, float64(0))
	require.NoError(t, err)

	// one series after convert time
	_, err = app.Append(0, labels.FromStrings(
		labels.MetricName, fmt.Sprintf("metric_%d", 2),
		"i", fmt.Sprintf("%v", 2),
	), 21, float64(0))
	require.NoError(t, err)

	// many series inside convert time
	for i := 0; i != 240; i++ {
		_, err = app.Append(0, labels.FromStrings(
			labels.MetricName, fmt.Sprintf("metric_%d", i+3),
			"i", fmt.Sprintf("%v", 1),
		), 11, float64(0))
		require.NoError(t, err)
	}

	require.NoError(t, app.Commit())

	h := st.Head()

	shards, err := ConvertTSDBBlock(
		ctx,
		bkt,
		10,
		20-1,
		[]Convertible{h},
		WithColDuration(time.Millisecond*10),
		WithColumnPageBuffers(parquet.NewFileBufferPool(t.TempDir(), "buffers.*")),
	)
	require.NoError(t, err)
	require.Equal(t, 1, shards)

	labelsFileName := schema.LabelsPfileNameForShard(DefaultConvertOpts.name, 0)
	chunksFileName := schema.ChunksPfileNameForShard(DefaultConvertOpts.name, 0)
	lf, cf, err := openParquetFiles(ctx, bkt, labelsFileName, chunksFileName)
	require.NoError(t, err)

	series, _, err := readSeries(t, lf, cf)
	require.NoError(t, err)
	require.Len(t, series, 241)
}

func Test_SortedLabels(t *testing.T) {
	ctx := context.Background()

	bkt, err := filesystem.NewBucket(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = bkt.Close() })

	numberOfBLocks := 10
	totalSeries := 0
	storages := make([]*teststorage.TestStorage, numberOfBLocks)
	heads := make([]Convertible, numberOfBLocks)
	for i := 0; i < numberOfBLocks; i++ {
		storages[i] = teststorage.New(t)
		t.Cleanup(func() { _ = storages[i].Close() })
		heads[i] = storages[i].Head()
	}

	for si, s := range storages {
		app := s.Appender(ctx)
		// Some very random series
		for i := 0; i < 240; i++ {
			_, err := app.Append(0, labels.FromStrings(
				labels.MetricName, fmt.Sprintf("%v", rand.Int31()),
				"type", fmt.Sprintf("block_%v", si),
				"zzz", fmt.Sprintf("%v", rand.Int31()),
				"i", fmt.Sprintf("%v", i),
			), 10, float64(i))
			require.NoError(t, err)
			totalSeries++
		}
		require.NoError(t, app.Commit())
	}

	// Lets create some common series on both blocks
	name := "duplicated"
	zzz := "duplicated"
	for i := 0; i < 240; i++ {
		for j := 0; j < 2; j++ {
			app := storages[int(rand.Int31())%len(storages)].Appender(ctx)
			lbls := labels.FromStrings(
				labels.MetricName, name,
				"type", "duplicated",
				"zzz", zzz,
				"i", fmt.Sprintf("%v", i),
			)

			_, err := app.Append(0, lbls, int64(j), float64(i))
			require.NoError(t, err)
			require.NoError(t, app.Commit())
		}
		totalSeries++
	}

	// lets sort first by `zzz` as its not the default sorting on TSDB
	shards, err := ConvertTSDBBlock(
		ctx,
		bkt,
		0,
		time.Minute.Milliseconds(),
		heads,
		WithColDuration(time.Minute*10),
		WithSortBy("zzz", labels.MetricName),
		WithColumnPageBuffers(parquet.NewFileBufferPool(t.TempDir(), "buffers.*")),
	)
	require.NoError(t, err)
	require.Equal(t, 1, shards)

	labelsFileName := schema.LabelsPfileNameForShard(DefaultConvertOpts.name, 0)
	chunksFileName := schema.ChunksPfileNameForShard(DefaultConvertOpts.name, 0)
	lf, cf, err := openParquetFiles(ctx, bkt, labelsFileName, chunksFileName)
	require.NoError(t, err)

	series, chunks, err := readSeries(t, lf, cf)
	require.NoError(t, err)
	require.Equal(t, len(series), totalSeries, "series count mismatch")

	for i := 0; i < len(series)-1; i++ {
		require.LessOrEqual(t, series[i].Get("zzz"), series[i+1].Get("zzz"))
		if series[i].Get("zzz") == series[i+1].Get("zzz") {
			require.LessOrEqual(t, series[i].Get(labels.MetricName), series[i+1].Get(labels.MetricName))
		}
		require.Len(t, chunks[i], 1)
		st := chunks[i][0].Chunk.Iterator(nil)
		expectedSamples := 1
		if series[i].Get("type") == "duplicated" {
			expectedSamples++
		}
		totalSamples := 0

		for st.Next() != chunkenc.ValNone {
			totalSamples++
		}

		require.Equal(t, expectedSamples, totalSamples, "series", series[i])

		require.NoError(t, st.Err())
	}
}

func openParquetFiles(ctx context.Context, bkt objstore.Bucket, labelsFileName, chunksFileName string) (*parquet.File, *parquet.File, error) {
	labelsAttr, err := bkt.Attributes(ctx, labelsFileName)
	if err != nil {
		return nil, nil, err
	}
	labelsFile, err := parquet.OpenFile(util.NewBucketReadAt(ctx, labelsFileName, bkt), labelsAttr.Size)
	if err != nil {
		return nil, nil, err
	}

	chunksAttr, err := bkt.Attributes(ctx, chunksFileName)
	if err != nil {
		return nil, nil, err
	}

	chunksFile, err := parquet.OpenFile(util.NewBucketReadAt(ctx, chunksFileName, bkt), chunksAttr.Size)
	if err != nil {
		return nil, nil, err
	}
	return labelsFile, chunksFile, nil
}

func readSeries(t *testing.T, labelsFile, chunksFile *parquet.File) ([]labels.Labels, [][]chunks.Meta, error) {
	lr := parquet.NewGenericReader[any](labelsFile)
	cr := parquet.NewGenericReader[any](chunksFile)

	labelsBuff := make([]parquet.Row, 100)
	chunksBuff := make([]parquet.Row, 100)
	rLbls := make([]labels.Labels, 0, 100)
	rChunks := make([][]chunks.Meta, 0, 100)
	dec := schema.NewPrometheusParquetChunksDecoder(chunkenc.NewPool())
	for {
		nl, _ := lr.ReadRows(labelsBuff)
		if nl == 0 {
			break
		}

		nc, _ := cr.ReadRows(chunksBuff)

		if nc != nl {
			return nil, nil, fmt.Errorf("unexpected number of rows read: %d, expected %d", nl, nc)
		}
		s, _, err := rowToSeries(t, lr.Schema(), dec, labelsBuff[:nl])
		if err != nil {
			return nil, nil, err
		}
		_, c, err := rowToSeries(t, cr.Schema(), dec, chunksBuff[:nl])
		if err != nil {
			return nil, nil, err
		}
		rLbls = append(rLbls, s...)
		rChunks = append(rChunks, c...)
	}

	return rLbls, rChunks, nil
}

func rowToSeries(t *testing.T, s *parquet.Schema, dec *schema.PrometheusParquetChunksDecoder, rows []parquet.Row) ([]labels.Labels, [][]chunks.Meta, error) {
	cols := s.Columns()
	b := labels.NewScratchBuilder(10)
	series := make([]labels.Labels, len(rows))
	chunksMetas := make([][]chunks.Meta, len(rows))
	expectedLblsIdxs := []int{}
	foundLblsIdxs := []int{}
	for i, row := range rows {
		b.Reset()
		expectedLblsIdxs = expectedLblsIdxs[:0]
		foundLblsIdxs = foundLblsIdxs[:0]
		for colIdx, colVal := range row {
			col := cols[colIdx][0]
			label, ok := schema.ExtractLabelFromColumn(col)
			if ok {
				b.Add(label, colVal.String())
				foundLblsIdxs = append(foundLblsIdxs, colIdx)
			}

			if schema.IsDataColumn(col) && dec != nil {
				c, err := dec.Decode(colVal.ByteArray(), 0, math.MaxInt64)
				if err != nil {
					return nil, nil, err
				}
				chunksMetas[i] = append(chunksMetas[i], c...)
			}

			if col == schema.ColIndexes {
				lblIdx, err := schema.DecodeUintSlice(colVal.ByteArray())
				if err != nil {
					return nil, nil, err
				}
				expectedLblsIdxs = lblIdx
			}
		}
		series[i] = b.Labels()
		slices.Sort(foundLblsIdxs)
		require.Equal(t, expectedLblsIdxs, foundLblsIdxs)
	}

	return series, chunksMetas, nil
}
