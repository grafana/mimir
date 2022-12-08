// Copyright 2017 The Prometheus Authors
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

package tsdb

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	prom_testutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/prometheus/prometheus/tsdb/tsdbutil"
)

const (
	defaultLabelName  = "labelName"
	defaultLabelValue = "labelValue"
)

var DefaultBlockDuration = 2 * time.Hour.Milliseconds()

// query runs a matcher query against the querier and fully expands its data.
func query(t testing.TB, q storage.Querier, matchers ...*labels.Matcher) map[string][]tsdbutil.Sample {
	ss := q.Select(false, nil, matchers...)
	defer func() {
		require.NoError(t, q.Close())
	}()

	result := map[string][]tsdbutil.Sample{}
	for ss.Next() {
		series := ss.At()

		samples := []tsdbutil.Sample{}
		it := series.Iterator()
		for it.Next() {
			t, v := it.At()
			samples = append(samples, sample{t: t, v: v})
		}
		require.NoError(t, it.Err())

		if len(samples) == 0 {
			continue
		}

		name := series.Labels().String()
		result[name] = samples
	}
	require.NoError(t, ss.Err())
	require.Equal(t, 0, len(ss.Warnings()))

	return result
}

// genSeries generates series with a given number of labels and values.
func genSeries(totalSeries, labelCount int, mint, maxt int64) []storage.Series {
	if totalSeries == 0 || labelCount == 0 {
		return nil
	}

	series := make([]storage.Series, totalSeries)

	for i := 0; i < totalSeries; i++ {
		lbls := make(map[string]string, labelCount)
		lbls[defaultLabelName] = strconv.Itoa(i)
		for j := 1; len(lbls) < labelCount; j++ {
			lbls[defaultLabelName+strconv.Itoa(j)] = defaultLabelValue + strconv.Itoa(j)
		}
		samples := make([]tsdbutil.Sample, 0, maxt-mint+1)
		for t := mint; t < maxt; t++ {
			samples = append(samples, sample{t: t, v: rand.Float64()})
		}
		series[i] = storage.NewListSeries(labels.FromMap(lbls), samples)
	}
	return series
}

func newTestHead(t testing.TB, chunkRange int64, _, _ bool) *Head {
	opts := DefaultHeadOptions()
	opts.ChunkRange = chunkRange

	h, err := NewHead(nil, nil, opts, nil)
	require.NoError(t, err)

	return h
}

func BenchmarkCreateSeries(b *testing.B) {
	series := genSeries(b.N, 10, 0, 0)
	h := newTestHead(b, 10000, false, false)
	defer func() {
		require.NoError(b, h.Close())
	}()

	b.ReportAllocs()
	b.ResetTimer()

	for _, s := range series {
		h.getOrCreate(s.Labels().Hash(), s.Labels())
	}
}

// TestHead_HighConcurrencyReadAndWrite generates 1000 series with a step of 15s and fills a whole block with samples,
// this means in total it generates 4000 chunks because with a step of 15s there are 4 chunks per block per series.
// While appending the samples to the head it concurrently queries them from multiple go routines and verifies that the
// returned results are correct.
func TestHead_HighConcurrencyReadAndWrite(t *testing.T) {
	head := newTestHead(t, DefaultBlockDuration, false, false)
	defer func() {
		require.NoError(t, head.Close())
	}()

	seriesCnt := 1000
	readConcurrency := 2
	writeConcurrency := 10
	startTs := uint64(DefaultBlockDuration) // start at the second block relative to the unix epoch.
	qryRange := uint64(5 * time.Minute.Milliseconds())
	step := uint64(15 * time.Second / time.Millisecond)
	endTs := startTs + uint64(DefaultBlockDuration)

	labelSets := make([]labels.Labels, seriesCnt)
	for i := 0; i < seriesCnt; i++ {
		labelSets[i] = labels.FromStrings("seriesId", strconv.Itoa(i))
	}

	head.Init(0)

	g, ctx := errgroup.WithContext(context.Background())
	whileNotCanceled := func(f func() (bool, error)) error {
		for ctx.Err() == nil {
			cont, err := f()
			if err != nil {
				return err
			}
			if !cont {
				return nil
			}
		}
		return nil
	}

	// Create one channel for each write worker, the channels will be used by the coordinator
	// go routine to coordinate which timestamps each write worker has to write.
	writerTsCh := make([]chan uint64, writeConcurrency)
	for writerTsChIdx := range writerTsCh {
		writerTsCh[writerTsChIdx] = make(chan uint64)
	}

	// workerReadyWg is used to synchronize the start of the test,
	// we only start the test once all workers signal that they're ready.
	var workerReadyWg sync.WaitGroup
	workerReadyWg.Add(writeConcurrency + readConcurrency)

	// Start the write workers.
	for workerID := 0; workerID < writeConcurrency; workerID++ {
		// Create copy of workerID to be used by worker routine.
		workerID := workerID

		g.Go(func() error {
			// The label sets which this worker will write.
			workerLabelSets := labelSets[(seriesCnt/writeConcurrency)*workerID : (seriesCnt/writeConcurrency)*(workerID+1)]

			// Signal that this worker is ready.
			workerReadyWg.Done()

			return whileNotCanceled(func() (bool, error) {
				ts, ok := <-writerTsCh[workerID]
				if !ok {
					return false, nil
				}

				app := head.Appender(ctx)
				for i := 0; i < len(workerLabelSets); i++ {
					// We also use the timestamp as the sample value.
					_, err := app.Append(0, workerLabelSets[i], int64(ts), float64(ts))
					if err != nil {
						return false, fmt.Errorf("Error when appending to head: %w", err)
					}
				}

				return true, app.Commit()
			})
		})
	}

	// queryHead is a helper to query the head for a given time range and labelset.
	queryHead := func(mint, maxt uint64, label labels.Label) (map[string][]tsdbutil.Sample, error) {
		q, err := tsdb.NewBlockQuerier(head, int64(mint), int64(maxt))
		if err != nil {
			return nil, err
		}
		return query(t, q, labels.MustNewMatcher(labels.MatchEqual, label.Name, label.Value)), nil
	}

	// readerTsCh will be used by the coordinator go routine to coordinate which timestamps the reader should read.
	readerTsCh := make(chan uint64)

	// Start the read workers.
	for workerID := 0; workerID < readConcurrency; workerID++ {
		// Create copy of threadID to be used by worker routine.
		workerID := workerID

		g.Go(func() error {
			querySeriesRef := (seriesCnt / readConcurrency) * workerID

			// Signal that this worker is ready.
			workerReadyWg.Done()

			return whileNotCanceled(func() (bool, error) {
				ts, ok := <-readerTsCh
				if !ok {
					return false, nil
				}

				querySeriesRef = (querySeriesRef + 1) % seriesCnt
				lbls := labelSets[querySeriesRef]
				samples, err := queryHead(ts-qryRange, ts, lbls[0])
				if err != nil {
					return false, err
				}

				if len(samples) != 1 {
					return false, fmt.Errorf("expected 1 sample, got %d", len(samples))
				}

				series := lbls.String()
				expectSampleCnt := qryRange/step + 1
				if expectSampleCnt != uint64(len(samples[series])) {
					return false, fmt.Errorf("expected %d samples, got %d", expectSampleCnt, len(samples[series]))
				}

				for sampleIdx, sample := range samples[series] {
					expectedValue := ts - qryRange + (uint64(sampleIdx) * step)
					if sample.T() != int64(expectedValue) {
						return false, fmt.Errorf("expected sample %d to have ts %d, got %d", sampleIdx, expectedValue, sample.T())
					}
					if sample.V() != float64(expectedValue) {
						return false, fmt.Errorf("expected sample %d to have value %d, got %f", sampleIdx, expectedValue, sample.V())
					}
				}

				return true, nil
			})
		})
	}

	// Start the coordinator go routine.
	g.Go(func() error {
		currTs := startTs

		defer func() {
			// End of the test, close all channels to stop the workers.
			for _, ch := range writerTsCh {
				close(ch)
			}
			close(readerTsCh)
		}()

		// Wait until all workers are ready to start the test.
		workerReadyWg.Wait()
		return whileNotCanceled(func() (bool, error) {
			// Send the current timestamp to each of the writers.
			for _, ch := range writerTsCh {
				select {
				case ch <- currTs:
				case <-ctx.Done():
					return false, nil
				}
			}

			// Once data for at least <qryRange> has been ingested, send the current timestamp to the readers.
			if currTs > startTs+qryRange {
				select {
				case readerTsCh <- currTs - step:
				case <-ctx.Done():
					return false, nil
				}
			}

			currTs += step
			if currTs > endTs {
				return false, nil
			}

			return true, nil
		})
	})

	require.NoError(t, g.Wait())
}

func TestHead_ActiveAppenders(t *testing.T) {
	head := newTestHead(t, 1000, false, false)
	defer head.Close()

	require.NoError(t, head.Init(0))

	// First rollback with no samples.
	app := head.Appender(context.Background())
	require.Equal(t, 1.0, prom_testutil.ToFloat64(head.metrics.activeAppenders))
	require.NoError(t, app.Rollback())
	require.Equal(t, 0.0, prom_testutil.ToFloat64(head.metrics.activeAppenders))

	// Then commit with no samples.
	app = head.Appender(context.Background())
	require.NoError(t, app.Commit())
	require.Equal(t, 0.0, prom_testutil.ToFloat64(head.metrics.activeAppenders))

	// Now rollback with one sample.
	app = head.Appender(context.Background())
	_, err := app.Append(0, labels.FromStrings("foo", "bar"), 100, 1)
	require.NoError(t, err)
	require.Equal(t, 1.0, prom_testutil.ToFloat64(head.metrics.activeAppenders))
	require.NoError(t, app.Rollback())
	require.Equal(t, 0.0, prom_testutil.ToFloat64(head.metrics.activeAppenders))

	// Now commit with one sample.
	app = head.Appender(context.Background())
	_, err = app.Append(0, labels.FromStrings("foo", "bar"), 100, 1)
	require.NoError(t, err)
	require.NoError(t, app.Commit())
	require.Equal(t, 0.0, prom_testutil.ToFloat64(head.metrics.activeAppenders))
}

func TestHead_Truncate(t *testing.T) {
	h := newTestHead(t, 1000, false, false)
	defer func() {
		require.NoError(t, h.Close())
	}()

	h.initTime(0)

	s1, _, _ := h.getOrCreate(1, labels.FromStrings("a", "1", "b", "1"))
	s2, _, _ := h.getOrCreate(2, labels.FromStrings("a", "2", "b", "1"))
	s3, _, _ := h.getOrCreate(3, labels.FromStrings("a", "1", "b", "2"))
	s4, _, _ := h.getOrCreate(4, labels.FromStrings("a", "2", "b", "2", "c", "1"))

	s1.finishedChunks = []*memChunk{
		{minTime: 0, maxTime: 999},
		{minTime: 1000, maxTime: 1999},
		{minTime: 2000, maxTime: 2999},
	}
	s2.finishedChunks = []*memChunk{
		{minTime: 1000, maxTime: 1999},
		{minTime: 2000, maxTime: 2999},
		{minTime: 3000, maxTime: 3999},
	}
	s3.finishedChunks = []*memChunk{
		{minTime: 0, maxTime: 999},
		{minTime: 1000, maxTime: 1999},
	}
	s4.finishedChunks = []*memChunk{}

	// Truncation need not be aligned.
	require.NoError(t, h.Truncate(1))

	require.NoError(t, h.Truncate(2000))

	require.Equal(t, []*memChunk{
		{minTime: 2000, maxTime: 2999},
	}, h.series.getByID(s1.ref).finishedChunks)

	require.Equal(t, []*memChunk{
		{minTime: 2000, maxTime: 2999},
		{minTime: 3000, maxTime: 3999},
	}, h.series.getByID(s2.ref).finishedChunks)

	require.Nil(t, h.series.getByID(s3.ref))
	require.Nil(t, h.series.getByID(s4.ref))

	postingsA1, _ := index.ExpandPostings(h.postings.Get("a", "1"))
	postingsA2, _ := index.ExpandPostings(h.postings.Get("a", "2"))
	postingsB1, _ := index.ExpandPostings(h.postings.Get("b", "1"))
	postingsB2, _ := index.ExpandPostings(h.postings.Get("b", "2"))
	postingsC1, _ := index.ExpandPostings(h.postings.Get("c", "1"))
	postingsAll, _ := index.ExpandPostings(h.postings.Get("", ""))

	require.Equal(t, []storage.SeriesRef{storage.SeriesRef(s1.ref)}, postingsA1)
	require.Equal(t, []storage.SeriesRef{storage.SeriesRef(s2.ref)}, postingsA2)
	require.Equal(t, []storage.SeriesRef{storage.SeriesRef(s1.ref), storage.SeriesRef(s2.ref)}, postingsB1)
	require.Equal(t, []storage.SeriesRef{storage.SeriesRef(s1.ref), storage.SeriesRef(s2.ref)}, postingsAll)
	require.Nil(t, postingsB2)
	require.Nil(t, postingsC1)

	iter := h.postings.Symbols()
	symbols := []string{}
	for iter.Next() {
		symbols = append(symbols, iter.At())
	}
	require.Equal(t,
		[]string{"" /* from 'all' postings list */, "1", "2", "a", "b"},
		symbols)

	values := map[string]map[string]struct{}{}
	for _, name := range h.postings.LabelNames() {
		ss, ok := values[name]
		if !ok {
			ss = map[string]struct{}{}
			values[name] = ss
		}
		for _, value := range h.postings.LabelValues(name) {
			ss[value] = struct{}{}
		}
	}
	require.Equal(t, map[string]map[string]struct{}{
		"a": {"1": struct{}{}, "2": struct{}{}},
		"b": {"1": struct{}{}},
	}, values)
}

func TestComputeChunkEndTime(t *testing.T) {
	cases := []struct {
		start, cur, max int64
		res             int64
	}{
		{
			start: 0,
			cur:   250,
			max:   1000,
			res:   1000,
		},
		{
			start: 100,
			cur:   200,
			max:   1000,
			res:   550,
		},
		// Case where we fit floored 0 chunks. Must catch division by 0
		// and default to maximum time.
		{
			start: 0,
			cur:   500,
			max:   1000,
			res:   1000,
		},
		// Catch division by zero for cur == start. Strictly not a possible case.
		{
			start: 100,
			cur:   100,
			max:   1000,
			res:   104,
		},
	}

	for _, c := range cases {
		got := computeChunkEndTime(c.start, c.cur, c.max)
		if got != c.res {
			t.Errorf("expected %d for (start: %d, cur: %d, max: %d), got %d", c.res, c.start, c.cur, c.max, got)
		}
	}
}

func TestMemSeries_append(t *testing.T) {
	const chunkRange = 500

	lbls := labels.Labels{}
	s := newMemSeries(lbls, 1, lbls.Hash(), 0, defaultIsolationDisabled)

	// Add first two samples at the very end of a chunk range and the next two
	// on and after it.
	// New chunk must correctly be cut at 1000.
	ok, chunkCreated := s.append(998, 1, 0, chunkRange)
	require.True(t, ok, "append failed")
	require.True(t, chunkCreated, "first sample created chunk")

	ok, chunkCreated = s.append(999, 2, 0, chunkRange)
	require.True(t, ok, "append failed")
	require.False(t, chunkCreated, "second sample should use same chunk")

	ok, chunkCreated = s.append(1000, 3, 0, chunkRange)
	require.True(t, ok, "append failed")
	require.True(t, chunkCreated, "expected new chunk on boundary")

	ok, chunkCreated = s.append(1001, 4, 0, chunkRange)
	require.True(t, ok, "append failed")
	require.False(t, chunkCreated, "second sample should use same chunk")

	require.Equal(t, 1, len(s.finishedChunks), "there should be only 1 mmapped chunk")
	require.Equal(t, int64(998), s.finishedChunks[0].minTime, "wrong chunk range")
	require.Equal(t, int64(999), s.finishedChunks[0].maxTime, "wrong chunk range")
	require.Equal(t, int64(1000), s.headChunk.minTime, "wrong chunk range")
	require.Equal(t, int64(1001), s.headChunk.maxTime, "wrong chunk range")

	// Fill the range [1000,2000) with many samples. Intermediate chunks should be cut
	// at approximately 120 samples per chunk.
	for i := 1; i < 1000; i++ {
		ok, _ := s.append(1001+int64(i), float64(i), 0, chunkRange)
		require.True(t, ok, "append failed")
	}

	require.Greater(t, len(s.finishedChunks)+1, 7, "expected intermediate chunks")

	// All chunks but the first and last should now be moderately full.
	for i, c := range s.finishedChunks[1:] {
		chk := c.chunk
		require.Greater(t, chk.NumSamples(), 100, "unexpected small chunk %d of length %d", i, chk.NumSamples())
	}
}

func TestGCChunkAccess(t *testing.T) {
	// Put a chunk, select it. GC it and then access it.
	const chunkRange = 1000
	h := newTestHead(t, chunkRange, false, false)
	defer func() {
		require.NoError(t, h.Close())
	}()

	h.initTime(0)

	s, _, _ := h.getOrCreate(1, labels.FromStrings("a", "1"))

	// Appending 2 samples for the first chunk.
	ok, chunkCreated := s.append(0, 0, 0, chunkRange)
	require.True(t, ok, "series append failed")
	require.True(t, chunkCreated, "chunks was not created")
	ok, chunkCreated = s.append(999, 999, 0, chunkRange)
	require.True(t, ok, "series append failed")
	require.False(t, chunkCreated, "chunks was created")

	// A new chunks should be created here as it's beyond the chunk range.
	ok, chunkCreated = s.append(1000, 1000, 0, chunkRange)
	require.True(t, ok, "series append failed")
	require.True(t, chunkCreated, "chunks was not created")
	ok, chunkCreated = s.append(1999, 1999, 0, chunkRange)
	require.True(t, ok, "series append failed")
	require.False(t, chunkCreated, "chunks was created")

	idx := h.indexRange(0, 1500)
	var (
		lset   labels.Labels
		chunks []chunks.Meta
	)
	require.NoError(t, idx.Series(1, &lset, &chunks))

	require.Equal(t, labels.FromStrings("a", "1"), lset)
	require.Equal(t, 2, len(chunks))

	cr, err := h.chunksRange(0, 1500, nil)
	require.NoError(t, err)
	_, err = cr.Chunk(chunks[0])
	require.NoError(t, err)
	_, err = cr.Chunk(chunks[1])
	require.NoError(t, err)

	require.NoError(t, h.Truncate(1500)) // Remove a chunk.

	_, err = cr.Chunk(chunks[0])
	require.Equal(t, storage.ErrNotFound, err)
	_, err = cr.Chunk(chunks[1])
	require.NoError(t, err)
}

func TestGCSeriesAccess(t *testing.T) {
	// Put a series, select it. GC it and then access it.
	const chunkRange = 1000
	h := newTestHead(t, chunkRange, false, false)
	defer func() {
		require.NoError(t, h.Close())
	}()

	h.initTime(0)

	s, _, _ := h.getOrCreate(1, labels.FromStrings("a", "1"))

	// Appending 2 samples for the first chunk.
	ok, chunkCreated := s.append(0, 0, 0, chunkRange)
	require.True(t, ok, "series append failed")
	require.True(t, chunkCreated, "chunks was not created")
	ok, chunkCreated = s.append(999, 999, 0, chunkRange)
	require.True(t, ok, "series append failed")
	require.False(t, chunkCreated, "chunks was created")

	// A new chunks should be created here as it's beyond the chunk range.
	ok, chunkCreated = s.append(1000, 1000, 0, chunkRange)
	require.True(t, ok, "series append failed")
	require.True(t, chunkCreated, "chunks was not created")
	ok, chunkCreated = s.append(1999, 1999, 0, chunkRange)
	require.True(t, ok, "series append failed")
	require.False(t, chunkCreated, "chunks was created")

	idx := h.indexRange(0, 2000)
	var (
		lset   labels.Labels
		chunks []chunks.Meta
	)
	require.NoError(t, idx.Series(1, &lset, &chunks))

	require.Equal(t, labels.FromStrings("a", "1"), lset)
	require.Equal(t, 2, len(chunks))

	cr, err := h.chunksRange(0, 2000, nil)
	require.NoError(t, err)
	_, err = cr.Chunk(chunks[0])
	require.NoError(t, err)
	_, err = cr.Chunk(chunks[1])
	require.NoError(t, err)

	require.NoError(t, h.Truncate(2000)) // Remove the series.

	require.Equal(t, (*memSeries)(nil), h.series.getByID(1))

	_, err = cr.Chunk(chunks[0])
	require.Equal(t, storage.ErrNotFound, err)
	_, err = cr.Chunk(chunks[1])
	require.Equal(t, storage.ErrNotFound, err)
}

func TestUncommittedSamplesNotLostOnTruncate(t *testing.T) {
	h := newTestHead(t, 1000, false, false)
	defer func() {
		require.NoError(t, h.Close())
	}()

	h.initTime(0)

	app := h.appender()
	lset := labels.FromStrings("a", "1")
	_, err := app.Append(0, lset, 2100, 1)
	require.NoError(t, err)

	require.NoError(t, h.Truncate(2000))
	require.NotNil(t, h.series.getByHash(lset.Hash(), lset), "series should not have been garbage collected")

	require.NoError(t, app.Commit())

	q, err := tsdb.NewBlockQuerier(h, 1500, 2500)
	require.NoError(t, err)
	defer q.Close()

	ss := q.Select(false, nil, labels.MustNewMatcher(labels.MatchEqual, "a", "1"))
	require.Equal(t, true, ss.Next())
	for ss.Next() {
	}
	require.NoError(t, ss.Err())
	require.Equal(t, 0, len(ss.Warnings()))
}

func TestRemoveSeriesAfterRollbackAndTruncate(t *testing.T) {
	h := newTestHead(t, 1000, false, false)
	defer func() {
		require.NoError(t, h.Close())
	}()

	h.initTime(0)

	app := h.appender()
	lset := labels.FromStrings("a", "1")
	_, err := app.Append(0, lset, 2100, 1)
	require.NoError(t, err)

	require.NoError(t, h.Truncate(2000))
	require.NotNil(t, h.series.getByHash(lset.Hash(), lset), "series should not have been garbage collected")

	require.NoError(t, app.Rollback())

	q, err := tsdb.NewBlockQuerier(h, 1500, 2500)
	require.NoError(t, err)

	ss := q.Select(false, nil, labels.MustNewMatcher(labels.MatchEqual, "a", "1"))
	require.Equal(t, false, ss.Next())
	require.Equal(t, 0, len(ss.Warnings()))
	require.NoError(t, q.Close())

	// Truncate again, this time the series should be deleted
	require.NoError(t, h.Truncate(2050))
	require.Equal(t, (*memSeries)(nil), h.series.getByHash(lset.Hash(), lset))
}

func TestAddDuplicateLabelName(t *testing.T) {
	h := newTestHead(t, 1000, false, false)
	defer func() {
		require.NoError(t, h.Close())
	}()

	add := func(labels labels.Labels, labelName string) {
		app := h.Appender(context.Background())
		_, err := app.Append(0, labels, 0, 0)
		require.Error(t, err)
		require.Equal(t, fmt.Sprintf(`label name "%s" is not unique: invalid sample`, labelName), err.Error())
	}

	add(labels.FromStrings("a", "c", "a", "b"), "a")
	add(labels.FromStrings("a", "c", "a", "c"), "a")
	add(labels.FromStrings("__name__", "up", "job", "prometheus", "le", "500", "le", "400", "unit", "s"), "le")
}

func TestHeadLabelNamesValuesWithMinMaxRange(t *testing.T) {
	head := newTestHead(t, 1000, false, false)
	defer func() {
		require.NoError(t, head.Close())
	}()

	const (
		firstSeriesTimestamp  int64 = 100
		secondSeriesTimestamp int64 = 200
		lastSeriesTimestamp   int64 = 300
	)
	var (
		seriesTimestamps = []int64{
			firstSeriesTimestamp,
			secondSeriesTimestamp,
			lastSeriesTimestamp,
		}
		expectedLabelNames  = []string{"a", "b", "c"}
		expectedLabelValues = []string{"d", "e", "f"}
	)

	app := head.Appender(context.Background())
	for i, name := range expectedLabelNames {
		_, err := app.Append(0, labels.FromStrings(name, expectedLabelValues[i]), seriesTimestamps[i], 0)
		require.NoError(t, err)
	}
	require.NoError(t, app.Commit())
	require.Equal(t, head.MinTime(), firstSeriesTimestamp)
	require.Equal(t, head.MaxTime(), lastSeriesTimestamp)

	testCases := []struct {
		name           string
		mint           int64
		maxt           int64
		expectedNames  []string
		expectedValues []string
	}{
		{"maxt less than head min", head.MaxTime() - 10, head.MinTime() - 10, []string{}, []string{}},
		{"mint less than head max", head.MaxTime() + 10, head.MinTime() + 10, []string{}, []string{}},
		{"mint and maxt outside head", head.MaxTime() + 10, head.MinTime() - 10, []string{}, []string{}},
		{"mint and maxt within head", head.MaxTime() - 10, head.MinTime() + 10, expectedLabelNames, expectedLabelValues},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			headIdxReader := head.indexRange(tt.mint, tt.maxt)
			actualLabelNames, err := headIdxReader.LabelNames()
			require.NoError(t, err)
			require.Equal(t, tt.expectedNames, actualLabelNames)
			if len(tt.expectedValues) > 0 {
				for i, name := range expectedLabelNames {
					actualLabelValue, err := headIdxReader.SortedLabelValues(name)
					require.NoError(t, err)
					require.Equal(t, []string{tt.expectedValues[i]}, actualLabelValue)
				}
			}
		})
	}
}

func TestHeadLabelValuesWithMatchers(t *testing.T) {
	head := newTestHead(t, 1000, false, false)
	t.Cleanup(func() { require.NoError(t, head.Close()) })

	app := head.Appender(context.Background())
	for i := 0; i < 100; i++ {
		_, err := app.Append(0, labels.FromStrings(
			"tens", fmt.Sprintf("value%d", i/10),
			"unique", fmt.Sprintf("value%d", i),
		), 100, 0)
		require.NoError(t, err)
	}
	require.NoError(t, app.Commit())

	testCases := []struct {
		name           string
		labelName      string
		matchers       []*labels.Matcher
		expectedValues []string
	}{
		{
			name:           "get tens based on unique id",
			labelName:      "tens",
			matchers:       []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "unique", "value35")},
			expectedValues: []string{"value3"},
		}, {
			name:           "get unique ids based on a ten",
			labelName:      "unique",
			matchers:       []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "tens", "value1")},
			expectedValues: []string{"value10", "value11", "value12", "value13", "value14", "value15", "value16", "value17", "value18", "value19"},
		}, {
			name:           "get tens by pattern matching on unique id",
			labelName:      "tens",
			matchers:       []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "unique", "value[5-7]5")},
			expectedValues: []string{"value5", "value6", "value7"},
		}, {
			name:           "get tens by matching for absence of unique label",
			labelName:      "tens",
			matchers:       []*labels.Matcher{labels.MustNewMatcher(labels.MatchNotEqual, "unique", "")},
			expectedValues: []string{"value0", "value1", "value2", "value3", "value4", "value5", "value6", "value7", "value8", "value9"},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			headIdxReader := head.indexRange(0, 200)

			actualValues, err := headIdxReader.SortedLabelValues(tt.labelName, tt.matchers...)
			require.NoError(t, err)
			require.Equal(t, tt.expectedValues, actualValues)

			actualValues, err = headIdxReader.LabelValues(tt.labelName, tt.matchers...)
			sort.Strings(actualValues)
			require.NoError(t, err)
			require.Equal(t, tt.expectedValues, actualValues)
		})
	}
}

func TestHeadLabelNamesWithMatchers(t *testing.T) {
	head := newTestHead(t, 1000, false, false)
	defer func() {
		require.NoError(t, head.Close())
	}()

	app := head.Appender(context.Background())
	for i := 0; i < 100; i++ {
		_, err := app.Append(0, labels.FromStrings(
			"unique", fmt.Sprintf("value%d", i),
		), 100, 0)
		require.NoError(t, err)

		if i%10 == 0 {
			_, err := app.Append(0, labels.FromStrings(
				"tens", fmt.Sprintf("value%d", i/10),
				"unique", fmt.Sprintf("value%d", i),
			), 100, 0)
			require.NoError(t, err)
		}

		if i%20 == 0 {
			_, err := app.Append(0, labels.FromStrings(
				"tens", fmt.Sprintf("value%d", i/10),
				"twenties", fmt.Sprintf("value%d", i/20),
				"unique", fmt.Sprintf("value%d", i),
			), 100, 0)
			require.NoError(t, err)
		}
	}
	require.NoError(t, app.Commit())

	testCases := []struct {
		name          string
		labelName     string
		matchers      []*labels.Matcher
		expectedNames []string
	}{
		{
			name:          "get with non-empty unique: all",
			matchers:      []*labels.Matcher{labels.MustNewMatcher(labels.MatchNotEqual, "unique", "")},
			expectedNames: []string{"tens", "twenties", "unique"},
		}, {
			name:          "get with unique ending in 1: only unique",
			matchers:      []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "unique", "value.*1")},
			expectedNames: []string{"unique"},
		}, {
			name:          "get with unique = value20: all",
			matchers:      []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "unique", "value20")},
			expectedNames: []string{"tens", "twenties", "unique"},
		}, {
			name:          "get tens = 1: unique & tens",
			matchers:      []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "tens", "value1")},
			expectedNames: []string{"tens", "unique"},
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			headIdxReader := head.indexRange(0, 200)

			actualNames, err := headIdxReader.LabelNames(tt.matchers...)
			require.NoError(t, err)
			require.Equal(t, tt.expectedNames, actualNames)
		})
	}
}

func TestHeadShardedPostings(t *testing.T) {
	head := newTestHead(t, 1000, false, false)
	defer func() {
		require.NoError(t, head.Close())
	}()

	// Append some series.
	app := head.Appender(context.Background())
	for i := 0; i < 100; i++ {
		_, err := app.Append(0, labels.Labels{
			{Name: "unique", Value: fmt.Sprintf("value%d", i)},
			{Name: "const", Value: "1"},
		}, 100, 0)
		require.NoError(t, err)
	}
	require.NoError(t, app.Commit())

	ir := head.indexRange(0, 200)

	// List all postings for a given label value. This is what we expect to get
	// in output from all shards.
	p, err := ir.Postings("const", "1")
	require.NoError(t, err)

	var expected []storage.SeriesRef
	for p.Next() {
		expected = append(expected, p.At())
	}
	require.NoError(t, p.Err())
	require.Greater(t, len(expected), 0)

	// Query the same postings for each shard.
	const shardCount = uint64(4)
	actualShards := make(map[uint64][]storage.SeriesRef)
	actualPostings := make([]storage.SeriesRef, 0, len(expected))

	for shardIndex := uint64(0); shardIndex < shardCount; shardIndex++ {
		p, err = ir.Postings("const", "1")
		require.NoError(t, err)

		p = ir.ShardedPostings(p, shardIndex, shardCount)
		for p.Next() {
			ref := p.At()

			actualShards[shardIndex] = append(actualShards[shardIndex], ref)
			actualPostings = append(actualPostings, ref)
		}
		require.NoError(t, p.Err())
	}

	// We expect the postings merged out of shards is the exact same of the non sharded ones.
	require.ElementsMatch(t, expected, actualPostings)

	// We expect the series in each shard are the expected ones.
	for shardIndex, ids := range actualShards {
		for _, id := range ids {
			var lbls labels.Labels

			require.NoError(t, ir.Series(id, &lbls, nil))
			require.Equal(t, shardIndex, lbls.Hash()%shardCount)
		}
	}
}

func TestErrReuseAppender(t *testing.T) {
	head := newTestHead(t, 1000, false, false)
	defer func() {
		require.NoError(t, head.Close())
	}()

	app := head.Appender(context.Background())
	_, err := app.Append(0, labels.FromStrings("test", "test"), 0, 0)
	require.NoError(t, err)
	require.NoError(t, app.Commit())
	require.Error(t, app.Commit())
	require.Error(t, app.Rollback())

	app = head.Appender(context.Background())
	_, err = app.Append(0, labels.FromStrings("test", "test"), 1, 0)
	require.NoError(t, err)
	require.NoError(t, app.Rollback())
	require.Error(t, app.Rollback())
	require.Error(t, app.Commit())

	app = head.Appender(context.Background())
	_, err = app.Append(0, labels.FromStrings("test", "test"), 2, 0)
	require.NoError(t, err)
	require.NoError(t, app.Commit())
	require.Error(t, app.Rollback())
	require.Error(t, app.Commit())

	app = head.Appender(context.Background())
	_, err = app.Append(0, labels.FromStrings("test", "test"), 3, 0)
	require.NoError(t, err)
	require.NoError(t, app.Rollback())
	require.Error(t, app.Commit())
	require.Error(t, app.Rollback())
}

func TestHeadMintAfterTruncation(t *testing.T) {
	chunkRange := int64(2000)
	head := newTestHead(t, chunkRange, false, false)

	app := head.Appender(context.Background())
	_, err := app.Append(0, labels.FromStrings("a", "b"), 100, 100)
	require.NoError(t, err)
	_, err = app.Append(0, labels.FromStrings("a", "b"), 4000, 200)
	require.NoError(t, err)
	_, err = app.Append(0, labels.FromStrings("a", "b"), 8000, 300)
	require.NoError(t, err)
	require.NoError(t, app.Commit())

	// Truncating outside the appendable window and actual mint being outside
	// appendable window should leave mint at the actual mint.
	require.NoError(t, head.Truncate(3500))
	require.Equal(t, int64(4000), head.MinTime())
	require.Equal(t, int64(4000), head.minValidTime.Load())

	// After truncation outside the appendable window if the actual min time
	// is in the appendable window then we should leave mint at the start of appendable window.
	require.NoError(t, head.Truncate(5000))
	require.Equal(t, head.appendableMinValidTime(), head.MinTime())
	require.Equal(t, head.appendableMinValidTime(), head.minValidTime.Load())

	// If the truncation time is inside the appendable window, then the min time
	// should be the truncation time.
	require.NoError(t, head.Truncate(7500))
	require.Equal(t, int64(7500), head.MinTime())
	require.Equal(t, int64(7500), head.minValidTime.Load())

	require.NoError(t, head.Close())
}

func BenchmarkHeadLabelValuesWithMatchers(b *testing.B) {
	chunkRange := int64(2000)
	head := newTestHead(b, chunkRange, false, false)
	b.Cleanup(func() { require.NoError(b, head.Close()) })

	app := head.Appender(context.Background())

	metricCount := 1000000
	for i := 0; i < metricCount; i++ {
		_, err := app.Append(0, labels.FromStrings(
			"a_unique", fmt.Sprintf("value%d", i),
			"b_tens", fmt.Sprintf("value%d", i/(metricCount/10)),
			"c_ninety", fmt.Sprintf("value%d", i/(metricCount/10)/9), // "0" for the first 90%, then "1"
		), 100, 0)
		require.NoError(b, err)
	}
	require.NoError(b, app.Commit())

	headIdxReader := head.indexRange(0, 200)
	matchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "c_ninety", "value0")}

	b.ResetTimer()
	b.ReportAllocs()

	for benchIdx := 0; benchIdx < b.N; benchIdx++ {
		actualValues, err := headIdxReader.LabelValues("b_tens", matchers...)
		require.NoError(b, err)
		require.Equal(b, 9, len(actualValues))
	}
}

func TestIteratorSeekIntoBuffer(t *testing.T) {
	const chunkRange = 500

	lbls := labels.Labels{}
	s := newMemSeries(lbls, 1, lbls.Hash(), 0, defaultIsolationDisabled)

	for i := 0; i < 7; i++ {
		ok, _ := s.append(int64(i), float64(i), 0, chunkRange)
		require.True(t, ok, "sample append failed")
	}

	it := s.iterator(s.headChunkID(len(s.finishedChunks)), nil)

	// First point.
	ok := it.Seek(0)
	require.True(t, ok)
	ts, val := it.At()
	require.Equal(t, int64(0), ts)
	require.Equal(t, float64(0), val)

	// Advance one point.
	ok = it.Next()
	require.True(t, ok)
	ts, val = it.At()
	require.Equal(t, int64(1), ts)
	require.Equal(t, float64(1), val)

	// Seeking an older timestamp shouldn't cause the iterator to go backwards.
	ok = it.Seek(0)
	require.True(t, ok)
	ts, val = it.At()
	require.Equal(t, int64(1), ts)
	require.Equal(t, float64(1), val)

	// Seek into the buffer.
	ok = it.Seek(3)
	require.True(t, ok)
	ts, val = it.At()
	require.Equal(t, int64(3), ts)
	require.Equal(t, float64(3), val)

	// Iterate through the rest of the buffer.
	for i := 4; i < 7; i++ {
		ok = it.Next()
		require.True(t, ok)
		ts, val = it.At()
		require.Equal(t, int64(i), ts)
		require.Equal(t, float64(i), val)
	}

	// Run out of elements in the iterator.
	ok = it.Next()
	require.False(t, ok)
	ok = it.Seek(7)
	require.False(t, ok)
}

func TestIsQuerierCollidingWithTruncation(t *testing.T) {
	head := newTestHead(t, 10000, false, false)

	var (
		app = head.Appender(context.Background())
		ref = storage.SeriesRef(0)
		err error
	)

	for i := int64(0); i <= 3000; i++ {
		ref, err = app.Append(ref, labels.FromStrings("a", "b"), i, float64(i))
		require.NoError(t, err)
	}
	require.NoError(t, app.Commit())

	// This mocks truncation.
	head.memTruncationInProcess.Store(true)
	head.lastMemoryTruncationTime.Store(2000)

	// Test that IsQuerierValid suggests correct querier ranges.
	cases := []struct {
		mint, maxt                int64 // For the querier.
		expShouldClose, expGetNew bool
		expNewMint                int64
	}{
		{-200, -100, true, false, 0},
		{-200, 300, true, false, 0},
		{100, 1900, true, false, 0},
		{1900, 2200, true, true, 2000},
		{2000, 2500, false, false, 0},
	}

	for _, c := range cases {
		t.Run(fmt.Sprintf("mint=%d,maxt=%d", c.mint, c.maxt), func(t *testing.T) {
			shouldClose, getNew, newMint := head.IsQuerierCollidingWithTruncation(c.mint, c.maxt)
			require.Equal(t, c.expShouldClose, shouldClose)
			require.Equal(t, c.expGetNew, getNew)
			if getNew {
				require.Equal(t, c.expNewMint, newMint)
			}
		})
	}
}
