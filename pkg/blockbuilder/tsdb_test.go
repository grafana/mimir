package blockbuilder

import (
	"context"
	"math"
	"math/rand"
	"os"
	"path"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/grafana/mimir/pkg/mimirpb"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/util/test"
	"github.com/grafana/mimir/pkg/util/validation"
)

func TestTSDBBuilder(t *testing.T) {
	limits := defaultLimitsTestConfig()
	limits.OutOfOrderTimeWindow = 2 * model.Duration(time.Hour)
	limits.NativeHistogramsIngestionEnabled = true
	overrides, err := validation.NewOverrides(limits, nil)
	require.NoError(t, err)

	userID := strconv.Itoa(rand.Int())
	// Add a sample for all the cases and check for correctness.
	var expSamples []mimirpb.Sample
	var expHistograms []mimirpb.Histogram

	// val is int64 to make calling this function concise.
	createRequest := func(ts int64, accepted, isHistogram bool) *kgo.Record {
		var rec kgo.Record
		rec.Key = []byte(userID)

		req := mimirpb.WriteRequest{}

		var samples []mimirpb.Sample
		var histograms []mimirpb.Histogram
		var seriesValue string
		if isHistogram {
			seriesValue = "histogram"
			histograms = []mimirpb.Histogram{mimirpb.FromHistogramToHistogramProto(ts, test.GenerateTestHistogram(int(ts)))}
			if accepted {
				histograms[0].ResetHint = 0
				expHistograms = append(expHistograms, histograms[0])
			}
		} else {
			seriesValue = "float"
			samples = []mimirpb.Sample{{TimestampMs: ts, Value: float64(ts)}}
			if accepted {
				expSamples = append(expSamples, samples[0])
			}
		}
		req.Timeseries = append(req.Timeseries, mimirpb.PreallocTimeseries{
			TimeSeries: &mimirpb.TimeSeries{
				Labels: []mimirpb.LabelAdapter{
					{Name: "foo", Value: seriesValue},
				},
				Samples:    samples,
				Histograms: histograms,
			},
		})

		data, err := req.Marshal()
		require.NoError(t, err)
		rec.Value = data

		return &rec
	}
	addFloatSample := func(builder *tsdbBuilder, ts int64, lastEnd, currEnd int64, recordProcessedBefore, accepted bool) {
		rec := createRequest(ts, accepted, false)

		allProcessed, err := builder.process(context.Background(), rec, lastEnd, currEnd, recordProcessedBefore)
		require.NoError(t, err)
		require.Equal(t, accepted, allProcessed)
	}
	addHistogramSample := func(builder *tsdbBuilder, ts int64, lastEnd, currEnd int64, recordProcessedBefore, accepted bool) {
		rec := createRequest(ts, accepted, true)

		allProcessed, err := builder.process(context.Background(), rec, lastEnd, currEnd, recordProcessedBefore)
		require.NoError(t, err)
		require.Equal(t, accepted, allProcessed)
	}

	queryDB := func(db *tsdb.DB) {
		querier, err := db.Querier(math.MinInt64, math.MaxInt64)
		require.NoError(t, err)
		ss := querier.Select(
			context.Background(), true, nil,
			labels.MustNewMatcher(labels.MatchRegexp, "foo", ".*"),
		)

		var actSamples []mimirpb.Sample
		var actHistograms []mimirpb.Histogram
		for ss.Next() {
			series := ss.At()

			require.True(t, labels.Compare(labels.FromStrings("foo", "float"), series.Labels()) == 0 ||
				labels.Compare(labels.FromStrings("foo", "histogram"), series.Labels()) == 0)

			it := series.Iterator(nil)
			for typ := it.Next(); typ != chunkenc.ValNone; typ = it.Next() {
				switch typ {
				case chunkenc.ValFloat:
					ts, val := it.At()
					actSamples = append(actSamples, mimirpb.Sample{TimestampMs: ts, Value: val})
				case chunkenc.ValHistogram:
					ts, h := it.AtHistogram(nil)
					hp := mimirpb.FromHistogramToHistogramProto(ts, h)
					hp.ResetHint = 0
					actHistograms = append(actHistograms, hp)
				default:
					t.Fatalf("unexpected sample type %v", typ)
				}
			}
			require.NoError(t, it.Err())
		}

		require.NoError(t, ss.Err())
		require.NoError(t, querier.Close())

		sort.Slice(expSamples, func(i, j int) bool {
			return expSamples[i].TimestampMs < expSamples[j].TimestampMs
		})
		sort.Slice(expHistograms, func(i, j int) bool {
			return expHistograms[i].Timestamp < expHistograms[j].Timestamp
		})
		require.Equal(t, expSamples, actSamples)
		require.Equal(t, expHistograms, actHistograms)
	}

	processingRange := time.Hour.Milliseconds()
	blockRange := 2 * time.Hour.Milliseconds()
	for _, tc := range []struct {
		name                        string
		lastEnd, currEnd            int64
		verifyBlocksAfterCompaction func(blocks []*tsdb.Block)
	}{
		{
			name:    "current start is at even hour",
			lastEnd: 2 * processingRange,
			currEnd: 3 * processingRange,
			verifyBlocksAfterCompaction: func(blocks []*tsdb.Block) {
				require.Len(t, blocks, 4)

				lastEnd := 2 * processingRange
				// One in-order and one out-of-order block for the previous range.
				require.Equal(t, lastEnd-blockRange, blocks[0].MinTime())
				require.Equal(t, lastEnd, blocks[0].MaxTime())
				require.Equal(t, lastEnd-blockRange, blocks[1].MinTime())
				require.Equal(t, lastEnd, blocks[1].MaxTime())
				// One in-order and one out-of-order block for the current range.
				require.Equal(t, lastEnd, blocks[3].MinTime())
				require.Equal(t, lastEnd+blockRange, blocks[3].MaxTime())
				require.Equal(t, lastEnd, blocks[2].MinTime())
				require.Equal(t, lastEnd+blockRange, blocks[2].MaxTime())
			},
		},

		{
			name:    "current start is at odd hour",
			lastEnd: 3 * processingRange,
			currEnd: 4 * processingRange,
			verifyBlocksAfterCompaction: func(blocks []*tsdb.Block) {
				require.Len(t, blocks, 2)

				currEnd := 4 * processingRange
				// Both in-order and out-of-order blocks are in the same block range.
				require.Equal(t, currEnd-blockRange, blocks[0].MinTime())
				require.Equal(t, currEnd, blocks[0].MaxTime())
				require.Equal(t, currEnd-blockRange, blocks[1].MinTime())
				require.Equal(t, currEnd, blocks[1].MaxTime())
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			expSamples = expSamples[:0]
			expHistograms = expHistograms[:0]
			builder := newTSDBBuilder(log.NewNopLogger(), overrides, 0, mimir_tsdb.BlocksStorageConfig{
				TSDB: mimir_tsdb.TSDBConfig{
					Dir: t.TempDir(),
				},
			})

			currEnd, lastEnd := tc.currEnd, tc.lastEnd
			{ // Add float samples.
				// 1. Processing records that were processed before (they come first in real world).
				// A. This sample is already processed. So it should be ignored but say all processed
				//    because it is already in a block.
				addFloatSample(builder, lastEnd-10, lastEnd, currEnd, true, true)
				// Since this is already processed, it should not be added to the expected samples.
				expSamples = expSamples[:0]
				// B. This goes in this block.
				addFloatSample(builder, lastEnd+100, lastEnd, currEnd, true, true)
				// C. This sample should be processed in the future.
				addFloatSample(builder, currEnd+1, lastEnd, currEnd, true, false)

				// 2. Processing records that were not processed before.
				// A. Sample that belonged to previous processing period but came in late. Processed in current cycle.
				addFloatSample(builder, lastEnd-5, lastEnd, currEnd, false, true)
				// B. Sample that belongs to the current processing period.
				addFloatSample(builder, lastEnd+200, lastEnd, currEnd, false, true)
				// C. This sample should be processed in the future.
				addFloatSample(builder, currEnd+2, lastEnd, currEnd, false, false)

				// 3. Out of order sample in a new record.
				// A. In the current range but out of order w.r.t. the previous sample.
				addFloatSample(builder, lastEnd+20, lastEnd, currEnd, false, true)
				// B. Before current range and out of order w.r.t. the previous sample. Already covered above, but this
				// exists to explicitly state the case.
				addFloatSample(builder, lastEnd-20, lastEnd, currEnd, false, true)
			}
			{ // Add native histogram samples.
				// 1.A from above.
				addHistogramSample(builder, lastEnd-10, lastEnd, currEnd, true, true)
				expHistograms = expHistograms[:0]

				// 2.A from above. Although in real world recordProcessedBefore=false will only come after all recordProcessedBefore=true
				// are done, we are inserting it here because native histograms do not support out-of-order samples yet.
				// This sample here goes in the in-order block unlike above where 2.A goes in out-of-order block.
				addHistogramSample(builder, lastEnd-5, lastEnd, currEnd, false, true)

				// 1.B from above.
				addHistogramSample(builder, lastEnd+100, lastEnd, currEnd, true, true)
				// 1.C from above.
				addHistogramSample(builder, currEnd+1, lastEnd, currEnd, true, false)

				// 2.B from above.
				addHistogramSample(builder, lastEnd+200, lastEnd, currEnd, false, true)
				// 2.C from above.
				addHistogramSample(builder, currEnd+2, lastEnd, currEnd, false, false)

				// 3.A and 3.B not done. TODO: do it when out-of-order histograms are supported.
			}

			// Query the TSDB for the expected samples.
			db, err := builder.getOrCreateTSDB(userID)
			require.NoError(t, err)

			// Check the samples in the DB.
			queryDB(db.db)

			// This should create the appropriate blocks and close the DB.
			shipperDir := t.TempDir()
			err = builder.compactAndUpload(context.Background(), mockUploaderFunc(t, shipperDir))
			require.NoError(t, err)
			require.Nil(t, builder.getTSDB(userID))

			newDB, err := tsdb.Open(shipperDir, log.NewNopLogger(), nil, nil, nil)
			require.NoError(t, err)

			// One for the in-order current range. Two for the out-of-order blocks: ont for current range
			// and one for the previous range.
			blocks := newDB.Blocks()
			tc.verifyBlocksAfterCompaction(blocks)

			// Check correctness of samples in the blocks.
			queryDB(newDB)
			require.NoError(t, newDB.Close())
		})
	}
}

func mockUploaderFunc(t *testing.T, destDir string) func(context.Context, string) blockUploader {
	return func(context.Context, string) blockUploader {
		return func(blockDir string) error {
			err := os.Rename(blockDir, path.Join(destDir, path.Base(blockDir)))
			require.NoError(t, err)
			return nil
		}
	}
}

// It is important that processing empty request is a success, as in says all samples were processed,
// so that checkpointing can be done correctly.
func TestProcessingEmptyRequest(t *testing.T) {
	userID := "1"
	lastEnd := 2 * time.Hour.Milliseconds()
	currEnd := lastEnd + time.Hour.Milliseconds()

	overrides, err := validation.NewOverrides(defaultLimitsTestConfig(), nil)
	require.NoError(t, err)
	builder := newTSDBBuilder(log.NewNopLogger(), overrides, 0, mimir_tsdb.BlocksStorageConfig{
		TSDB: mimir_tsdb.TSDBConfig{
			Dir: t.TempDir(),
		},
	})

	// Has a timeseries with no samples.
	var rec kgo.Record
	rec.Key = []byte(userID)
	req := mimirpb.WriteRequest{}
	req.Timeseries = append(req.Timeseries, mimirpb.PreallocTimeseries{
		TimeSeries: &mimirpb.TimeSeries{
			Labels:  []mimirpb.LabelAdapter{{Name: "foo", Value: "bar"}},
			Samples: []mimirpb.Sample{},
		},
	})
	data, err := req.Marshal()
	require.NoError(t, err)
	rec.Value = data
	allProcessed, err := builder.process(context.Background(), &rec, lastEnd, currEnd, false)
	require.NoError(t, err)
	require.True(t, allProcessed)

	// Has no timeseries.
	req.Timeseries = req.Timeseries[:0]
	data, err = req.Marshal()
	require.NoError(t, err)
	rec.Value = data
	allProcessed, err = builder.process(context.Background(), &rec, lastEnd, currEnd, false)
	require.NoError(t, err)
	require.True(t, allProcessed)

	require.NoError(t, builder.tsdbs[userID].db.Close())
}

func defaultLimitsTestConfig() validation.Limits {
	limits := validation.Limits{}
	flagext.DefaultValues(&limits)
	return limits
}
