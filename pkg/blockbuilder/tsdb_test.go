package blockbuilder

import (
	"context"
	"math"
	"sort"
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
	"github.com/grafana/mimir/pkg/util/validation"
)

func TestTSDBBuilder(t *testing.T) {
	limits := defaultLimitsTestConfig()
	limits.OutOfOrderTimeWindow = 2 * model.Duration(time.Hour)
	overrides, err := validation.NewOverrides(limits, nil)
	require.NoError(t, err)

	userID := "1"

	// val is int64 to make calling this function concise.
	addSample := func(builder *tsdbBuilder, ts int64, lastEnd, currEnd int64, recordProcessedBefore, accepted bool) {
		var rec kgo.Record
		rec.Key = []byte(userID)

		req := mimirpb.WriteRequest{}

		req.Timeseries = append(req.Timeseries, mimirpb.PreallocTimeseries{
			TimeSeries: &mimirpb.TimeSeries{
				Labels: []mimirpb.LabelAdapter{
					{Name: "foo", Value: "bar"},
				},
				Samples: []mimirpb.Sample{
					{TimestampMs: ts, Value: float64(ts)},
				},
			},
		})

		data, err := req.Marshal()
		require.NoError(t, err)
		rec.Value = data
		allProcessed, err := builder.process(context.Background(), &rec, lastEnd, currEnd, recordProcessedBefore)
		require.NoError(t, err)
		require.Equal(t, accepted, allProcessed)
	}

	builder := newTSDBBuilder(log.NewNopLogger(), overrides, mimir_tsdb.BlocksStorageConfig{
		TSDB: mimir_tsdb.TSDBConfig{
			Dir: t.TempDir(),
		},
	})

	processingRange := time.Hour.Milliseconds()

	// TODO(codesome): try an odd hour as well
	// TODO(codesome): test histograms
	lastEnd := 2 * processingRange
	currEnd := lastEnd + processingRange

	// Add a sample for all the cases and check for correctness.
	var expSamples []mimirpb.Sample

	// 1. Processing records that were processed before (they come first in real world).
	// a. This sample is already processed. So it should be ignored but say all processed
	//    because it is already in a block.
	addSample(builder, lastEnd-10, lastEnd, currEnd, true, true)
	// b. This goes in this block.
	addSample(builder, lastEnd+100, lastEnd, currEnd, true, true)
	expSamples = append(expSamples, mimirpb.Sample{TimestampMs: lastEnd + 100, Value: float64(lastEnd + 100)})
	// c. This sample should be processed in the future.
	addSample(builder, currEnd+1, lastEnd, currEnd, true, false)

	// 2. Processing records that were not processed before.
	// a. Sample that belonged to previous processing period but came in late. Processed in current cycle.
	addSample(builder, lastEnd-5, lastEnd, currEnd, false, true)
	expSamples = append(expSamples, mimirpb.Sample{TimestampMs: lastEnd - 5, Value: float64(lastEnd - 5)})
	// b. Sample that belongs to the current processing period.
	addSample(builder, lastEnd+200, lastEnd, currEnd, false, true)
	expSamples = append(expSamples, mimirpb.Sample{TimestampMs: lastEnd + 200, Value: float64(lastEnd + 200)})
	// c. This sample should be processed in the future.
	addSample(builder, currEnd+2, lastEnd, currEnd, false, false)

	// 3. Out of order sample in a new record.
	// a. In the current range but out of order w.r.t. the previous sample.
	addSample(builder, lastEnd+20, lastEnd, currEnd, false, true)
	expSamples = append(expSamples, mimirpb.Sample{TimestampMs: lastEnd + 20, Value: float64(lastEnd + 20)})
	// b. Before current range and out of order w.r.t. the previous sample. Already covered above, but this
	// exists to explicitly state the case.
	addSample(builder, lastEnd-20, lastEnd, currEnd, false, true)
	expSamples = append(expSamples, mimirpb.Sample{TimestampMs: lastEnd - 20, Value: float64(lastEnd - 20)})

	// Query the TSDB for the expected samples.
	db, err := builder.getOrCreateTSDB(userID)
	require.NoError(t, err)

	queryDB := func(db *tsdb.DB) {
		querier, err := db.Querier(math.MinInt64, math.MaxInt64)
		require.NoError(t, err)
		ss := querier.Select(
			context.Background(), true, nil,
			labels.MustNewMatcher(labels.MatchEqual, "foo", "bar"),
		)

		require.True(t, ss.Next())
		series := ss.At()
		require.False(t, ss.Next())

		require.Equal(t, labels.FromStrings("foo", "bar"), series.Labels())
		it := series.Iterator(nil)
		var actSamples []mimirpb.Sample
		for it.Next() == chunkenc.ValFloat {
			ts, val := it.At()
			actSamples = append(actSamples, mimirpb.Sample{TimestampMs: ts, Value: val})
		}
		require.NoError(t, it.Err())
		require.NoError(t, ss.Err())
		require.NoError(t, querier.Close())

		sort.Slice(expSamples, func(i, j int) bool {
			return expSamples[i].TimestampMs < expSamples[j].TimestampMs
		})
		require.Equal(t, expSamples, actSamples)
	}

	// Check the samples in the DB.
	queryDB(db.db)

	dbDir := builder.blocksStorageConfig.TSDB.BlocksDir(userID)
	// This should create the appropriate blocks and close the DB.
	err = builder.compactAndRemoveDBs(context.Background())
	require.NoError(t, err)
	require.Nil(t, builder.getTSDB(userID))

	newDB, err := tsdb.Open(dbDir, log.NewNopLogger(), nil, nil, nil)
	require.NoError(t, err)

	// One for the in-order current range. Two for the out-of-order blocks: ont for current range
	// and one for the previous range.
	blocks := newDB.Blocks()
	require.Len(t, blocks, 3)

	blockRange := 2 * time.Hour.Milliseconds()
	// out of order block for the previous range.
	require.Equal(t, lastEnd-blockRange, blocks[0].MinTime())
	require.Equal(t, lastEnd, blocks[0].MaxTime())
	// One in-order and one out-of-order block for the current range.
	require.Equal(t, lastEnd, blocks[1].MinTime())
	require.Equal(t, lastEnd+blockRange, blocks[1].MaxTime())
	require.Equal(t, lastEnd, blocks[2].MinTime())
	require.Equal(t, lastEnd+blockRange, blocks[2].MaxTime())
	// Check correctness of samples in the blocks.
	queryDB(newDB)
	require.NoError(t, newDB.Close())
}

// It is important that processing empty request is a success, as in says all samples were processed,
// so that checkpointing can be done correctly.
func TestProcessingEmptyRequest(t *testing.T) {
	userID := "1"
	lastEnd := 2 * time.Hour.Milliseconds()
	currEnd := lastEnd + time.Hour.Milliseconds()

	overrides, err := validation.NewOverrides(defaultLimitsTestConfig(), nil)
	require.NoError(t, err)
	builder := newTSDBBuilder(log.NewNopLogger(), overrides, mimir_tsdb.BlocksStorageConfig{
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

	require.NoError(t, builder.closeAndRemoveDBs())
}

func defaultLimitsTestConfig() validation.Limits {
	limits := validation.Limits{}
	flagext.DefaultValues(&limits)
	return limits
}
