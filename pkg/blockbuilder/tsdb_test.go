// SPDX-License-Identifier: AGPL-3.0-only

package blockbuilder

import (
	"context"
	"fmt"
	"math"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/prometheus/common/promslog"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/ingest"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/util/test"
	"github.com/grafana/mimir/pkg/util/validation"
)

func createWriteRequest(t *testing.T, suffix string, samples []mimirpb.Sample, histograms []mimirpb.Histogram) []byte {
	req := mimirpb.WriteRequest{}

	var seriesValue string
	if len(histograms) > 0 {
		seriesValue = "histogram"
	} else {
		seriesValue = "float"
	}
	req.Timeseries = append(req.Timeseries, mimirpb.PreallocTimeseries{
		TimeSeries: &mimirpb.TimeSeries{
			Labels: []mimirpb.LabelAdapter{
				{Name: "foo", Value: fmt.Sprintf("%s%s", seriesValue, suffix)},
			},
			Samples:    samples,
			Histograms: histograms,
		},
	})

	data, err := req.Marshal()
	require.NoError(t, err)

	return data
}

func floatSample(ts int64, val float64) []mimirpb.Sample {
	return []mimirpb.Sample{{TimestampMs: ts, Value: val}}
}

func histogramSample(ts int64) []mimirpb.Histogram {
	return []mimirpb.Histogram{
		mimirpb.FromHistogramToHistogramProto(ts, test.GenerateTestHistogram(int(ts))),
	}
}

func TestTSDBBuilder(t *testing.T) {
	ctx := t.Context()

	processingRange := time.Hour.Milliseconds()
	blockRange := 2 * time.Hour.Milliseconds()

	createRecord := func(userID string, samples []mimirpb.Sample, histograms []mimirpb.Histogram) *kgo.Record {
		return &kgo.Record{
			Key:   []byte(userID),
			Value: createWriteRequest(t, "", samples, histograms),
		}
	}

	testCases := []struct {
		name                        string
		limits                      *validation.Limits
		samples                     []testSample
		histograms                  []testHistogram
		verifyBlocksAfterCompaction func(blocks []*tsdb.Block)
	}{
		{
			name: "current start is at even hour",
			limits: &validation.Limits{
				OutOfOrderTimeWindow:             model.Duration(30 * time.Minute),
				NativeHistogramsIngestionEnabled: true,
			},
			samples: []testSample{
				{ts: 2*processingRange + 100, val: 1},
				{ts: 3*processingRange + 1, val: 1},
				{ts: 3*processingRange + 2, val: 1},
			},
			histograms: []testHistogram{
				{ts: 2*processingRange + 100},
				{ts: 3*processingRange + 1},
				{ts: 3*processingRange + 2},
			},
			verifyBlocksAfterCompaction: func(blocks []*tsdb.Block) {
				require.Len(t, blocks, 1) // 1 block for main userID

				lastEnd := 2 * processingRange
				// One block for the current range
				require.Equal(t, lastEnd, blocks[0].MinTime())
				require.Equal(t, lastEnd+blockRange, blocks[0].MaxTime())
			},
		},
		{
			name: "current start is at odd hour",
			limits: &validation.Limits{
				OutOfOrderTimeWindow:             model.Duration(30 * time.Minute),
				NativeHistogramsIngestionEnabled: true,
			},
			samples: []testSample{
				{ts: 3*processingRange + 100, val: 1},
				{ts: 4*processingRange + 1, val: 1},
				{ts: 4*processingRange + 2, val: 1},
			},
			histograms: []testHistogram{
				{ts: 3*processingRange + 100},
				{ts: 4*processingRange + 1},
				{ts: 4*processingRange + 2},
			},
			verifyBlocksAfterCompaction: func(blocks []*tsdb.Block) {
				require.Len(t, blocks, 2) // 2 blocks due to spanning across 2 block ranges

				currEnd := 4 * processingRange
				// Two blocks spanning different 2-hour ranges
				require.Equal(t, currEnd-blockRange, blocks[0].MinTime())
				require.Equal(t, currEnd, blocks[0].MaxTime())
				require.Equal(t, currEnd, blocks[1].MinTime())
				require.Equal(t, currEnd+blockRange, blocks[1].MaxTime())
			},
		},
		{
			name: "out-of-order samples within OOO window",
			limits: &validation.Limits{
				OutOfOrderTimeWindow:             model.Duration(30 * time.Minute),
				NativeHistogramsIngestionEnabled: true,
			},
			samples: []testSample{
				{ts: 2*processingRange + 1000, val: 1.0},
				{ts: 2*processingRange + 200, val: 2.0}, // 800ms before the in-order sample
			},
			histograms: []testHistogram{
				{ts: 2*processingRange + 1000},
				{ts: 2*processingRange + 200}, // 800ms before the in-order histogram
			},
			verifyBlocksAfterCompaction: func(blocks []*tsdb.Block) {
				// OOO samples may create multiple blocks (in-order + out-of-order)
				require.GreaterOrEqual(t, len(blocks), 1) // At least 1 block
			},
		},
		{
			name: "out-of-order samples beyond OOO window",
			limits: &validation.Limits{
				OutOfOrderTimeWindow:             model.Duration(30 * time.Minute),
				NativeHistogramsIngestionEnabled: true,
			},
			samples: []testSample{
				{ts: 2*processingRange + 1000, val: 1.0},
				{ts: 2*processingRange + 1000 - 1900000, val: 2.0, shouldDiscard: true}, // More than 30 minutes before
			},
			histograms: []testHistogram{
				{ts: 2*processingRange + 1000},
				{ts: 2*processingRange + 1000 - 1900000, shouldDiscard: true}, // More than 30 minutes before
			},
			verifyBlocksAfterCompaction: func(blocks []*tsdb.Block) {
				require.GreaterOrEqual(t, len(blocks), 1) // At least 1 block
			},
		},
		{
			name: "out-of-order samples for tenant without OOO window",
			limits: &validation.Limits{
				OutOfOrderTimeWindow:             model.Duration(0), // No OOO window
				NativeHistogramsIngestionEnabled: true,
			},
			samples: []testSample{
				{ts: 2*processingRange + 1000, val: 1.0},
				{ts: 2*processingRange + 500, val: 2.0, shouldDiscard: true}, // Before the in-order sample
			},
			histograms: []testHistogram{
				{ts: 2*processingRange + 1000},
				{ts: 2*processingRange + 500, shouldDiscard: true}, // Before the in-order histogram
			},
			verifyBlocksAfterCompaction: func(blocks []*tsdb.Block) {
				require.GreaterOrEqual(t, len(blocks), 1) // At least 1 block
			},
		},
		{
			name: "duplicate samples with different values",
			limits: &validation.Limits{
				OutOfOrderTimeWindow:             model.Duration(30 * time.Minute),
				NativeHistogramsIngestionEnabled: true,
			},
			samples: []testSample{
				{ts: 2*processingRange + 1000, val: 1.0},
				{ts: 2*processingRange + 1000, val: 2.0, shouldDiscard: true}, // Same timestamp, different value
			},
			verifyBlocksAfterCompaction: func(blocks []*tsdb.Block) {
				require.GreaterOrEqual(t, len(blocks), 1) // At least 1 block
			},
		},
		{
			name: "samples too old beyond retention",
			limits: &validation.Limits{
				OutOfOrderTimeWindow:             model.Duration(30 * time.Minute),
				NativeHistogramsIngestionEnabled: true,
			},
			samples: []testSample{
				{ts: 2*processingRange + 1000, val: 1.0},
				{ts: 0, val: 2.0, shouldDiscard: true}, // Very old sample
			},
			verifyBlocksAfterCompaction: func(blocks []*tsdb.Block) {
				require.GreaterOrEqual(t, len(blocks), 1) // At least 1 block
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			userID := "user1"
			limits := map[string]*validation.Limits{
				userID: tc.limits,
			}
			overrides := validation.NewOverrides(defaultLimitsTestConfig(), validation.NewMockTenantLimits(limits))
			metrics := newTSDBBBuilderMetrics(prometheus.NewPedanticRegistry())

			builder := NewTSDBBuilder(log.NewNopLogger(), t.TempDir(), mimir_tsdb.BlocksStorageConfig{}, overrides, metrics, 0)

			// Hold samples for all cases and check for the correctness.
			var (
				expSamples    []mimirpb.Sample
				expHistograms []mimirpb.Histogram
			)

			// Add float samples
			for _, s := range tc.samples {
				samples := floatSample(s.ts, s.val)
				if !s.shouldDiscard {
					expSamples = append(expSamples, samples...)
				}
				rec := createRecord(userID, samples, nil)
				err := builder.Process(ctx, rec)
				require.NoError(t, err)
			}

			// Add histogram samples
			for _, h := range tc.histograms {
				histograms := histogramSample(h.ts)
				if !h.shouldDiscard {
					for i := range histograms {
						histograms[i].ResetHint = 0
						expHistograms = append(expHistograms, histograms[i])
					}
				}
				rec := createRecord(userID, nil, histograms)
				err := builder.Process(ctx, rec)
				require.NoError(t, err)
			}

			// Query the TSDB for the expected samples.
			tenant := tsdbTenant{
				partitionID: 0,
				tenantID:    userID,
			}
			db, err := builder.getOrCreateTSDB(tenant)
			require.NoError(t, err)

			// Check the samples in the DB.
			compareQuery(t, db.DB, expSamples, expHistograms, labels.MustNewMatcher(labels.MatchRegexp, "foo", ".*"))

			// This should create the appropriate blocks and close the DB.
			shipperDir := t.TempDir()
			_, err = builder.CompactAndUpload(ctx, mockUploaderFunc(t, shipperDir))
			require.NoError(t, err)
			require.Nil(t, builder.tsdbs[tenant])

			newDB, err := tsdb.Open(shipperDir, promslog.NewNopLogger(), nil, nil, nil)
			require.NoError(t, err)

			blocks := newDB.Blocks()
			tc.verifyBlocksAfterCompaction(blocks)

			// Check correctness of samples in the blocks.
			compareQuery(t, newDB, expSamples, expHistograms, labels.MustNewMatcher(labels.MatchRegexp, "foo", ".*"))
			require.NoError(t, newDB.Close())
		})
	}
}

type testSample struct {
	ts            int64
	val           float64
	shouldDiscard bool
}

type testHistogram struct {
	ts            int64
	shouldDiscard bool
}

func TestTSDBBuilder_CompactAndUpload_fail(t *testing.T) {
	overrides := validation.NewOverrides(defaultLimitsTestConfig(), nil)
	metrics := newTSDBBBuilderMetrics(prometheus.NewPedanticRegistry())
	builder := NewTSDBBuilder(log.NewNopLogger(), t.TempDir(), mimir_tsdb.BlocksStorageConfig{}, overrides, metrics, 0)
	t.Cleanup(func() {
		require.NoError(t, builder.Close())
	})

	userID := "user1"
	tenant := tsdbTenant{
		partitionID: 0,
		tenantID:    userID,
	}
	_, err := builder.getOrCreateTSDB(tenant)
	require.NoError(t, err)

	errUploadFailed := fmt.Errorf("upload failed")
	_, err = builder.CompactAndUpload(context.Background(), func(_ context.Context, _, _ string, _ []tsdb.BlockMeta) error {
		return errUploadFailed
	})
	require.ErrorIs(t, err, errUploadFailed)
}

func compareQueryWithDir(t *testing.T, bucketDir string, expSamples []mimirpb.Sample, expHistograms []mimirpb.Histogram, matchers ...*labels.Matcher) *tsdb.DB {
	db, err := tsdb.Open(bucketDir, promslog.NewNopLogger(), nil, nil, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	compareQuery(t, db, expSamples, expHistograms, matchers...)

	return db
}

func compareQuery(t *testing.T, db *tsdb.DB, expSamples []mimirpb.Sample, expHistograms []mimirpb.Histogram, matchers ...*labels.Matcher) {
	querier, err := db.Querier(math.MinInt64, math.MaxInt64)
	require.NoError(t, err)
	ss := querier.Select(
		context.Background(), true, nil,
		matchers...,
	)

	var actSamples []mimirpb.Sample
	var actHistograms []mimirpb.Histogram
	for ss.Next() {
		series := ss.At()

		fooVal := series.Labels().Get("foo")
		require.True(t, strings.HasPrefix(fooVal, "float") ||
			strings.HasPrefix(fooVal, "histogram"))

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

func mockUploaderFunc(t *testing.T, destDir string) blockUploader {
	return func(_ context.Context, _, dbDir string, metas []tsdb.BlockMeta) error {
		for _, meta := range metas {
			blockDir := path.Join(dbDir, meta.ULID.String())
			err := os.Rename(blockDir, path.Join(destDir, path.Base(blockDir)))
			require.NoError(t, err)
		}
		return nil
	}
}

// It is important that processing empty request is a success, as in says all samples were processed,
// so that checkpointing can be done correctly.
func TestProcessingEmptyRequest(t *testing.T) {
	userID := "1"

	overrides := validation.NewOverrides(defaultLimitsTestConfig(), nil)
	metrics := newTSDBBBuilderMetrics(prometheus.NewPedanticRegistry())
	builder := NewTSDBBuilder(log.NewNopLogger(), t.TempDir(), mimir_tsdb.BlocksStorageConfig{}, overrides, metrics, 0)

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
	err = builder.Process(context.Background(), &rec)
	require.NoError(t, err)

	// Has no timeseries.
	req.Timeseries = req.Timeseries[:0]
	data, err = req.Marshal()
	require.NoError(t, err)
	rec.Value = data
	err = builder.Process(context.Background(), &rec)
	require.NoError(t, err)

	require.NoError(t, builder.tsdbs[tsdbTenant{0, userID}].Close())
}

func TestTSDBBuilder_KafkaRecordVersion(t *testing.T) {
	t.Run("record version header missing entirely", func(t *testing.T) {
		userID := "1"
		processingRange := time.Hour.Milliseconds()
		lastEnd := 2 * processingRange

		overrides := validation.NewOverrides(defaultLimitsTestConfig(), nil)
		metrics := newTSDBBBuilderMetrics(prometheus.NewPedanticRegistry())
		builder := NewTSDBBuilder(log.NewNopLogger(), t.TempDir(), mimir_tsdb.BlocksStorageConfig{}, overrides, metrics, 0)
		samples := floatSample(lastEnd+20, 1)
		histograms := histogramSample(lastEnd + 40)

		rec := &kgo.Record{
			Key:   []byte(userID),
			Value: createWriteRequest(t, "", samples, histograms),
		}
		err := builder.Process(context.Background(), rec)

		require.NoError(t, err)
	})

	t.Run("record version supported", func(t *testing.T) {
		userID := "1"
		attemptedRecordVersion := 1
		processingRange := time.Hour.Milliseconds()
		lastEnd := 2 * processingRange

		overrides := validation.NewOverrides(defaultLimitsTestConfig(), nil)
		metrics := newTSDBBBuilderMetrics(prometheus.NewPedanticRegistry())
		builder := NewTSDBBuilder(log.NewNopLogger(), t.TempDir(), mimir_tsdb.BlocksStorageConfig{}, overrides, metrics, 0)
		samples := floatSample(lastEnd+20, 1)
		histograms := histogramSample(lastEnd + 40)

		rec := &kgo.Record{
			Key:     []byte(userID),
			Value:   createWriteRequest(t, "", samples, histograms),
			Headers: []kgo.RecordHeader{ingest.RecordVersionHeader(attemptedRecordVersion)},
		}
		err := builder.Process(context.Background(), rec)

		require.NoError(t, err)
	})

	t.Run("record version unsupported", func(t *testing.T) {
		userID := "1"
		attemptedRecordVersion := 101
		processingRange := time.Hour.Milliseconds()
		lastEnd := 2 * processingRange

		overrides := validation.NewOverrides(defaultLimitsTestConfig(), nil)
		metrics := newTSDBBBuilderMetrics(prometheus.NewPedanticRegistry())
		builder := NewTSDBBuilder(log.NewNopLogger(), t.TempDir(), mimir_tsdb.BlocksStorageConfig{}, overrides, metrics, 0)
		samples := floatSample(lastEnd+20, 1)
		histograms := histogramSample(lastEnd + 40)

		rec := &kgo.Record{
			Key:     []byte(userID),
			Value:   createWriteRequest(t, "", samples, histograms),
			Headers: []kgo.RecordHeader{ingest.RecordVersionHeader(attemptedRecordVersion)},
		}
		err := builder.Process(context.Background(), rec)

		require.ErrorContains(t, err, fmt.Sprintf("unsupported version: %d, max supported version: %d", attemptedRecordVersion, ingest.LatestRecordVersion))
	})
}

// TestTSDBBuilderLimits tests the correct enforcements of series limits and also
// that series limit error does not cause the processing to fail (i.e. do not error out).
func TestTSDBBuilderLimits(t *testing.T) {
	var (
		user1 = "user1"
		user2 = "user2"
		// Limits should be applied only if the limits is under 50
		applyGlobalSeriesLimitUnder = 50
	)

	limits := map[string]*validation.Limits{
		user1: {
			MaxGlobalSeriesPerUser:           30,
			NativeHistogramsIngestionEnabled: true,
		},
		user2: {
			MaxGlobalSeriesPerUser:           150,
			NativeHistogramsIngestionEnabled: true,
		},
	}
	overrides := validation.NewOverrides(defaultLimitsTestConfig(), validation.NewMockTenantLimits(limits))

	metrics := newTSDBBBuilderMetrics(prometheus.NewPedanticRegistry())
	builder := NewTSDBBuilder(log.NewNopLogger(), t.TempDir(), mimir_tsdb.BlocksStorageConfig{}, overrides, metrics, applyGlobalSeriesLimitUnder)
	t.Cleanup(func() {
		require.NoError(t, builder.Close())
	})

	var (
		processingRange = time.Hour.Milliseconds()
		lastEnd         = 2 * processingRange
		ts              = lastEnd + (processingRange / 2)
	)
	createRequest := func(userID string, seriesID int) *kgo.Record {
		var (
			samples    []mimirpb.Sample
			histograms []mimirpb.Histogram
		)
		if seriesID%2 == 0 {
			samples = floatSample(ts, float64(seriesID))
		} else {
			histograms = histogramSample(ts)
		}
		return &kgo.Record{
			Key:   []byte(userID),
			Value: createWriteRequest(t, strconv.Itoa(seriesID), samples, histograms),
		}
	}

	for seriesID := 1; seriesID <= 100; seriesID++ {
		for userID := range limits {
			rec := createRequest(userID, seriesID)
			err := builder.Process(context.Background(), rec)
			require.NoError(t, err)
		}
	}

	// user1 had a limit of 30, which is less than applyGlobalSeriesLimitUnder.
	// So the limit must be applied.
	db, err := builder.getOrCreateTSDB(tsdbTenant{tenantID: user1})
	require.NoError(t, err)
	require.Equal(t, uint64(30), db.Head().NumSeries())

	// user2 had a limit of 100, which is greather than applyGlobalSeriesLimitUnder.
	// So the limit must not be applied.
	db, err = builder.getOrCreateTSDB(tsdbTenant{tenantID: user2})
	require.NoError(t, err)
	require.Equal(t, uint64(100), db.Head().NumSeries())
}

// TestTSDBBuilderNativeHistogramEnabledError tests that when native histograms are disabled for a tenant,
// the TSDB builder does not error out when trying to ingest native histogram for that tenant.
func TestTSDBBuilderNativeHistogramEnabledError(t *testing.T) {
	var (
		user1 = "user1"
		user2 = "user2"
	)

	limits := map[string]*validation.Limits{
		user1: {
			NativeHistogramsIngestionEnabled: true,
		},
		user2: {
			NativeHistogramsIngestionEnabled: false,
		},
	}
	overrides := validation.NewOverrides(defaultLimitsTestConfig(), validation.NewMockTenantLimits(limits))

	metrics := newTSDBBBuilderMetrics(prometheus.NewPedanticRegistry())
	builder := NewTSDBBuilder(log.NewNopLogger(), t.TempDir(), mimir_tsdb.BlocksStorageConfig{}, overrides, metrics, 0)
	t.Cleanup(func() {
		require.NoError(t, builder.Close())
	})

	var (
		processingRange = time.Hour.Milliseconds()
		lastEnd         = 2 * processingRange
		ts              = lastEnd + (processingRange / 2)
	)
	for seriesID := 1; seriesID <= 100; seriesID++ {
		for userID := range limits {
			rec := &kgo.Record{
				Key:   []byte(userID),
				Value: createWriteRequest(t, strconv.Itoa(seriesID), nil, histogramSample(ts)),
			}
			err := builder.Process(context.Background(), rec)
			require.NoError(t, err)
		}
	}

	// user1 had native histograms enabled. We should see it in the TSDB.
	db, err := builder.getOrCreateTSDB(tsdbTenant{tenantID: user1})
	require.NoError(t, err)
	require.Equal(t, uint64(100), db.Head().NumSeries())

	// user2 had native histograms disabled. Nothing should be in the TSDB.
	db, err = builder.getOrCreateTSDB(tsdbTenant{tenantID: user2})
	require.NoError(t, err)
	require.Equal(t, uint64(0), db.Head().NumSeries())
}

func defaultLimitsTestConfig() validation.Limits {
	limits := validation.Limits{}
	flagext.DefaultValues(&limits)
	return limits
}

// TestBuilderCreatedTimestamp tests the the Remote Write protocol
// Created timestamp field is correctly handled.
// The Created timestamp injects extra zero samples at the
// specified timestamp - if it's before the next sample.
func TestBuilderCreatedTimestamp(t *testing.T) {
	var (
		user1 = "user_ooo_disabled"
		user2 = "user_all_enabled"
	)

	limits := map[string]*validation.Limits{
		user1: {
			NativeHistogramsIngestionEnabled:         true,
			OTelCreatedTimestampZeroIngestionEnabled: true,
			OutOfOrderTimeWindow:                     0,
		},
		user2: {
			NativeHistogramsIngestionEnabled:         true,
			OTelCreatedTimestampZeroIngestionEnabled: true,
			OutOfOrderTimeWindow:                     model.Duration(time.Hour),
		},
	}
	overrides := validation.NewOverrides(defaultLimitsTestConfig(), validation.NewMockTenantLimits(limits))

	processingRange := int64(100000)
	lastEnd := 2 * processingRange
	currEnd := 3 * processingRange

	simpleTestHistogram := func(ts int64, count uint64) mimirpb.Histogram {
		return mimirpb.Histogram{
			Count:         &mimirpb.Histogram_CountInt{CountInt: count},
			ZeroThreshold: 1e-128,
			ZeroCount:     &mimirpb.Histogram_ZeroCountInt{ZeroCountInt: count},
			Timestamp:     ts,
		}
	}
	expectedHistogram := func(ts int64, count uint64) test.Sample {
		return test.Sample{
			TS: ts,
			Hist: &histogram.Histogram{
				Count:         count,
				ZeroThreshold: 1e-128,
				ZeroCount:     count,
			},
		}
	}
	simpleTestFloatHistogram := func(ts int64, count float64) mimirpb.Histogram {
		return mimirpb.Histogram{
			Count:         &mimirpb.Histogram_CountFloat{CountFloat: count},
			ZeroThreshold: 1e-128,
			ZeroCount:     &mimirpb.Histogram_ZeroCountFloat{ZeroCountFloat: count},
			Timestamp:     ts,
		}
	}
	expectedFloatHistogram := func(ts int64, count float64) test.Sample {
		return test.Sample{
			TS: ts,
			FloatHist: &histogram.FloatHistogram{
				Count:         count,
				ZeroThreshold: 1e-128,
				ZeroCount:     count,
			},
		}
	}

	testCases := map[string]struct {
		input           []mimirpb.TimeSeries // We'll generate the Labels, just set the samples and created timestamp.
		expectSamples   []test.Sample
		expectDiscarded int
	}{
		"float samples": {
			input: []mimirpb.TimeSeries{
				{
					// Samples and Created timestamp (CT) outside the current block.
					Samples: []mimirpb.Sample{
						{TimestampMs: lastEnd - 50000 + 100, Value: 1},
						{TimestampMs: lastEnd - 50000 + 300, Value: 2},
					},
					CreatedTimestamp: lastEnd - 50000 + 200,
				},
				{
					// Sample inside the current block, but CT outside.
					Samples: []mimirpb.Sample{
						{TimestampMs: lastEnd + 100, Value: 3},
					},
					CreatedTimestamp: lastEnd - 50000 + 200,
				},
				{
					// Samples and CT inside the current block.
					Samples: []mimirpb.Sample{
						{TimestampMs: lastEnd + 300, Value: 4},
						{TimestampMs: lastEnd + 400, Value: 5},
					},
					CreatedTimestamp: lastEnd + 200,
				},
				{
					// Repeated CT.
					Samples: []mimirpb.Sample{
						{TimestampMs: lastEnd + 500, Value: 6},
					},
					CreatedTimestamp: lastEnd + 200,
				},
				{
					// Samples and CT mixed in in the current block.
					Samples: []mimirpb.Sample{
						{TimestampMs: lastEnd + 600, Value: 7},
						{TimestampMs: lastEnd + 800, Value: 8},
					},
					CreatedTimestamp: lastEnd + 700,
				},
				{
					// CT alone is ignored.
					Samples:          []mimirpb.Sample{},
					CreatedTimestamp: lastEnd + 1000,
				},
				{
					// CT inside current block but some samples in the next block.
					Samples: []mimirpb.Sample{
						{TimestampMs: currEnd - 200, Value: 9},
						{TimestampMs: currEnd + 200, Value: 10},
					},
					CreatedTimestamp: currEnd - 100,
				},
			},
			expectSamples: []test.Sample{
				{TS: lastEnd - 50000 + 100, Val: 1},
				{TS: lastEnd - 50000 + 200, Val: 0},
				{TS: lastEnd - 50000 + 300, Val: 2},
				{TS: lastEnd + 100, Val: 3},
				{TS: lastEnd + 200, Val: 0},
				{TS: lastEnd + 300, Val: 4},
				{TS: lastEnd + 400, Val: 5},
				{TS: lastEnd + 500, Val: 6},
				{TS: lastEnd + 600, Val: 7},
				{TS: lastEnd + 700, Val: 0},
				{TS: lastEnd + 800, Val: 8},
				{TS: currEnd - 200, Val: 9},
				{TS: currEnd - 100, Val: 0},
				{TS: currEnd + 200, Val: 10},
			},
		},
		"histogram samples": {
			input: []mimirpb.TimeSeries{
				{
					// Samples and Created timestamp (CT) outside the current block.
					Histograms: []mimirpb.Histogram{
						simpleTestHistogram(lastEnd-50000+100, 1),
						simpleTestHistogram(lastEnd-50000+300, 2),
					},
					CreatedTimestamp: lastEnd - 50000 + 200,
				},
				{
					// Sample inside the current block, but CT outside.
					Histograms: []mimirpb.Histogram{
						simpleTestHistogram(lastEnd+100, 3),
					},
					CreatedTimestamp: lastEnd - 50000 + 200,
				},
				{
					// Samples and CT inside the current block.
					Histograms: []mimirpb.Histogram{
						simpleTestHistogram(lastEnd+300, 4),
						simpleTestHistogram(lastEnd+400, 5),
					},
					CreatedTimestamp: lastEnd + 200,
				},
				{
					// Repeated CT.
					Histograms: []mimirpb.Histogram{
						simpleTestHistogram(lastEnd+500, 6),
					},
					CreatedTimestamp: lastEnd + 200,
				},
				{
					// Samples and CT mixed in in the current block.
					Histograms: []mimirpb.Histogram{
						simpleTestHistogram(lastEnd+600, 7),
						simpleTestHistogram(lastEnd+800, 8),
					},
					CreatedTimestamp: lastEnd + 700,
				},
				{
					// CT alone is ignored.
					Histograms:       []mimirpb.Histogram{},
					CreatedTimestamp: lastEnd + 900,
				},
				{
					// Test float histogram produces the correct zero sample.
					Histograms: []mimirpb.Histogram{
						simpleTestFloatHistogram(lastEnd+1100, 8.5),
					},
					CreatedTimestamp: lastEnd + 1000,
				},
				{
					// CT inside current block but some samples in the next block.
					Histograms: []mimirpb.Histogram{
						simpleTestHistogram(currEnd-200, 9),
						simpleTestHistogram(currEnd+200, 10),
					},
					CreatedTimestamp: currEnd - 100,
				},
			},
			expectSamples: []test.Sample{
				expectedHistogram(lastEnd-50000+100, 1),
				{TS: lastEnd - 50000 + 200, Hist: zeroHistogram},
				expectedHistogram(lastEnd-50000+300, 2),
				expectedHistogram(lastEnd+100, 3),
				{TS: lastEnd + 200, Hist: zeroHistogram},
				expectedHistogram(lastEnd+300, 4),
				expectedHistogram(lastEnd+400, 5),
				expectedHistogram(lastEnd+500, 6),
				expectedHistogram(lastEnd+600, 7),
				{TS: lastEnd + 700, Hist: zeroHistogram},
				expectedHistogram(lastEnd+800, 8),
				{TS: lastEnd + 1000, FloatHist: zeroFloatHistogram},
				expectedFloatHistogram(lastEnd+1100, 8.5),
				expectedHistogram(currEnd-200, 9),
				{TS: currEnd - 100, Hist: zeroHistogram},
				expectedHistogram(currEnd+200, 10),
			},
		},
		"float created sample duplicates previous sample": {
			input: []mimirpb.TimeSeries{
				{
					Samples: []mimirpb.Sample{
						{TimestampMs: lastEnd + 100, Value: 7},
					},
				},
				{
					Samples: []mimirpb.Sample{
						{TimestampMs: lastEnd + 200, Value: 8},
					},
					CreatedTimestamp: lastEnd + 100, // Duplicate the previous sample.
				},
			},
			expectSamples: []test.Sample{
				{TS: lastEnd + 100, Val: 7},
				{TS: lastEnd + 200, Val: 8},
			},
			expectDiscarded: 1,
		},
		"histogram created sample duplicates previous sample": {
			input: []mimirpb.TimeSeries{
				{
					Histograms: []mimirpb.Histogram{
						simpleTestHistogram(lastEnd+100, 7),
					},
				},
				{
					Histograms: []mimirpb.Histogram{
						simpleTestHistogram(lastEnd+200, 8),
					},
					CreatedTimestamp: lastEnd + 100, // Duplicate the previous sample.
				},
			},
			expectSamples: []test.Sample{
				expectedHistogram(lastEnd+100, 7),
				expectedHistogram(lastEnd+200, 8),
			},
			expectDiscarded: 1,
		},
	}

	registry := prometheus.NewPedanticRegistry()
	metrics := newTSDBBBuilderMetrics(registry)
	logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stdout))
	builder := NewTSDBBuilder(logger, t.TempDir(), mimir_tsdb.BlocksStorageConfig{}, overrides, metrics, 0)
	t.Cleanup(func() {
		require.NoError(t, builder.Close())
	})

	tcNumber := 0
	discarded := 0
	for name, tc := range testCases {
		for _, user := range []string{user1, user2} {
			tcName := fmt.Sprintf("number=%d tc=%s user=%s", tcNumber, name, user)
			t.Run(tcName, func(t *testing.T) {
				metricName := fmt.Sprintf("test_%s_%d", user, tcNumber)
				// Process the write requests.
				for _, input := range tc.input {
					ts := &mimirpb.TimeSeries{
						Labels:           mimirpb.FromLabelsToLabelAdapters(labels.FromStrings(model.MetricNameLabel, metricName)),
						Samples:          make([]mimirpb.Sample, len(input.Samples)),
						Histograms:       make([]mimirpb.Histogram, len(input.Histograms)),
						CreatedTimestamp: input.CreatedTimestamp,
					}
					copy(ts.Samples, input.Samples)
					copy(ts.Histograms, input.Histograms)
					writeReq := mimirpb.WriteRequest{
						Timeseries: []mimirpb.PreallocTimeseries{
							{
								TimeSeries: ts,
							},
						},
					}
					data, err := writeReq.Marshal()
					require.NoError(t, err)
					rec := &kgo.Record{
						Key:   []byte(user),
						Value: data,
					}
					err = builder.Process(context.Background(), rec)
					require.NoError(t, err)
				}
				// Verify.
				db, err := builder.getOrCreateTSDB(tsdbTenant{tenantID: user})
				require.NoError(t, err)

				querier, err := db.Querier(math.MinInt64, math.MaxInt64)
				require.NoError(t, err)
				ss := querier.Select(
					context.Background(), true, nil,
					labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, metricName),
				)

				require.True(t, ss.Next(), "DB has a series matching %s{}", metricName)
				series := ss.At()

				it := series.Iterator(nil)
				actual := []test.Sample{}
				for typ := it.Next(); typ != chunkenc.ValNone; typ = it.Next() {
					switch typ {
					case chunkenc.ValFloat:
						ts, val := it.At()
						actual = append(actual, test.Sample{
							TS:  ts,
							Val: val,
						})
					case chunkenc.ValHistogram:
						ts, h := it.AtHistogram(nil)
						h.CounterResetHint = histogram.UnknownCounterReset // Don't care.
						actual = append(actual, test.Sample{
							TS:   ts,
							Hist: h,
						})
					case chunkenc.ValFloatHistogram:
						ts, fh := it.AtFloatHistogram(nil)
						fh.CounterResetHint = histogram.UnknownCounterReset // Don't care.
						actual = append(actual, test.Sample{
							TS:        ts,
							FloatHist: fh,
						})
					default:
						t.Fatalf("unexpected sample type %v", typ)
					}
				}
				require.NoError(t, it.Err())
				require.Equal(t, tc.expectSamples, actual)
				discarded += tc.expectDiscarded
				expectMetrics := ""
				if discarded > 0 {
					expectMetrics = fmt.Sprintf(`# HELP cortex_blockbuilder_tsdb_process_samples_discarded_total The total number of samples that were discarded while processing records in one partition.
					# TYPE cortex_blockbuilder_tsdb_process_samples_discarded_total counter
					cortex_blockbuilder_tsdb_process_samples_discarded_total{partition="0"} %d
				`, discarded)
				}
				require.NoError(t, testutil.GatherAndCompare(registry, strings.NewReader(expectMetrics), "cortex_blockbuilder_tsdb_process_samples_discarded_total"))
			})
		}
		tcNumber++
	}
}
