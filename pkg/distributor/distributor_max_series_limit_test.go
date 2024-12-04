// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
)

func TestDistributor_Push_ShouldEnforceMaxSeriesLimits(t *testing.T) {
	const (
		userID = "user-1"
	)

	var (
		now        = time.Now()
		testConfig = prepConfig{
			numDistributors:         1,
			ingestStorageEnabled:    true,
			ingestStoragePartitions: 1,
			limits:                  prepareDefaultLimits(),
			configure: func(cfg *Config) {
				// Run a number of clients equal to the number of partitions, so that each partition
				// has its own client, as requested by some test cases.
				cfg.IngestStorageConfig.KafkaConfig.WriteClients = 3
			},
		}
	)

	// To keep this test simpler, every test case creates the same write request.
	createWriteRequest := func() *mimirpb.WriteRequest {
		return &mimirpb.WriteRequest{
			Timeseries: []mimirpb.PreallocTimeseries{
				makeTimeseries([]string{model.MetricNameLabel, "series_1"}, makeSamples(now.UnixMilli(), 1), makeExemplars([]string{"trace_id", "xxx"}, now.UnixMilli(), 1)),
				makeTimeseries([]string{model.MetricNameLabel, "series_2"}, makeSamples(now.UnixMilli(), 2), nil),
				makeTimeseries([]string{model.MetricNameLabel, "series_3"}, makeSamples(now.UnixMilli(), 3), nil),
				makeTimeseries([]string{model.MetricNameLabel, "series_4"}, makeSamples(now.UnixMilli(), 4), nil),
				makeTimeseries([]string{model.MetricNameLabel, "series_5"}, makeSamples(now.UnixMilli(), 5), nil),
			},
		}
	}

	// Pre-compute the series hashes.
	sampleReq := createWriteRequest()
	series1Hash := labels.StableHash(mimirpb.FromLabelAdaptersToLabels(sampleReq.Timeseries[0].Labels))
	series2Hash := labels.StableHash(mimirpb.FromLabelAdaptersToLabels(sampleReq.Timeseries[1].Labels))
	series3Hash := labels.StableHash(mimirpb.FromLabelAdaptersToLabels(sampleReq.Timeseries[2].Labels))
	series4Hash := labels.StableHash(mimirpb.FromLabelAdaptersToLabels(sampleReq.Timeseries[3].Labels))
	series5Hash := labels.StableHash(mimirpb.FromLabelAdaptersToLabels(sampleReq.Timeseries[4].Labels))

	testCases := map[string]struct {
		trackSeriesRejectedHashes []uint64
		trackSeriesErr            error
		expectedAcceptedSeries    []string
	}{
		"no series rejected": {
			expectedAcceptedSeries: []string{"series_1", "series_2", "series_3", "series_4", "series_5"},
		},
		"some series rejected": {
			trackSeriesRejectedHashes: []uint64{series4Hash, series2Hash}, // Rejected hashes in different order than series in the request.
			expectedAcceptedSeries:    []string{"series_1", "series_3", "series_5"},
		},
		"all series rejected": {
			trackSeriesRejectedHashes: []uint64{series1Hash, series2Hash, series3Hash, series4Hash, series5Hash},
			expectedAcceptedSeries:    []string{},
		},
		"failed to track series via usage-tracker": {
			trackSeriesErr: errors.New("usage-tracker service is unavailable"),
		},
	}

	for testName, testData := range testCases {
		testData := testData

		t.Run(testName, func(t *testing.T) {
			t.Parallel()

			distributors, _, regs, kafkaCluster := prepare(t, testConfig)
			require.Len(t, distributors, 1)
			require.Len(t, regs, 1)

			// Enable the usage-tracker using a client mock.
			usageTracker := &usageTrackerClientMock{}
			usageTracker.On("TrackSeries", mock.Anything, userID, mock.Anything).Return(testData.trackSeriesRejectedHashes, testData.trackSeriesErr)

			distributors[0].cfg.UsageTrackerEnabled = true
			distributors[0].usageTrackerClient = usageTracker

			// Send write request.
			ctx := user.InjectOrgID(context.Background(), userID)
			res, err := distributors[0].Push(ctx, createWriteRequest())
			if testData.trackSeriesErr == nil {
				require.NoError(t, err)
				require.NotNil(t, res)
			} else {
				require.ErrorContains(t, err, "failed to enforce max series limit")
				require.Nil(t, res)
			}

			// We expect TrackSeries() has been called with all input series hashes, in the same order.
			usageTracker.AssertNumberOfCalls(t, "TrackSeries", 1)
			usageTracker.AssertCalled(t, "TrackSeries", mock.Anything, userID, []uint64{series1Hash, series2Hash, series3Hash, series4Hash, series5Hash})

			// Ensure the expected series has been correctly written to partitions.
			actualSeriesByPartition := readAllMetricNamesByPartitionFromKafka(t, kafkaCluster.ListenAddrs(), testConfig.ingestStoragePartitions, time.Second)
			if len(testData.expectedAcceptedSeries) == 0 {
				require.Empty(t, actualSeriesByPartition)
			} else {
				require.Equal(t, map[int32][]string{0: testData.expectedAcceptedSeries}, actualSeriesByPartition)
			}

			// Ensure the number of discarded samples has been correctly tracked.
			if expectedDiscardedSamples := len(testData.trackSeriesRejectedHashes); expectedDiscardedSamples > 0 {
				require.NoError(t, testutil.GatherAndCompare(regs[0], strings.NewReader(fmt.Sprintf(`
					# HELP cortex_discarded_samples_total The total number of samples that were discarded.
					# TYPE cortex_discarded_samples_total counter
					cortex_discarded_samples_total{group="",reason="per_user_series_limit",user="user-1"} %d
				`, expectedDiscardedSamples)), "cortex_discarded_samples_total"))
			} else {
				require.NoError(t, testutil.GatherAndCompare(regs[0], strings.NewReader(""), "cortex_discarded_samples_total"))
			}
		})
	}
}

func BenchmarkDistributor_prePushMaxSeriesLimitMiddleware(b *testing.B) {
	var (
		now                 = time.Now()
		userID              = "user-1"
		ctx                 = user.InjectOrgID(context.Background(), userID)
		numSeriesPerRequest = 1000

		testConfig = prepConfig{
			numDistributors:         1,
			ingestStorageEnabled:    true,
			ingestStoragePartitions: 1,
			limits:                  prepareDefaultLimits(),
			configure: func(cfg *Config) {
				// Run a number of clients equal to the number of partitions, so that each partition
				// has its own client, as requested by some test cases.
				cfg.IngestStorageConfig.KafkaConfig.WriteClients = 3
			},
		}
	)

	testCases := map[string]struct {
		rejectedSeriesPercentage float64
	}{
		"no series rejected": {
			rejectedSeriesPercentage: 0,
		},
		"10% series rejected": {
			rejectedSeriesPercentage: 0.1,
		},
		"50% series rejected": {
			rejectedSeriesPercentage: 0.5,
		},
		"all series rejected": {
			rejectedSeriesPercentage: 1,
		},
	}

	for testName, testData := range testCases {
		b.Run(testName, func(b *testing.B) {
			// Pre-generate all write requests that will be used in this test.
			reqs := make([]*mimirpb.WriteRequest, 0, b.N)
			for r := 0; r < b.N; r++ {
				req := &mimirpb.WriteRequest{
					Timeseries: make([]mimirpb.PreallocTimeseries, 0, numSeriesPerRequest),
				}

				for s := 0; s < numSeriesPerRequest; s++ {
					req.Timeseries = append(req.Timeseries, makeTimeseries([]string{model.MetricNameLabel, fmt.Sprintf("series_%d", s)}, makeSamples(now.UnixMilli(), float64(s)), nil))
				}

				reqs = append(reqs, req)
			}

			// Create a distributor.
			distributors, _, _, _ := prepare(b, testConfig)
			require.Len(b, distributors, 1)

			// Enable the usage-tracker using a mock.
			usageTracker := &usageTrackerClientRejectionMock{rejectedSeriesPercentage: testData.rejectedSeriesPercentage}

			distributors[0].cfg.UsageTrackerEnabled = true
			distributors[0].usageTrackerClient = usageTracker

			// Get the middleware function.
			noop := func(ctx context.Context, req *Request) error { return nil }
			fn := distributors[0].prePushMaxSeriesLimitMiddleware(noop)

			b.ResetTimer()

			for n := 0; n < b.N; n++ {
				err := fn(ctx, newRequest(func() (req *mimirpb.WriteRequest, cleanup func(), err error) { return reqs[n], func() {}, nil }))
				if err != nil {
					b.Fatal(err.Error())
				}
			}
		})
	}
}

type usageTrackerClientMock struct {
	mock.Mock
	services.Service
}

func (m *usageTrackerClientMock) TrackSeries(ctx context.Context, userID string, series []uint64) ([]uint64, error) {
	args := m.Called(ctx, userID, series)
	return args.Get(0).([]uint64), args.Error(1)
}

type usageTrackerClientRejectionMock struct {
	services.Service

	// The percentage (0,1) of series to reject.
	rejectedSeriesPercentage float64
}

func (m *usageTrackerClientRejectionMock) TrackSeries(_ context.Context, _ string, series []uint64) ([]uint64, error) {
	if m.rejectedSeriesPercentage <= 0 {
		return nil, nil
	}
	if m.rejectedSeriesPercentage >= 1 {
		return slices.Clone(series), nil
	}

	rejected := make([]uint64, int(float64(len(series))*m.rejectedSeriesPercentage))
	copy(rejected, series[len(series)-len(rejected):])

	return rejected, nil
}
