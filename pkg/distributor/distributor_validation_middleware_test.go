// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"cmp"
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
	util_test "github.com/grafana/mimir/pkg/util/test"
	"github.com/grafana/mimir/pkg/util/validation"
)

func TestDistributor_prePushValidationMiddleware(t *testing.T) {
	const (
		pastGracePeriod     = time.Hour
		creationGracePeriod = 10 * time.Minute
		oooWindow           = 30 * time.Minute
		maxBuckets          = 1

		defaultUser    = "user"
		oooUser        = "user-ooo-window"
		maxBucketsUser = "user-max-buckets"
		exemplarsUser  = "user-exemplars"
	)

	now := time.Now()
	var (
		currentTS = now.UnixMilli()
		otherTS   = now.Add(-time.Minute).UnixMilli()
		tooOldTS  = now.Add(-2 * pastGracePeriod).UnixMilli()
		tooNewTS  = now.Add(2 * creationGracePeriod).UnixMilli()
		// beyondOOOTS is inside the past grace period but outside the out-of-order window.
		beyondOOOTS = now.Add(-pastGracePeriod).UnixMilli()

		validLabels    = []string{model.MetricNameLabel, "series"}
		validHistogram = util_test.GenerateTestHistogram(1)

		tooOldErr = fmt.Sprintf(sampleTimestampTooOldMsgFormat, tooOldTS, "series")
		tooNewErr = fmt.Sprintf(sampleTimestampTooNewMsgFormat, tooNewTS, "series")
	)
	invalidSchemaHistogram := makeHistograms(currentTS, validHistogram)
	invalidSchemaHistogram[0].Schema = 100

	type series struct {
		labels     []string // defaults to validLabels
		samples    []mimirpb.Sample
		histograms []mimirpb.Histogram
		exemplars  []mimirpb.Exemplar
	}

	testCases := []struct {
		name         string
		user         string
		input        []series
		want         []series
		wantErr      string
		wantSoftErr  bool
		wantRejected map[string]int // reason -> discarded samples
	}{
		{
			name: "all samples valid",
			input: []series{{samples: []mimirpb.Sample{
				{TimestampMs: otherTS, Value: 1},
				{TimestampMs: currentTS, Value: 2},
			}}},
			want: []series{{samples: []mimirpb.Sample{
				{TimestampMs: otherTS, Value: 1},
				{TimestampMs: currentTS, Value: 2},
			}}},
		},
		{
			name: "all samples invalid",
			input: []series{{samples: []mimirpb.Sample{
				{TimestampMs: tooOldTS, Value: 1},
				{TimestampMs: tooNewTS, Value: 2},
			}}},
			want:         nil,
			wantErr:      tooOldErr,
			wantRejected: map[string]int{reasonTooFarInPast: 1, reasonTooFarInFuture: 1},
		},
		{
			name: "valid and invalid samples",
			input: []series{{samples: []mimirpb.Sample{
				{TimestampMs: tooOldTS, Value: 1},
				{TimestampMs: otherTS, Value: 2},
				{TimestampMs: tooNewTS, Value: 3},
				{TimestampMs: currentTS, Value: 4},
			}}},
			want: []series{{samples: []mimirpb.Sample{
				{TimestampMs: otherTS, Value: 2},
				{TimestampMs: currentTS, Value: 4},
			}}},
			wantErr:      tooOldErr,
			wantSoftErr:  true,
			wantRejected: map[string]int{reasonTooFarInPast: 1, reasonTooFarInFuture: 1},
		},
		{
			name: "valid and invalid histograms",
			input: []series{{histograms: append(
				makeHistograms(tooOldTS, validHistogram),
				makeHistograms(currentTS, validHistogram)...,
			)}},
			want:         []series{{histograms: makeHistograms(currentTS, validHistogram)}},
			wantErr:      tooOldErr,
			wantSoftErr:  true,
			wantRejected: map[string]int{reasonTooFarInPast: 1},
		},
		{
			name: "invalid sample in one series does not affect valid series",
			input: []series{
				{samples: []mimirpb.Sample{{TimestampMs: tooOldTS, Value: 1}}},
				{samples: []mimirpb.Sample{{TimestampMs: currentTS, Value: 2}}},
			},
			want:         []series{{samples: []mimirpb.Sample{{TimestampMs: currentTS, Value: 2}}}},
			wantErr:      tooOldErr,
			wantSoftErr:  true,
			wantRejected: map[string]int{reasonTooFarInPast: 1},
		},
		{
			name: "sample too far in past",
			input: []series{{samples: []mimirpb.Sample{
				{TimestampMs: tooOldTS, Value: 1},
				{TimestampMs: currentTS, Value: 2},
			}}},
			want:         []series{{samples: []mimirpb.Sample{{TimestampMs: currentTS, Value: 2}}}},
			wantErr:      tooOldErr,
			wantSoftErr:  true,
			wantRejected: map[string]int{reasonTooFarInPast: 1},
		},
		{
			name: "sample too far in future",
			input: []series{{samples: []mimirpb.Sample{
				{TimestampMs: currentTS, Value: 1},
				{TimestampMs: tooNewTS, Value: 2},
			}}},
			want:         []series{{samples: []mimirpb.Sample{{TimestampMs: currentTS, Value: 1}}}},
			wantErr:      tooNewErr,
			wantSoftErr:  true,
			wantRejected: map[string]int{reasonTooFarInFuture: 1},
		},
		{
			name: "sample outside the out-of-order window",
			user: oooUser,
			input: []series{{samples: []mimirpb.Sample{
				{TimestampMs: beyondOOOTS, Value: 1},
				{TimestampMs: currentTS, Value: 2},
			}}},
			want:         []series{{samples: []mimirpb.Sample{{TimestampMs: currentTS, Value: 2}}}},
			wantErr:      fmt.Sprintf(sampleTimestampTooOldOOOMsgFormat, beyondOOOTS, "series"),
			wantSoftErr:  true,
			wantRejected: map[string]int{reasonSampleTimestampTooOld: 1},
		},
		{
			name: "duplicate samples not treated as rejected",
			input: []series{{samples: []mimirpb.Sample{
				{TimestampMs: currentTS, Value: 1},
				{TimestampMs: currentTS, Value: 1},
			}}},
			want:         []series{{samples: []mimirpb.Sample{{TimestampMs: currentTS, Value: 1}}}},
			wantRejected: map[string]int{reasonDuplicateTimestamp: 1},
		},
		{
			name: "duplicate histograms not treated as rejected",
			input: []series{{histograms: append(
				makeHistograms(currentTS, validHistogram),
				makeHistograms(currentTS, validHistogram)...,
			)}},
			want:         []series{{histograms: makeHistograms(currentTS, validHistogram)}},
			wantRejected: map[string]int{reasonDuplicateTimestamp: 1},
		},
		{
			name: "duplicate and invalid samples in the same series",
			input: []series{{samples: []mimirpb.Sample{
				{TimestampMs: tooOldTS, Value: 1},
				{TimestampMs: currentTS, Value: 2},
				{TimestampMs: currentTS, Value: 2},
			}}},
			want:         []series{{samples: []mimirpb.Sample{{TimestampMs: currentTS, Value: 2}}}},
			wantErr:      tooOldErr,
			wantSoftErr:  true,
			wantRejected: map[string]int{reasonTooFarInPast: 1, reasonDuplicateTimestamp: 1},
		},
		{
			name: "invalid float samples and valid histograms in the same series",
			input: []series{{
				samples:    []mimirpb.Sample{{TimestampMs: tooOldTS, Value: 1}},
				histograms: makeHistograms(currentTS, validHistogram),
			}},
			want:         []series{{histograms: makeHistograms(currentTS, validHistogram)}},
			wantErr:      tooOldErr,
			wantSoftErr:  true,
			wantRejected: map[string]int{reasonTooFarInPast: 1},
		},
		{
			name: "valid float samples and invalid histograms in the same series",
			input: []series{{
				samples:    []mimirpb.Sample{{TimestampMs: currentTS, Value: 1}},
				histograms: makeHistograms(tooNewTS, validHistogram),
			}},
			want:         []series{{samples: []mimirpb.Sample{{TimestampMs: currentTS, Value: 1}}}},
			wantErr:      tooNewErr,
			wantSoftErr:  true,
			wantRejected: map[string]int{reasonTooFarInFuture: 1},
		},
		{
			name: "histogram with invalid schema and valid samples",
			input: []series{{
				samples:    []mimirpb.Sample{{TimestampMs: currentTS, Value: 1}},
				histograms: invalidSchemaHistogram,
			}},
			want:         []series{{samples: []mimirpb.Sample{{TimestampMs: currentTS, Value: 1}}}},
			wantErr:      fmt.Sprintf(invalidSchemaNativeHistogramMsgFormat, 100),
			wantSoftErr:  true,
			wantRejected: map[string]int{reasonInvalidNativeHistogramSchema: 1},
		},
		{
			name: "histogram with too many buckets and valid samples",
			user: maxBucketsUser,
			input: []series{{
				samples: []mimirpb.Sample{{TimestampMs: currentTS, Value: 1}},
				// validHistogram is invalid here because of maxBucketsUser limits
				histograms: makeHistograms(currentTS, validHistogram),
			}},
			want: []series{{samples: []mimirpb.Sample{{TimestampMs: currentTS, Value: 1}}}},
			wantErr: fmt.Sprintf(maxNativeHistogramBucketsMsgFormat,
				currentTS, mimirpb.FromLabelAdaptersToString(makeTimeseries(validLabels, nil, nil, nil).Labels),
				makeHistograms(currentTS, validHistogram)[0].BucketCount(), maxBuckets),
			wantSoftErr:  true,
			wantRejected: map[string]int{reasonMaxNativeHistogramBuckets: 1},
		},
		{
			name: "series with invalid labels is dropped and valid series is kept",
			input: []series{
				{
					labels:  []string{"foo", "bar"},
					samples: []mimirpb.Sample{{TimestampMs: otherTS, Value: 1}},
				},
				{samples: []mimirpb.Sample{{TimestampMs: currentTS, Value: 3}}},
			},
			want:         []series{{samples: []mimirpb.Sample{{TimestampMs: currentTS, Value: 3}}}},
			wantErr:      noMetricNameMsgFormat,
			wantSoftErr:  true,
			wantRejected: map[string]int{reasonMissingMetricName: 1},
		},
		{
			name: "series with invalid labels counts all of its samples",
			input: []series{
				{
					labels: []string{"foo", "bar"},
					samples: []mimirpb.Sample{
						{TimestampMs: otherTS - 2, Value: 1},
						{TimestampMs: otherTS - 1, Value: 2},
						{TimestampMs: otherTS, Value: 3},
					},
				},
				{samples: []mimirpb.Sample{{TimestampMs: currentTS, Value: 4}}},
			},
			want:         []series{{samples: []mimirpb.Sample{{TimestampMs: currentTS, Value: 4}}}},
			wantErr:      noMetricNameMsgFormat,
			wantSoftErr:  true,
			wantRejected: map[string]int{reasonMissingMetricName: 3},
		},
		{
			name: "series with invalid labels counts all of its samples and histograms",
			input: []series{
				{
					labels: []string{"foo", "bar"},
					samples: []mimirpb.Sample{
						{TimestampMs: otherTS - 1, Value: 1},
						{TimestampMs: otherTS, Value: 2},
					},
					histograms: append(
						makeHistograms(currentTS-1, validHistogram),
						makeHistograms(currentTS, validHistogram)...,
					),
				},
				{samples: []mimirpb.Sample{{TimestampMs: currentTS, Value: 3}}},
			},
			want:         []series{{samples: []mimirpb.Sample{{TimestampMs: currentTS, Value: 3}}}},
			wantErr:      noMetricNameMsgFormat,
			wantSoftErr:  true,
			wantRejected: map[string]int{reasonMissingMetricName: 4},
		},
		{
			name: "all samples dropped due to invalid labels are counted by reason",
			input: []series{
				{
					labels: []string{"foo", "bar"},
					samples: []mimirpb.Sample{
						{TimestampMs: otherTS, Value: 1},
						{TimestampMs: currentTS, Value: 2},
					},
				},
				{
					labels: []string{model.MetricNameLabel, "\xffinvalid"},
					samples: []mimirpb.Sample{
						{TimestampMs: otherTS - 1, Value: 3},
						{TimestampMs: otherTS, Value: 4},
						{TimestampMs: currentTS, Value: 5},
					},
				},
			},
			want:    nil,
			wantErr: noMetricNameMsgFormat,
			wantRejected: map[string]int{
				reasonMissingMetricName: 2,
				reasonInvalidMetricName: 3,
			},
		},
		{
			name: "exemplars are kept on series that lost samples",
			user: exemplarsUser,
			input: []series{{
				samples: []mimirpb.Sample{
					{TimestampMs: tooOldTS, Value: 1},
					{TimestampMs: currentTS, Value: 2},
				},
				exemplars: makeExemplars([]string{"trace_id", "abc"}, currentTS, 1),
			}},
			want: []series{{
				samples:   []mimirpb.Sample{{TimestampMs: currentTS, Value: 2}},
				exemplars: makeExemplars([]string{"trace_id", "abc"}, currentTS, 1),
			}},
			wantErr:      tooOldErr,
			wantSoftErr:  true,
			wantRejected: map[string]int{reasonTooFarInPast: 1},
		},
	}

	// Tenant limits are not merged with the defaults, so each user starts from a copy of them.
	overrides := validation.MockOverrides(func(defaults *validation.Limits, tenantLimits map[string]*validation.Limits) {
		defaults.PastGracePeriod = model.Duration(pastGracePeriod)
		defaults.CreationGracePeriod = model.Duration(creationGracePeriod)
		defaults.NativeHistogramsIngestionEnabled = true

		ooo := *defaults
		ooo.PastGracePeriod = 0
		ooo.OutOfOrderTimeWindow = model.Duration(oooWindow)
		ooo.EnforceOOOWindowOnDistributor = true
		tenantLimits[oooUser] = &ooo

		maxBucketsLimits := *defaults
		maxBucketsLimits.MaxNativeHistogramBuckets = maxBuckets
		maxBucketsLimits.ReduceNativeHistogramOverMaxBuckets = false
		tenantLimits[maxBucketsUser] = &maxBucketsLimits

		exemplars := *defaults
		exemplars.MaxGlobalExemplarsPerUser = 100
		tenantLimits[exemplarsUser] = &exemplars
	})

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			userID := cmp.Or(tc.user, defaultUser)
			ds, _, regs, _ := prepare(t, prepConfig{
				numDistributors: 1,
				overrides:       func(*validation.Limits) *validation.Overrides { return overrides },
			})
			require.Len(t, regs, 1)

			writeReq := &mimirpb.WriteRequest{}
			for _, s := range tc.input {
				seriesLbls := s.labels
				if seriesLbls == nil {
					seriesLbls = validLabels
				}
				writeReq.Timeseries = append(
					writeReq.Timeseries,
					makeTimeseries(seriesLbls, s.samples, s.histograms, s.exemplars),
				)
			}

			var forwarded []series
			next := func(_ context.Context, r *Request) error {
				req, err := r.WriteRequest()
				if err != nil {
					return err
				}
				for _, ts := range req.Timeseries {
					forwarded = append(forwarded, series{
						samples:    append([]mimirpb.Sample(nil), ts.Samples...),
						histograms: append([]mimirpb.Histogram(nil), ts.Histograms...),
						exemplars:  append([]mimirpb.Exemplar(nil), ts.Exemplars...),
					})
				}
				return nil
			}

			ctx := user.InjectOrgID(t.Context(), userID)
			err := ds[0].prePushValidationMiddleware(next)(ctx, NewParsedRequest(writeReq, 0))

			if tc.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, tc.wantErr)
				var vErr validationError
				require.ErrorAs(t, err, &vErr)
				require.Equal(t, tc.wantSoftErr, vErr.IsSoft())

				// Duplicates are discarded, but not rejected.
				var wantRejectedSamples int64
				for reason, n := range tc.wantRejected {
					if reason != reasonDuplicateTimestamp {
						wantRejectedSamples += int64(n)
					}
				}
				if tc.wantSoftErr {
					assert.Equal(t, wantRejectedSamples, vErr.rejectedSamples)
				}
			}

			// Normalize empty slices, which the middleware leaves behind when it removes all values.
			for i := range forwarded {
				if len(forwarded[i].exemplars) == 0 {
					forwarded[i].exemplars = nil
				}
				if len(forwarded[i].samples) == 0 {
					forwarded[i].samples = nil
				}
				if len(forwarded[i].histograms) == 0 {
					forwarded[i].histograms = nil
				}
			}
			assert.Equal(t, tc.want, forwarded)

			var expected strings.Builder
			if len(tc.wantRejected) > 0 {
				expected.WriteString("# HELP cortex_discarded_samples_total The total number of samples that were discarded.\n")
				expected.WriteString("# TYPE cortex_discarded_samples_total counter\n")
			}
			// Metrics are gathered in label order, so list the reasons sorted.
			reasons := make([]string, 0, len(tc.wantRejected))
			for reason := range tc.wantRejected {
				reasons = append(reasons, reason)
			}
			slices.Sort(reasons)
			for _, reason := range reasons {
				fmt.Fprintf(&expected, "cortex_discarded_samples_total{group=\"\",reason=%q,user=%q} %d\n", reason, userID, tc.wantRejected[reason])
			}
			assert.NoError(t, testutil.GatherAndCompare(regs[0], strings.NewReader(expected.String()), "cortex_discarded_samples_total"))
		})
	}
}

func BenchmarkDistributor_prePushValidationMiddleware(b *testing.B) {
	const pastGracePeriod = time.Hour

	ctx := user.InjectOrgID(context.Background(), "user")

	var limits validation.Limits
	flagext.DefaultValues(&limits)
	limits.PastGracePeriod = model.Duration(pastGracePeriod)
	limits.NativeHistogramsIngestionEnabled = true
	limits.IngestionRate = math.MaxFloat64
	limits.IngestionBurstSize = math.MaxInt32

	ds, _, _, _ := prepare(b, prepConfig{numDistributors: 1, limits: &limits})
	noop := func(context.Context, *Request) error { return nil }
	fn := ds[0].prePushValidationMiddleware(noop)

	now := time.Now()
	validTS := now.UnixMilli()
	invalidTS := now.Add(-2 * pastGracePeriod).UnixMilli()
	histogram := util_test.GenerateTestHistogram(1)

	build := func(histograms bool, numSeries, valuesPerSeries, invalidPercent int, shuffle bool) *mimirpb.WriteRequest {
		rnd := rand.New(rand.NewPCG(1, 2))
		ts := make([]mimirpb.PreallocTimeseries, 0, numSeries)
		for s := 0; s < numSeries; s++ {
			numInvalid := ((s+1)*valuesPerSeries*invalidPercent)/100 - (s*valuesPerSeries*invalidPercent)/100

			timestamps := make([]int64, valuesPerSeries)
			for i := range timestamps {
				if i < numInvalid {
					timestamps[i] = invalidTS + int64(i)
				} else {
					timestamps[i] = validTS - int64(valuesPerSeries-i)
				}
			}
			if shuffle {
				rnd.Shuffle(len(timestamps), func(i, j int) { timestamps[i], timestamps[j] = timestamps[j], timestamps[i] })
			}

			var samples []mimirpb.Sample
			var hs []mimirpb.Histogram
			for i, timestamp := range timestamps {
				if histograms {
					hs = append(hs, mimirpb.FromHistogramToHistogramProto(timestamp, histogram))
				} else {
					samples = append(samples, mimirpb.Sample{TimestampMs: timestamp, Value: float64(i)})
				}
			}
			ts = append(ts, makeTimeseries([]string{model.MetricNameLabel, fmt.Sprintf("series_%d", s)}, samples, hs, nil))
		}
		return &mimirpb.WriteRequest{Timeseries: ts}
	}

	shapes := []struct {
		name            string
		numSeries       int
		valuesPerSeries int
	}{
		{"series=1000/values=1", 1000, 1},
		{"series=100/values=10", 100, 10},
		{"series=10/values=100", 10, 100},
	}
	validities := []struct {
		name           string
		invalidPercent int
	}{
		{"all_valid", 0},
		{"all_invalid", 100},
		{"mixed_10%_invalid", 10},
	}

	for _, histograms := range []bool{false, true} {
		kind := "samples"
		if histograms {
			kind = "histograms"
		}
		for _, shape := range shapes {
			for _, shuffle := range []bool{false, true} {
				if shuffle && shape.valuesPerSeries == 1 {
					// There is no order to a single value.
					continue
				}
				order := "sorted"
				if shuffle {
					order = "shuffled"
				}
				for _, validity := range validities {
					b.Run(fmt.Sprintf("%s/%s/%s/%s", kind, shape.name, order, validity.name), func(b *testing.B) {
						b.ReportAllocs()
						b.ResetTimer()
						for n := 0; n < b.N; n++ {
							// The middleware modifies the request in place and returns removed series
							// to the pool, so each iteration needs a freshly built one.
							b.StopTimer()
							req := build(histograms, shape.numSeries, shape.valuesPerSeries, validity.invalidPercent, shuffle)
							pushReq := NewParsedRequest(req, req.Size())
							b.StartTimer()

							err := fn(ctx, pushReq)
							if validity.invalidPercent > 0 {
								var vErr validationError
								require.ErrorAs(b, err, &vErr)
							} else {
								require.NoError(b, err)
							}
						}
					})
				}
			}
		}
	}
}
