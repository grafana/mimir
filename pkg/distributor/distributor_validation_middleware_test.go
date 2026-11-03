// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
	util_test "github.com/grafana/mimir/pkg/util/test"
	"github.com/grafana/mimir/pkg/util/validation"
)

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
