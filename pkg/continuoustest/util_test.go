// SPDX-License-Identifier: AGPL-3.0-only

package continuoustest

import (
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAlignTimestampToInterval(t *testing.T) {
	assert.Equal(t, time.Unix(30, 0), alignTimestampToInterval(time.Unix(30, 0), 10*time.Second))
	assert.Equal(t, time.Unix(30, 0), alignTimestampToInterval(time.Unix(31, 0), 10*time.Second))
	assert.Equal(t, time.Unix(30, 0), alignTimestampToInterval(time.Unix(39, 0), 10*time.Second))
	assert.Equal(t, time.Unix(40, 0), alignTimestampToInterval(time.Unix(40, 0), 10*time.Second))
}

func TestGetQueryStep(t *testing.T) {
	tests := map[string]struct {
		start         time.Time
		end           time.Time
		writeInterval time.Duration
		expectedStep  time.Duration
	}{
		"should return write interval if expected number of samples is < 1000": {
			start:         time.UnixMilli(0),
			end:           time.UnixMilli(3600 * 1000),
			writeInterval: 10 * time.Second,
			expectedStep:  10 * time.Second,
		},
		"should align step to write interval and guarantee no more than 1000 samples": {
			start:         time.UnixMilli(0),
			end:           time.UnixMilli(86400 * 1000),
			writeInterval: 10 * time.Second,
			expectedStep:  90 * time.Second,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			actualStep := getQueryStep(testData.start, testData.end, testData.writeInterval)
			assert.Equal(t, testData.expectedStep, actualStep)
		})
	}
}

func TestVerifySamplesSum(t *testing.T) {
	testVerifySamplesSumFloats(t, generateSineWaveValue, "generateSineWaveValue")
	for i := 0; i < 4; i++ {
		testVerifySamplesSumHistograms(t, generateHistogramValue[i], generateSampleHistogram[i], fmt.Sprintf("generateHistogramValue[%d]", i))
	}
}

func testVerifySamplesSumFloats(t *testing.T, generateValue generateValueFunc, testLabel string) {
	// Round to millis since that's the precision of Prometheus timestamps.
	now := time.UnixMilli(time.Now().UnixMilli()).UTC()

	tests := map[string]struct {
		samples                 []model.SamplePair
		expectedSeries          int
		expectedStep            time.Duration
		expectedLastMatchingIdx int
		expectedErr             string
	}{
		"should return no error if all samples value and timestamp match the expected one (1 series)": {
			samples: []model.SamplePair{
				newSamplePair(now.Add(10*time.Second), generateValue(now.Add(10*time.Second))),
				newSamplePair(now.Add(20*time.Second), generateValue(now.Add(20*time.Second))),
				newSamplePair(now.Add(30*time.Second), generateValue(now.Add(30*time.Second))),
			},
			expectedSeries:          1,
			expectedStep:            10 * time.Second,
			expectedLastMatchingIdx: 0,
			expectedErr:             "",
		},
		"should return no error if all samples value and timestamp match the expected one (multiple series)": {
			samples: []model.SamplePair{
				newSamplePair(now.Add(10*time.Second), 5*generateValue(now.Add(10*time.Second))),
				newSamplePair(now.Add(20*time.Second), 5*generateValue(now.Add(20*time.Second))),
				newSamplePair(now.Add(30*time.Second), 5*generateValue(now.Add(30*time.Second))),
			},
			expectedSeries:          5,
			expectedStep:            10 * time.Second,
			expectedLastMatchingIdx: 0,
			expectedErr:             "",
		},
		"should return error if there's a missing series": {
			samples: []model.SamplePair{
				newSamplePair(now.Add(10*time.Second), 4*generateValue(now.Add(10*time.Second))),
				newSamplePair(now.Add(20*time.Second), 4*generateValue(now.Add(20*time.Second))),
				newSamplePair(now.Add(30*time.Second), 4*generateValue(now.Add(30*time.Second))),
			},
			expectedSeries:          5,
			expectedStep:            10 * time.Second,
			expectedLastMatchingIdx: -1,
			expectedErr:             "sample at timestamp .* has value .* while was expecting .*",
		},
		"should return error if there's a missing sample": {
			samples: []model.SamplePair{
				newSamplePair(now.Add(10*time.Second), 5*generateValue(now.Add(10*time.Second))),
				newSamplePair(now.Add(30*time.Second), 5*generateValue(now.Add(30*time.Second))),
			},
			expectedSeries:          5,
			expectedStep:            10 * time.Second,
			expectedLastMatchingIdx: 1,
			expectedErr:             "sample at timestamp .* was expected to have timestamp .*",
		},
		"should return error if the 2nd last sample has an unexpected timestamp": {
			samples: []model.SamplePair{
				newSamplePair(now.Add(10*time.Second), 5*generateValue(now.Add(10*time.Second))),
				newSamplePair(now.Add(21*time.Second), 5*generateValue(now.Add(21*time.Second))),
				newSamplePair(now.Add(30*time.Second), 5*generateValue(now.Add(30*time.Second))),
			},
			expectedSeries:          5,
			expectedStep:            10 * time.Second,
			expectedLastMatchingIdx: 2,
			expectedErr:             "sample at timestamp .* was expected to have timestamp .*",
		},
	}

	for testName, testData := range tests {
		t.Run(fmt.Sprintf("%s:%s", testLabel, testName), func(t *testing.T) {
			matrix := model.Matrix{{Values: testData.samples}}
			actualLastMatchingIdx, actualErr := verifySamplesSum(matrix, testData.expectedSeries, testData.expectedStep, generateValue)
			if testData.expectedErr == "" {
				assert.NoError(t, actualErr)
			} else {
				assert.Error(t, actualErr)
				assert.Regexp(t, testData.expectedErr, actualErr.Error())
			}
			assert.Equal(t, testData.expectedLastMatchingIdx, actualLastMatchingIdx)
		})
	}
}

func testVerifySamplesSumHistograms(t *testing.T, generateValue generateValueFunc, generateSampleHistogram generateSampleHistogramFunc, testLabel string) {
	// Round to millis since that's the precision of Prometheus timestamps.
	now := time.UnixMilli(time.Now().UnixMilli()).UTC()

	tests := map[string]struct {
		histograms              []model.SampleHistogramPair
		expectedSeries          int
		expectedStep            time.Duration
		expectedLastMatchingIdx int
		expectedErr             string
	}{
		"should return no error if all histograms value and timestamp match the expected one (1 series)": {
			histograms: []model.SampleHistogramPair{
				newSampleHistogramPair(now.Add(10*time.Second), generateSampleHistogram(now.Add(10*time.Second))),
				newSampleHistogramPair(now.Add(20*time.Second), generateSampleHistogram(now.Add(20*time.Second))),
				newSampleHistogramPair(now.Add(30*time.Second), generateSampleHistogram(now.Add(30*time.Second))),
			},
			expectedSeries:          1,
			expectedStep:            10 * time.Second,
			expectedLastMatchingIdx: 0,
			expectedErr:             "",
		},
		"should return error if there's a missing histogram": {
			histograms: []model.SampleHistogramPair{
				newSampleHistogramPair(now.Add(10*time.Second), generateSampleHistogram(now.Add(10*time.Second))),
				newSampleHistogramPair(now.Add(30*time.Second), generateSampleHistogram(now.Add(30*time.Second))),
			},
			expectedSeries:          1,
			expectedStep:            10 * time.Second,
			expectedLastMatchingIdx: 1,
			expectedErr:             "histogram at timestamp .* was expected to have timestamp .*",
		},
		"should return error if the 2nd last histogram has an unexpected timestamp": {
			histograms: []model.SampleHistogramPair{
				newSampleHistogramPair(now.Add(10*time.Second), generateSampleHistogram(now.Add(10*time.Second))),
				newSampleHistogramPair(now.Add(21*time.Second), generateSampleHistogram(now.Add(21*time.Second))),
				newSampleHistogramPair(now.Add(30*time.Second), generateSampleHistogram(now.Add(30*time.Second))),
			},
			expectedSeries:          1,
			expectedStep:            10 * time.Second,
			expectedLastMatchingIdx: 2,
			expectedErr:             "histogram at timestamp .* was expected to have timestamp .*",
		},
	}

	for testName, testData := range tests {
		t.Run(fmt.Sprintf("%s:%s", testLabel, testName), func(t *testing.T) {
			matrix := model.Matrix{{Histograms: testData.histograms}}
			actualLastMatchingIdx, actualErr := verifySamplesSum(matrix, testData.expectedSeries, testData.expectedStep, generateValue)
			if testData.expectedErr == "" {
				assert.NoError(t, actualErr)
			} else {
				assert.Error(t, actualErr)
				assert.Regexp(t, testData.expectedErr, actualErr.Error())
			}
			assert.Equal(t, testData.expectedLastMatchingIdx, actualLastMatchingIdx)
		})
	}
}

func TestMinTime(t *testing.T) {
	first := time.Now()
	second := first.Add(time.Second)

	assert.Equal(t, first, minTime(first, second))
	assert.Equal(t, first, minTime(second, first))
}

func TestMaxTime(t *testing.T) {
	first := time.Now()
	second := first.Add(time.Second)

	assert.Equal(t, second, maxTime(first, second))
	assert.Equal(t, second, maxTime(second, first))
}

func TestRandTime(t *testing.T) {
	min := time.Unix(1000, 0)
	max := time.Unix(10000, 0)

	for i := 0; i < 100; i++ {
		actual := randTime(min, max)
		require.GreaterOrEqual(t, actual.Unix(), min.Unix())
		require.LessOrEqual(t, actual.Unix(), max.Unix())
	}
}

func newSamplePair(ts time.Time, value float64) model.SamplePair {
	return model.SamplePair{
		Timestamp: model.Time(ts.UnixMilli()),
		Value:     model.SampleValue(value),
	}
}

func newSampleHistogramPair(ts time.Time, hist *model.SampleHistogram) model.SampleHistogramPair {
	return model.SampleHistogramPair{
		Timestamp: model.Time(ts.UnixMilli()),
		Histogram: hist,
	}
}

// generateSamplesSum generates a list of samples whose timestamps range between from and to (both included),
// where each sample value is numSeries multiplied by the expected value at the sample's timestamp.
func generateSamplesSum(from, to time.Time, numSeries int, step time.Duration, generateValue generateValueFunc) []model.SamplePair {
	var samples []model.SamplePair

	for ts := from; !ts.After(to); ts = ts.Add(step) {
		samples = append(samples, newSamplePair(ts, float64(numSeries)*generateValue(ts)))
	}

	return samples
}
