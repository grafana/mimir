// SPDX-License-Identifier: AGPL-3.0-only

package continuoustest

import (
	"fmt"
	"strings"
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
	for _, histProfile := range histogramProfiles {
		testVerifySamplesSumHistograms(t, histProfile.generateValue, histProfile.generateSampleHistogram, fmt.Sprintf("generateHistogramValue for %s", histProfile.typeLabel))
	}
}

func testVerifySamplesSumFloats(t *testing.T, generateValue generateValueFunc, testLabel string) {
	// Round to millis since that's the precision of Prometheus timestamps.
	now := time.Now().Round(time.Millisecond).UTC()

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
			actualLastMatchingIdx, actualErr := verifySamplesSum(matrix, testData.expectedSeries, testData.expectedStep, generateValue, nil)
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
	now := time.Now().Round(time.Millisecond).UTC()

	if strings.HasSuffix(testLabel, "histogram_int_gauge") {
		// If you don't do this, should_return_error_if_there's_a_missing_series will sometimes fail when the last histogram has an expected value of 0, making the number of series irrelevant to the expected sum which will be 0. This causes the error to happen on the second iteration with lastMatchingIndex as 2 instead of the usual first iteration with lastMatchingIndex as -1. So while it still fails with the expected error, the test fails as the lastMatchingIndex is unexpected. To fix this, we make sure the timestamp for the last histogram does not result in an expected value of 0.
		for generateHistogramIntValue(now.Add(30*time.Second), true) == 0 {
			now = now.Add(1 * time.Second)
		}
	}

	tests := map[string]struct {
		histograms              []model.SampleHistogramPair
		expectedSeries          int
		expectedStep            time.Duration
		expectedLastMatchingIdx int
		expectedErr             string
	}{
		"should return no error if all histograms value and timestamp match the expected one (1 series)": {
			histograms: []model.SampleHistogramPair{
				newSampleHistogramPair(now.Add(10*time.Second), generateSampleHistogram(now.Add(10*time.Second), 1)),
				newSampleHistogramPair(now.Add(20*time.Second), generateSampleHistogram(now.Add(20*time.Second), 1)),
				newSampleHistogramPair(now.Add(30*time.Second), generateSampleHistogram(now.Add(30*time.Second), 1)),
			},
			expectedSeries:          1,
			expectedStep:            10 * time.Second,
			expectedLastMatchingIdx: 0,
			expectedErr:             "",
		},
		"should return no error if all histograms value and timestamp match the expected one (multiple series)": {
			histograms: []model.SampleHistogramPair{
				newSampleHistogramPair(now.Add(10*time.Second), generateSampleHistogram(now.Add(10*time.Second), 5)),
				newSampleHistogramPair(now.Add(20*time.Second), generateSampleHistogram(now.Add(20*time.Second), 5)),
				newSampleHistogramPair(now.Add(30*time.Second), generateSampleHistogram(now.Add(30*time.Second), 5)),
			},
			expectedSeries:          5,
			expectedStep:            10 * time.Second,
			expectedLastMatchingIdx: 0,
			expectedErr:             "",
		},
		"should return error if there's a missing series": {
			histograms: []model.SampleHistogramPair{
				newSampleHistogramPair(now.Add(10*time.Second), generateSampleHistogram(now.Add(10*time.Second), 4)),
				newSampleHistogramPair(now.Add(20*time.Second), generateSampleHistogram(now.Add(20*time.Second), 4)),
				newSampleHistogramPair(now.Add(30*time.Second), generateSampleHistogram(now.Add(30*time.Second), 4)),
			},
			expectedSeries:          5,
			expectedStep:            10 * time.Second,
			expectedLastMatchingIdx: -1,
			expectedErr:             "histogram at timestamp .* has sum .* while was expecting .*",
		},
		"should return error if there's a missing histogram": {
			histograms: []model.SampleHistogramPair{
				newSampleHistogramPair(now.Add(10*time.Second), generateSampleHistogram(now.Add(10*time.Second), 5)),
				newSampleHistogramPair(now.Add(30*time.Second), generateSampleHistogram(now.Add(30*time.Second), 5)),
			},
			expectedSeries:          5,
			expectedStep:            10 * time.Second,
			expectedLastMatchingIdx: 1,
			expectedErr:             "histogram at timestamp .* was expected to have timestamp .*",
		},
		"should return error if the 2nd last histogram has an unexpected timestamp": {
			histograms: []model.SampleHistogramPair{
				newSampleHistogramPair(now.Add(10*time.Second), generateSampleHistogram(now.Add(10*time.Second), 5)),
				newSampleHistogramPair(now.Add(21*time.Second), generateSampleHistogram(now.Add(21*time.Second), 5)),
				newSampleHistogramPair(now.Add(30*time.Second), generateSampleHistogram(now.Add(30*time.Second), 5)),
			},
			expectedSeries:          5,
			expectedStep:            10 * time.Second,
			expectedLastMatchingIdx: 2,
			expectedErr:             "histogram at timestamp .* was expected to have timestamp .*",
		},
	}

	for testName, testData := range tests {
		t.Run(fmt.Sprintf("%s:%s", testLabel, testName), func(t *testing.T) {
			matrix := model.Matrix{{Histograms: testData.histograms}}
			actualLastMatchingIdx, actualErr := verifySamplesSum(matrix, testData.expectedSeries, testData.expectedStep, generateValue, generateSampleHistogram)
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

func TestCompareSampleValues(t *testing.T) {
	tests := map[string]struct {
		actual    float64
		expected  float64
		tolerance float64
		result    bool
	}{
		"histogram_float_counter shouldn't work with float tolerance": {
			actual:    336157191.999972,
			expected:  336157192.000000,
			tolerance: maxComparisonDeltaFloat,
			result:    false,
		},
		"histogram_float_counter should work with histogram tolerance": {
			actual:    336157191.999972,
			expected:  336157192.000000,
			tolerance: maxComparisonDeltaHistogram,
			result:    true,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			res := compareFloatValues(testData.actual, testData.expected, testData.tolerance)
			assert.Equal(t, testData.result, res)
		})
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

// generateFloatSamplesSum generates a list of float samples whose timestamps range between from and to
// (both inclusive), where each sample value is numSeries multiplied by the expected value at the sample's timestamp.
func generateFloatSamplesSum(from, to time.Time, numSeries int, step time.Duration, generateValue generateValueFunc) []model.SamplePair {
	var samples []model.SamplePair

	for ts := from; !ts.After(to); ts = ts.Add(step) {
		samples = append(samples, newSamplePair(ts, float64(numSeries)*generateValue(ts)))
	}

	return samples
}

// generateHistogramSamplesSum generates a list of histogram samples whose timestamps range between from and to
// (both inclusive), where each histogram is the sum of numSeries instances of the expected histogram for its timestamp.
func generateHistogramSamplesSum(from, to time.Time, numSeries int, step time.Duration, generateSampleHistogram generateSampleHistogramFunc) []model.SampleHistogramPair {
	var samples []model.SampleHistogramPair

	for ts := from; !ts.After(to); ts = ts.Add(step) {
		samples = append(samples, newSampleHistogramPair(ts, generateSampleHistogram(ts, numSeries)))
	}

	return samples
}

func TestFormatExpectedAndActualValuesComparison(t *testing.T) {
	now := time.UnixMilli(1701142000000).UTC()

	testCases := map[string]struct {
		samples        []model.SamplePair
		series         int
		expectedOutput string
	}{
		"all values match, one expected series": {
			samples: []model.SamplePair{
				newSamplePair(now.Add(10*time.Second), generateSineWaveValue(now.Add(10*time.Second))),
				newSamplePair(now.Add(20*time.Second), generateSineWaveValue(now.Add(20*time.Second))),
				newSamplePair(now.Add(30*time.Second), generateSineWaveValue(now.Add(30*time.Second))),
			},
			series: 1,
			expectedOutput: `Timestamp      Expected  Actual
1701142010000 (2023-11-28T03:26:50Z)  1.0865  1.0865
1701142020000 (2023-11-28T03:27:00Z)  1.0489  1.0489
1701142030000 (2023-11-28T03:27:10Z)  1.0219  1.0219
`,
		},
		"all values match, multiple expected series": {
			samples: []model.SamplePair{
				newSamplePair(now.Add(10*time.Second), 3*generateSineWaveValue(now.Add(10*time.Second))),
				newSamplePair(now.Add(20*time.Second), 3*generateSineWaveValue(now.Add(20*time.Second))),
				newSamplePair(now.Add(30*time.Second), 3*generateSineWaveValue(now.Add(30*time.Second))),
			},
			series: 3,
			expectedOutput: `Timestamp      Expected  Actual
1701142010000 (2023-11-28T03:26:50Z)  3.2594  3.2594
1701142020000 (2023-11-28T03:27:00Z)  3.1468  3.1468
1701142030000 (2023-11-28T03:27:10Z)  3.0656  3.0656
`,
		},
		"one value differs": {
			samples: []model.SamplePair{
				newSamplePair(now.Add(10*time.Second), 3*generateSineWaveValue(now.Add(10*time.Second))),
				newSamplePair(now.Add(20*time.Second), 3*generateSineWaveValue(now.Add(20*time.Second))+1),
				newSamplePair(now.Add(30*time.Second), 3*generateSineWaveValue(now.Add(30*time.Second))),
			},
			series: 3,
			expectedOutput: `Timestamp      Expected  Actual
1701142010000 (2023-11-28T03:26:50Z)  3.2594  3.2594
1701142020000 (2023-11-28T03:27:00Z)  3.1468  4.1468  (value differs!)
1701142030000 (2023-11-28T03:27:10Z)  3.0656  3.0656
`,
		},
		"multiple values differ": {
			samples: []model.SamplePair{
				newSamplePair(now.Add(10*time.Second), 3*generateSineWaveValue(now.Add(10*time.Second))),
				newSamplePair(now.Add(20*time.Second), 3*generateSineWaveValue(now.Add(20*time.Second))+1),
				newSamplePair(now.Add(30*time.Second), 3*generateSineWaveValue(now.Add(30*time.Second))-1),
			},
			series: 3,
			expectedOutput: `Timestamp      Expected  Actual
1701142010000 (2023-11-28T03:26:50Z)  3.2594  3.2594
1701142020000 (2023-11-28T03:27:00Z)  3.1468  4.1468  (value differs!)
1701142030000 (2023-11-28T03:27:10Z)  3.0656  2.0656  (value differs!)
`,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			matrix := model.Matrix{{Values: testCase.samples}}
			output := formatExpectedAndActualValuesComparison(matrix, testCase.series, generateSineWaveValue)
			require.Equal(t, testCase.expectedOutput, output)
		})
	}
}
