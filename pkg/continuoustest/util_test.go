// SPDX-License-Identifier: AGPL-3.0-only

package continuoustest

import (
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

func TestVerifySineWaveSamplesSum(t *testing.T) {
	// Round to millis since that's the precision of Prometheus timestamps.
	now := time.UnixMilli(time.Now().UnixMilli()).UTC()

	tests := map[string]struct {
		samples        []model.SamplePair
		expectedSeries int
		expectedStep   time.Duration
		expectedErr    string
	}{
		"should return no error if all samples value and timestamp match the expected one (1 series)": {
			samples: []model.SamplePair{
				newSamplePair(now.Add(10*time.Second), generateSineWaveValue(now.Add(10*time.Second))),
				newSamplePair(now.Add(20*time.Second), generateSineWaveValue(now.Add(20*time.Second))),
				newSamplePair(now.Add(30*time.Second), generateSineWaveValue(now.Add(30*time.Second))),
			},
			expectedSeries: 1,
			expectedStep:   10 * time.Second,
			expectedErr:    "",
		},
		"should return no error if all samples value and timestamp match the expected one (multiple series)": {
			samples: []model.SamplePair{
				newSamplePair(now.Add(10*time.Second), 5*generateSineWaveValue(now.Add(10*time.Second))),
				newSamplePair(now.Add(20*time.Second), 5*generateSineWaveValue(now.Add(20*time.Second))),
				newSamplePair(now.Add(30*time.Second), 5*generateSineWaveValue(now.Add(30*time.Second))),
			},
			expectedSeries: 5,
			expectedStep:   10 * time.Second,
			expectedErr:    "",
		},
		"should return error if there's a missing series": {
			samples: []model.SamplePair{
				newSamplePair(now.Add(10*time.Second), 4*generateSineWaveValue(now.Add(10*time.Second))),
				newSamplePair(now.Add(20*time.Second), 4*generateSineWaveValue(now.Add(20*time.Second))),
				newSamplePair(now.Add(30*time.Second), 4*generateSineWaveValue(now.Add(30*time.Second))),
			},
			expectedSeries: 5,
			expectedStep:   10 * time.Second,
			expectedErr:    "sample at timestamp .* has value .* while was expecting .*",
		},
		"should return error if there's a missing sample": {
			samples: []model.SamplePair{
				newSamplePair(now.Add(10*time.Second), 5*generateSineWaveValue(now.Add(10*time.Second))),
				newSamplePair(now.Add(30*time.Second), 5*generateSineWaveValue(now.Add(30*time.Second))),
			},
			expectedSeries: 5,
			expectedStep:   10 * time.Second,
			expectedErr:    "sample at timestamp .* was expected to have timestamp .*",
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			matrix := model.Matrix{{Values: testData.samples}}
			actual := verifySineWaveSamplesSum(matrix, testData.expectedSeries, testData.expectedStep)
			if testData.expectedErr == "" {
				assert.NoError(t, actual)
			} else {
				assert.Error(t, actual)
				assert.Regexp(t, testData.expectedErr, actual.Error())
			}
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
