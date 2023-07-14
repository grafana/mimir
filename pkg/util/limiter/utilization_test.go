// SPDX-License-Identifier: AGPL-3.0-only

package limiter

import (
	"math"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUtilizationBasedLimiter(t *testing.T) {
	const gigabyte = 1024 * 1024 * 1024

	setup := func(t *testing.T, cpuLimit float64, memoryLimit uint64) (*UtilizationBasedLimiter, *fakeUtilizationScanner) {
		fakeScanner := &fakeUtilizationScanner{}
		lim := NewUtilizationBasedLimiter(cpuLimit, memoryLimit, log.NewNopLogger(), prometheus.NewPedanticRegistry())
		lim.utilizationScanner = fakeScanner
		require.Empty(t, lim.LimitingReason(), "Limiting should initially be disabled")

		return lim, fakeScanner
	}

	tim := time.Now()

	t.Run("CPU based limiting should be enabled if set to a value greater than 0", func(t *testing.T) {
		lim, _ := setup(t, 0.11, gigabyte)

		// Warmup the CPU utilization.
		for i := 0; i < int(resourceUtilizationSlidingWindow.Seconds()); i++ {
			lim.compute(tim)
			tim = tim.Add(time.Second)
		}

		// The fake utilization scanner linearly increases CPU usage for a minute
		for i := 0; i < 59; i++ {
			lim.compute(tim)
			tim = tim.Add(time.Second)
			require.Empty(t, lim.LimitingReason(), "Limiting should be disabled")
		}
		lim.compute(tim)
		tim = tim.Add(time.Second)
		require.Equal(t, "cpu", lim.LimitingReason(), "Limiting should be enabled due to CPU")

		// The fake utilization scanner drops CPU usage again after a minute, so we expect
		// limiting to be disabled shortly.
		for i := 0; i < 5; i++ {
			lim.compute(tim)
			tim = tim.Add(time.Second)
		}
		require.Empty(t, lim.LimitingReason(), "Limiting should be disabled again")
	})

	t.Run("CPU based limiting should be disabled if set to 0", func(t *testing.T) {
		lim, _ := setup(t, 0, gigabyte)

		for i := 0; i < 60; i++ {
			lim.compute(tim)
			tim = tim.Add(time.Second)
			require.Empty(t, lim.LimitingReason(), "Limiting should be disabled")
		}
	})

	t.Run("memory based limiting should be enabled if set to a value greater than 0", func(t *testing.T) {
		lim, fakeScanner := setup(t, 0.11, gigabyte)

		// Compute the utilization a first time to warm up the limiter.
		lim.compute(tim)

		fakeScanner.memoryUtilization = gigabyte
		lim.compute(tim)
		tim = tim.Add(time.Second)
		require.Equal(t, "memory", lim.LimitingReason(), "Limiting should be enabled due to memory")

		fakeScanner.memoryUtilization = gigabyte - 1
		lim.compute(tim)
		require.Empty(t, lim.LimitingReason(), "Limiting should be disabled again")
	})

	t.Run("memory based limiting should be disabled if set to 0", func(t *testing.T) {
		lim, fakeScanner := setup(t, 0.11, 0)

		// Compute the utilization a first time to warm up the limiter.
		lim.compute(tim)

		fakeScanner.memoryUtilization = gigabyte
		lim.compute(tim)
		tim = tim.Add(time.Second)
		require.Empty(t, lim.LimitingReason(), "Limiting should be disabled")
	})
}

func TestFormatCPU(t *testing.T) {
	assert.Equal(t, "0.00", formatCPU(0))
	assert.Equal(t, "0.11", formatCPU(0.11))
	assert.Equal(t, "0.11", formatCPU(0.111))
	assert.Equal(t, "0.12", formatCPU(0.115))
	assert.Equal(t, "2.10", formatCPU(2.1))
}

func TestFormatCPULimit(t *testing.T) {
	assert.Equal(t, "disabled", formatCPULimit(0))
	assert.Equal(t, "0.11", formatCPULimit(0.111))
	assert.Equal(t, "0.12", formatCPULimit(0.115))
}

func TestFormatMemory(t *testing.T) {
	assert.Equal(t, "0", formatMemory(0))
	assert.Equal(t, "1073741824", formatMemory(1024*1024*1024))
	assert.Equal(t, "1073741825", formatMemory((1024*1024*1024)+1))
}

func TestFormatMemoryLimit(t *testing.T) {
	assert.Equal(t, "disabled", formatMemoryLimit(0))
	assert.Equal(t, "1073741824", formatMemoryLimit(1024*1024*1024))
	assert.Equal(t, "1073741825", formatMemoryLimit((1024*1024*1024)+1))
}

func TestUtilizationBasedLimiter_CPUUtilizationSensitivity(t *testing.T) {
	tests := map[string]struct {
		instantCPUValues          []float64
		expectedMaxCPUUtilization float64
	}{
		"2 minutes idle": {
			instantCPUValues:          generateConstCPUUtilization(120, 0),
			expectedMaxCPUUtilization: 0,
		},
		"2 minutes at constant utilization": {
			instantCPUValues:          generateConstCPUUtilization(120, 2.00),
			expectedMaxCPUUtilization: 2,
		},
		"1 minute idle + 10 seconds spike + 50 seconds idle": {
			instantCPUValues: func() []float64 {
				values := generateConstCPUUtilization(60, 0)
				values = append(values, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1)
				values = append(values, generateConstCPUUtilization(50, 0)...)
				return values
			}(),
			expectedMaxCPUUtilization: 1.49,
		},
		"10 seconds spike + 110 seconds idle (moving average warms up the first 60 seconds)": {
			instantCPUValues: func() []float64 {
				values := []float64{10, 9, 8, 7, 6, 5, 4, 3, 2, 1}
				values = append(values, generateConstCPUUtilization(110, 0)...)
				return values
			}(),
			expectedMaxCPUUtilization: 1.44,
		},
		"1 minute base utilization + 10 seconds spike + 50 seconds base utilization": {
			instantCPUValues: func() []float64 {
				values := generateConstCPUUtilization(60, 1.0)
				values = append(values, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1)
				values = append(values, generateConstCPUUtilization(50, 1.0)...)
				return values
			}(),
			expectedMaxCPUUtilization: 2.25,
		},
		"1 minute base utilization + 10 seconds steady spike + 50 seconds base utilization": {
			instantCPUValues: func() []float64 {
				values := generateConstCPUUtilization(60, 1.0)
				values = append(values, generateConstCPUUtilization(10, 10.0)...)
				values = append(values, generateConstCPUUtilization(50, 1.0)...)
				return values
			}(),
			expectedMaxCPUUtilization: 3.55,
		},
		"1 minute base utilization + 30 seconds steady spike + 30 seconds base utilization": {
			instantCPUValues: func() []float64 {
				values := generateConstCPUUtilization(60, 1.0)
				values = append(values, generateConstCPUUtilization(30, 10.0)...)
				values = append(values, generateConstCPUUtilization(30, 1.0)...)
				return values
			}(),
			expectedMaxCPUUtilization: 6.69,
		},
		"linear increase and then linear decrease utilization": {
			instantCPUValues: func() []float64 {
				values := generateLinearStepCPUUtilization(60, 0, 0.1)
				values = append(values, generateLinearStepCPUUtilization(60, 60*0.1, -0.1)...)
				return values
			}(),
			expectedMaxCPUUtilization: 4.13,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			scanner := &preRecordedUtilizationScanner{instantCPUValues: testData.instantCPUValues}

			lim := NewUtilizationBasedLimiter(1, 0, log.NewNopLogger(), prometheus.NewPedanticRegistry())
			lim.utilizationScanner = scanner

			minCPUUtilization := float64(math.MaxInt64)
			maxCPUUtilization := float64(math.MinInt64)

			for i, ts := 0, time.Now(); i < len(testData.instantCPUValues); i++ {
				currCPUUtilization, _ := lim.compute(ts)
				ts = ts.Add(time.Second)

				// Keep track of the max CPU utilization as computed by the limiter.
				if currCPUUtilization < minCPUUtilization {
					minCPUUtilization = currCPUUtilization
				}
				if currCPUUtilization > maxCPUUtilization {
					maxCPUUtilization = currCPUUtilization
				}
			}

			assert.InDelta(t, 0, minCPUUtilization, 0.01) // The minimum should always be 0 because of the warmup period.
			assert.InDelta(t, testData.expectedMaxCPUUtilization, maxCPUUtilization, 0.01)
		})
	}
}

type fakeUtilizationScanner struct {
	totalTime         float64
	counter           int
	memoryUtilization uint64
}

func (s *fakeUtilizationScanner) Scan() (float64, uint64, error) {
	s.totalTime += float64(1) / float64(60-s.counter)
	s.counter++
	s.counter %= 60
	return s.totalTime, s.memoryUtilization, nil
}

func (s *fakeUtilizationScanner) String() string {
	return "fake"
}

// preRecordedUtilizationScanner allows to replay CPU values.
type preRecordedUtilizationScanner struct {
	instantCPUValues []float64

	// Keeps track of the accumulated CPU utilization.
	totalCPUUtilization float64
}

func (s *preRecordedUtilizationScanner) Scan() (float64, uint64, error) {
	if len(s.instantCPUValues) == 0 {
		return s.totalCPUUtilization, 0, nil
	}

	s.totalCPUUtilization += s.instantCPUValues[0]
	s.instantCPUValues = s.instantCPUValues[1:]
	return s.totalCPUUtilization, 0, nil
}

func (s *preRecordedUtilizationScanner) String() string {
	return ""
}

func generateConstCPUUtilization(count int, value float64) []float64 {
	values := make([]float64, 0, count)
	for i := 0; i < count; i++ {
		values = append(values, value)
	}
	return values
}

func generateLinearStepCPUUtilization(count int, from, step float64) []float64 {
	values := make([]float64, 0, count)
	for i := 0; i < count; i++ {
		values = append(values, from+(float64(i)*step))
	}
	return values
}
