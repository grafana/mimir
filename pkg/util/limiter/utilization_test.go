// SPDX-License-Identifier: AGPL-3.0-only

package limiter

import (
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUtilizationBasedLimiter(t *testing.T) {
	const gigabyte = 1024 * 1024 * 1024

	setup := func(t *testing.T, cpuLimit float64, memoryLimit uint64) (*UtilizationBasedLimiter, *fakeUtilizationScanner) {
		fakeScanner := &fakeUtilizationScanner{}
		lim := NewUtilizationBasedLimiter(cpuLimit, memoryLimit, log.NewNopLogger())
		lim.utilizationScanner = fakeScanner
		require.Empty(t, lim.LimitingReason(), "Limiting should initially be disabled")

		return lim, fakeScanner
	}

	tim := time.Now()

	t.Run("CPU based limiting should be enabled if set to a value greater than 0", func(t *testing.T) {
		lim, _ := setup(t, 0.11, gigabyte)

		// The fake utilization scanner linearly increases CPU usage for a minute
		for i := 0; i < 59; i++ {
			lim.compute(tim)
			tim = tim.Add(time.Second)
			require.Empty(t, lim.LimitingReason(), "Limiting should be disabled")
		}
		lim.compute(tim)
		tim = tim.Add(time.Second)
		require.Equal(t, "cpu", lim.LimitingReason(), "Limiting should be enabled due to CPU")

		// The fake utilization scanner drops CPU usage again after a minute
		lim.compute(tim)
		tim = tim.Add(time.Second)
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
