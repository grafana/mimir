// SPDX-License-Identifier: AGPL-3.0-only

package limiter

import (
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"
)

func TestUtilizationBasedLimiter(t *testing.T) {
	const cpuLimit = 0.11
	// 1 GB
	const memLimit = 1_073_741_824

	fakeScanner := &fakeUtilizationScanner{}
	lim := NewUtilizationBasedLimiter(cpuLimit, memLimit, log.NewNopLogger())
	lim.utilizationScanner = fakeScanner
	require.Empty(t, lim.LimitingReason(), "Limiting should initially be disabled")

	tim := time.Now()

	t.Run("CPU based limiting", func(t *testing.T) {
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

	t.Run("memory based limiting", func(t *testing.T) {
		fakeScanner.memoryUtilization = memLimit
		lim.compute(tim)
		tim = tim.Add(time.Second)
		require.Equal(t, "memory", lim.LimitingReason(), "Limiting should be enabled due to memory")

		fakeScanner.memoryUtilization = memLimit - 1
		lim.compute(tim)
		require.Empty(t, lim.LimitingReason(), "Limiting should be disabled again")
	})
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
