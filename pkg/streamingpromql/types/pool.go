// SPDX-License-Identifier: AGPL-3.0-only

package types

import (
	"math/bits"

	"github.com/prometheus/prometheus/promql"

	"github.com/grafana/mimir/pkg/util/limiter"
	"github.com/grafana/mimir/pkg/util/pool"
)

const (
	// There's not too much science behind this number: this is the based on examining the largest queries seen at Grafana Labs.
	// The number must also align with a power of two for our pools.
	MaxExpectedSeriesPerResult = 8_388_608
)

var (
	matrixPool = pool.NewBucketedPool(MaxExpectedSeriesPerResult, func(size int) promql.Matrix {
		return make(promql.Matrix, 0, size)
	})
)

func GetMatrix(size int) promql.Matrix {
	return matrixPool.Get(size)
}

func PutMatrix(m promql.Matrix) {
	matrixPool.Put(m)
}

// EnsureFPointSliceCapacityIsPowerOfTwo returns d if its capacity is already a power of two, or otherwise a new slice with the same elements and a
// capacity that is a power of two.
//
// If a new slice is created, the memory consumption estimate is adjusted assuming the old slice is no longer used.
//
// This exists because many places in MQE assume that slices have come from our pools and always have a capacity that is a power of two.
// For example, the ring buffer implementations rely on the fact that slices have a capacity that is a power of two.
func EnsureFPointSliceCapacityIsPowerOfTwo(points []promql.FPoint, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) ([]promql.FPoint, error) {
	if pool.IsPowerOfTwo(cap(points)) {
		return points, nil
	}

	nextPowerOfTwo := 1 << bits.Len(uint(cap(points)-1))
	newSlice, err := FPointSlicePool.Get(nextPowerOfTwo, memoryConsumptionTracker)
	if err != nil {
		return nil, err
	}

	newSlice = newSlice[:len(points)]
	copy(newSlice, points)

	// Don't return the old slice to the pool, but update the memory consumption estimate.
	// The pool won't use it because it's not a power of two, so there's no point in calling Put() on it.
	memoryConsumptionTracker.DecreaseMemoryConsumption(uint64(cap(points))*FPointSize, limiter.FPointSlices)

	return newSlice, nil
}

// EnsureHPointSliceCapacityIsPowerOfTwo is like EnsureFPointSliceCapacityIsPowerOfTwo, but for HPoint slices.
func EnsureHPointSliceCapacityIsPowerOfTwo(points []promql.HPoint, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) ([]promql.HPoint, error) {
	if pool.IsPowerOfTwo(cap(points)) {
		return points, nil
	}

	nextPowerOfTwo := 1 << bits.Len(uint(cap(points)-1))
	newSlice, err := HPointSlicePool.Get(nextPowerOfTwo, memoryConsumptionTracker)
	if err != nil {
		return nil, err
	}

	newSlice = newSlice[:len(points)]
	copy(newSlice, points)

	// Don't return the old slice to the pool, but update the memory consumption estimate.
	// The pool won't use it because it's not a power of two, so there's no point in calling Put() on it.
	memoryConsumptionTracker.DecreaseMemoryConsumption(uint64(cap(points))*HPointSize, limiter.HPointSlices)

	return newSlice, nil
}
