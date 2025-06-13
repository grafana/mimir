package testdata

import (
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
	"github.com/grafana/mimir/pkg/util/pool"
)

func poolFromTypesPackage() {
	b := []bool{}
	tracker := limiter.NewMemoryConsumptionTracker(0, nil, "")

	types.BoolSlicePool.Put(b, tracker) // want `return value from 'Put' not used`
	_ = types.BoolSlicePool.Put(b, tracker)
	b = types.BoolSlicePool.Put(b, tracker)
}

func localPool() {
	i := []int{}
	tracker := limiter.NewMemoryConsumptionTracker(0, nil, "")
	otherPool := types.NewLimitingBucketedPool(
		pool.NewBucketedPool(10, func(size int) []int {
			return make([]int, 0, size)
		}),
		limiter.IntSlices,
		types.IntSize,
		true,
		nil,
	)

	otherPool.Put(i, tracker) // want `return value from 'Put' not used`
	_ = otherPool.Put(i, tracker)
	i = otherPool.Put(i, tracker)
}

func ignoresOtherTypeWithPutMethod() {
	i := []int{}
	x := otherStruct{}
	x.Put(i)
}

func ignoresPutMethod() {
	i := []int{}
	Put(i)
}

type otherStruct struct{}

func (f otherStruct) Put(_ []int) []int {
	return nil
}

func Put(_ []int) []int {
	return nil
}
