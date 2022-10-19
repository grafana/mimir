// SPDX-License-Identifier: AGPL-3.0-only

package forwarding

import (
	"reflect"
	"sync"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
)

// TestUsingPools doesn't have any real test case, it just calls all the pool methods to make
// sure that they don't panic due to some nil pointer dereference.
func TestUsingPools(t *testing.T) {
	pools := newPools()

	protoBuf := pools.getProtobuf()
	pools.putProtobuf(protoBuf)

	snappy := pools.getSnappy()
	pools.putSnappy(snappy)

	req := pools.getReq()
	pools.putReq(req)

	ts := pools.getTs()
	pools.putTs(ts)

	tsSlice := pools.getTsSlice()
	pools.putTsSlice(tsSlice)
}

// validatingPools creates an instances of pools where all the used pools have been mocked out with validators
// that allow us to validate that the pools are used correctly.
// The specified caps must be large enough to hold all the data that will be stored in the respective slices because
// otherwise any "append()" will replace the slice which will result in a test failure because the original slice
// won't be returned to the pool.
func validatingPools(t *testing.T, tsSliceCap, protobufCap, snappyCap int) (*pools, func()) {
	t.Helper()

	pools := &pools{}

	validatingProtobufPool := newByteSlicePool(t, protobufCap)
	pools.getProtobuf = validatingProtobufPool.get
	pools.putProtobuf = validatingProtobufPool.put

	validatingSnappyPool := newByteSlicePool(t, snappyCap)
	pools.getSnappy = validatingSnappyPool.get
	pools.putSnappy = validatingSnappyPool.put

	validatingRequestPool := newValidatingPool(t,
		func() *request {
			return &request{}
		},
		func(obj *request) int {
			// We uniquely identify objects of type *request by the address which the pointer is referring to.
			return int(reflect.ValueOf(obj).Pointer())
		}, nil,
	)
	pools.getReq = validatingRequestPool.get
	pools.putReq = validatingRequestPool.put

	validatingTsPool := newValidatingPool(t,
		func() *mimirpb.TimeSeries {
			return &mimirpb.TimeSeries{}
		},
		func(obj *mimirpb.TimeSeries) int {
			// We uniquely identify objects of type *TimeSeries by the address which the pointer is referring to.
			return int(reflect.ValueOf(obj).Pointer())
		}, nil,
	)
	pools.getTs = validatingTsPool.get
	pools.putTs = validatingTsPool.put

	validatingTsSlicePool := newValidatingPool(t,
		func() []mimirpb.PreallocTimeseries {
			return make([]mimirpb.PreallocTimeseries, 0, tsSliceCap)
		},
		func(obj []mimirpb.PreallocTimeseries) int {
			// We uniquely identify objects of type []mimirpb.PreallocTimeseries by the address of the underlying data array.
			return int((*reflect.SliceHeader)(unsafe.Pointer(&obj)).Data)
		},
		func(obj []mimirpb.PreallocTimeseries) []mimirpb.PreallocTimeseries {
			for _, ts := range obj {
				// When returning a slice of PreallocTimeseries to the pool we first need to return the contained
				// TimeSeries objects to their pool, the original methods in the mimirpb package do the same.
				validatingTsPool.put(ts.TimeSeries)
			}
			return obj
		},
	)
	pools.getTsSlice = validatingTsSlicePool.get
	pools.putTsSlice = validatingTsSlicePool.put

	validateUsage := func() {
		validatingProtobufPool.validateUsage()
		validatingSnappyPool.validateUsage()
		validatingRequestPool.validateUsage()
		validatingTsPool.validateUsage()
		validatingTsSlicePool.validateUsage()
	}

	return pools, validateUsage
}

func newByteSlicePool(t *testing.T, cap int) *validatingPool[*[]byte] {
	return newValidatingPool(t,
		func() *[]byte {
			obj := make([]byte, 0, cap)
			return &obj
		},
		func(obj *[]byte) int {
			// We uniquely identify objects of type []byte by the address of the underlying data array.
			return int((*reflect.SliceHeader)(unsafe.Pointer(obj)).Data)
		}, nil,
	)
}

// validatingPool is a pool of objects that validates that it is used correctly by keeping track of a unique ID
// of each object it instantiates and whether the object has been returned to the pool.
type validatingPool[T any] struct {
	t                   *testing.T
	objsInstantiated    map[int]bool
	objsInstantiatedMtx sync.Mutex
	objsReturned        []T
	new                 func() T
	id                  func(T) int
	prepareForPut       func(T) T
}

func newValidatingPool[T any](t *testing.T, new func() T, id func(T) int, prepareForPut func(T) T) *validatingPool[T] {
	return &validatingPool[T]{
		t: t,

		// objsInstantiated is keyed by the ids of the objects that have been instantiated by the pool,
		// the bool value indicates whether this object has already been returned to the pool.
		objsInstantiated: make(map[int]bool),

		// new is the function to instantiate a new object.
		new: new,

		// id is the function to generate a unique id for a given object.
		id: id,

		// prepareForPut is an optional function which is called on a given object when it is returned to the pool.
		prepareForPut: prepareForPut,
	}
}

// get returns an object from the pool, the object must be returned to the pool before validateUsage() is called.
func (v *validatingPool[T]) get() T {
	v.t.Helper()

	obj := v.new()
	id := v.id(obj)

	v.objsInstantiatedMtx.Lock()
	defer v.objsInstantiatedMtx.Unlock()

	_, ok := v.objsInstantiated[id]
	require.False(v.t, ok, "object has already been instantiated")
	v.objsInstantiated[id] = false

	return obj
}

// put returns an object to the pool, the object must have been created by the pool and it must only be returned once.
func (v *validatingPool[T]) put(obj T) {
	v.t.Helper()

	if v.prepareForPut != nil {
		obj = v.prepareForPut(obj)
	}

	id := v.id(obj)

	v.objsInstantiatedMtx.Lock()
	defer v.objsInstantiatedMtx.Unlock()

	returned, ok := v.objsInstantiated[id]
	require.True(v.t, ok, "object is not from this pool")
	require.False(v.t, returned, "this object has already been returned to the pool")
	v.objsInstantiated[id] = true

	// We need to keep a reference to the returned obj to ensure that the next call to get() cannot return a new
	// object which happens to have the same id because this would lead to a collision in objsInstantiated.
	v.objsReturned = append(v.objsReturned, obj)
}

// validateUsage validates that all the objects created by the pool have been returned to it.
func (v *validatingPool[T]) validateUsage() {
	v.t.Helper()

	v.objsInstantiatedMtx.Lock()
	defer v.objsInstantiatedMtx.Unlock()

	for id, returned := range v.objsInstantiated {
		require.True(v.t, returned, "object with id %d has not been returned to pool", id)
	}
}
