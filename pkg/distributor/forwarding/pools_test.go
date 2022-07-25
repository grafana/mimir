// SPDX-License-Identifier: AGPL-3.0-only

package forwarding

import (
	"bytes"
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

	bytesReader := pools.getBytesReader()
	pools.putBytesReader(bytesReader)

	tsByTargets := pools.getTsByTargets()
	pools.putTsByTargets(tsByTargets)

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

	validatingProtobufPool := newValidatingByteSlicePool(t, protobufCap)
	pools.getProtobuf = validatingProtobufPool.get
	pools.putProtobuf = validatingProtobufPool.put

	validatingSnappyPool := newValidatingByteSlicePool(t, snappyCap)
	pools.getSnappy = validatingSnappyPool.get
	pools.putSnappy = validatingSnappyPool.put

	validatingRequestPool := newValidatingRequestPool(t)
	pools.getReq = validatingRequestPool.get
	pools.putReq = validatingRequestPool.put

	validatingBytesReaderPool := newValidatingBytesReaderPool(t)
	pools.getBytesReader = validatingBytesReaderPool.get
	pools.putBytesReader = validatingBytesReaderPool.put

	validatingTsByTargetsPool := newValidatingTsByTargetsPool(t)
	pools.getTsByTargets = validatingTsByTargetsPool.get
	pools.putTsByTargets = validatingTsByTargetsPool.put

	validatingMockTsPool := newValidatingTsPool(t)
	pools.getTs = validatingMockTsPool.get
	pools.putTs = validatingMockTsPool.put

	validatingMockTsSlicePool := newValidatingTsSlicePool(t, tsSliceCap, validatingMockTsPool.put)
	pools.getTsSlice = validatingMockTsSlicePool.get
	pools.putTsSlice = validatingMockTsSlicePool.put

	validateUsage := func() {
		validatingProtobufPool.validateUsage()
		validatingSnappyPool.validateUsage()
		validatingRequestPool.validateUsage()
		validatingBytesReaderPool.validateUsage()
		validatingTsByTargetsPool.validateUsage()
		validatingMockTsPool.validateUsage()
		validatingMockTsSlicePool.validateUsage()
	}

	return pools, validateUsage
}

// validatingPool is a pool of objects that validates that it is used correctly by keeping track of a unique ID
// of each object it instantiates and whether the object has been returned to the pool.
type validatingPool struct {
	t                   *testing.T
	objsInstantiated    map[interface{}]bool
	objsInstantiatedMtx sync.Mutex
	objsReturned        []interface{}
	new                 func() interface{}
	id                  func(interface{}) interface{}
	prepareForPut       func(interface{}) interface{}
}

func newValidatingPool(t *testing.T, new func() interface{}, id func(interface{}) interface{}, prepareForPut func(interface{}) interface{}) *validatingPool {
	return &validatingPool{
		t:                t,
		objsInstantiated: make(map[interface{}]bool),
		new:              new,
		id:               id,
		prepareForPut:    prepareForPut,
	}
}

// get returns an object from the pool, the object must be returned to the pool before validateUsage() is called.
func (v *validatingPool) get() interface{} {
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
func (v *validatingPool) put(obj interface{}) {
	v.t.Helper()

	id := v.id(obj)

	if v.prepareForPut != nil {
		obj = v.prepareForPut(obj)
	}

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
func (v *validatingPool) validateUsage() {
	v.t.Helper()

	v.objsInstantiatedMtx.Lock()
	defer v.objsInstantiatedMtx.Unlock()

	for id, returned := range v.objsInstantiated {
		require.True(v.t, returned, "object with id %d has not been returned to pool", id)
	}
}

type validatingByteSlicePool struct {
	validatingPool
}

func newValidatingByteSlicePool(t *testing.T, capacity int) *validatingByteSlicePool {
	interfaceToType := func(obj interface{}) *[]byte {
		switch obj := obj.(type) {
		case *[]byte:
			return obj
		default:
			t.Fatalf("Object of invalid type given: %s", reflect.TypeOf(obj))
			return nil // Just for linter.
		}
	}

	new := func() interface{} {
		obj := make([]byte, 0, capacity)
		return &obj
	}

	id := func(obj interface{}) interface{} {
		objT := interfaceToType(obj)

		// We uniquely identify objects of type *[]byte by the address of the underlying data array.
		return (*reflect.SliceHeader)(unsafe.Pointer(objT)).Data
	}

	return &validatingByteSlicePool{*newValidatingPool(t, new, id, nil)}
}

// get returns a pointer to a byte slice from the pool, it must be returned to the pool before validateUsage() is called.
func (v *validatingByteSlicePool) get() *[]byte {
	return v.validatingPool.get().(*[]byte)
}

// put returns a pointer to a byte slice to the pool, it  must have been created by the pool and it must only be returned once.
func (v *validatingByteSlicePool) put(obj *[]byte) {
	v.validatingPool.put(obj)
}

type validatingRequestPool struct {
	validatingPool
}

func newValidatingRequestPool(t *testing.T) *validatingRequestPool {
	interfaceToType := func(obj interface{}) *request {
		switch obj := obj.(type) {
		case *request:
			return obj
		default:
			t.Fatalf("Object of invalid type given: %s", reflect.TypeOf(obj))
			return nil // Just for linter.
		}
	}

	new := func() interface{} {
		return &request{}
	}

	id := func(obj interface{}) interface{} {
		objT := interfaceToType(obj)

		// We uniquely identify objects of type *request by the address which the pointer is referring to.
		return reflect.ValueOf(objT).Pointer()
	}

	return &validatingRequestPool{*newValidatingPool(t, new, id, nil)}
}

// get returns a pointer to a request from the pool, it must be returned to the pool before validateUsage() is called.
func (v *validatingRequestPool) get() *request {
	return v.validatingPool.get().(*request)
}

// put returns a pointer to a request to the pool, it  must have been created by the pool and it must only be returned once.
func (v *validatingRequestPool) put(obj *request) {
	v.validatingPool.put(obj)
}

type validatingBytesReaderPool struct {
	validatingPool
}

func newValidatingBytesReaderPool(t *testing.T) *validatingBytesReaderPool {
	interfaceToType := func(obj interface{}) *bytes.Reader {
		switch obj := obj.(type) {
		case *bytes.Reader:
			return obj
		default:
			t.Fatalf("Object of invalid type given: %s", reflect.TypeOf(obj))
			return nil // Just for linter.
		}
	}

	new := func() interface{} {
		return bytes.NewReader(nil)
	}

	id := func(obj interface{}) interface{} {
		objT := interfaceToType(obj)

		// We uniquely identify objects of type *bytes.Reader by the address which the pointer is referring to.
		return reflect.ValueOf(objT).Pointer()
	}

	return &validatingBytesReaderPool{*newValidatingPool(t, new, id, nil)}
}

// get returns a pointer to a bytes reader from the pool, it must be returned to the pool before validateUsage() is called.
func (v *validatingBytesReaderPool) get() *bytes.Reader {
	return v.validatingPool.get().(*bytes.Reader)
}

// put returns a pointer to a bytes reader to the pool, it  must have been created by the pool and it must only be returned once.
func (v *validatingBytesReaderPool) put(obj *bytes.Reader) {
	v.validatingPool.put(obj)
}

type validatingTsByTargetsPool struct {
	validatingPool
}

func newValidatingTsByTargetsPool(t *testing.T) *validatingTsByTargetsPool {
	interfaceToType := func(obj interface{}) tsByTargets {
		switch obj := obj.(type) {
		case tsByTargets:
			return obj
		default:
			t.Fatalf("Object of invalid type given: %s", reflect.TypeOf(obj))
			return nil // Just for linter.
		}
	}

	new := func() interface{} {
		return make(tsByTargets)
	}

	id := func(obj interface{}) interface{} {
		objT := interfaceToType(obj)

		// We uniquely identify objects of type tsByTargets by the address which the pointer
		// is referring to because map types are just pointers.
		return reflect.ValueOf(objT).Pointer()
	}

	return &validatingTsByTargetsPool{*newValidatingPool(t, new, id, nil)}
}

// get returns a tsByTargets from the pool, it must be returned to the pool before validateUsage() is called.
func (v *validatingTsByTargetsPool) get() tsByTargets {
	return v.validatingPool.get().(tsByTargets)
}

// put returns a tsByTargets to the pool, it  must have been created by the pool and it must only be returned once.
func (v *validatingTsByTargetsPool) put(obj tsByTargets) {
	v.validatingPool.put(obj)
}

type validatingTsPool struct {
	validatingPool
}

func newValidatingTsPool(t *testing.T) *validatingTsPool {
	interfaceToType := func(obj interface{}) *mimirpb.TimeSeries {
		switch obj := obj.(type) {
		case *mimirpb.TimeSeries:
			return obj
		default:
			t.Fatalf("Object of invalid type given: %s", reflect.TypeOf(obj))
			return nil // Just for linter.
		}
	}

	new := func() interface{} {
		return &mimirpb.TimeSeries{}
	}

	id := func(obj interface{}) interface{} {
		objT := interfaceToType(obj)

		// We uniquely identify objects of type *TimeSeries by the address which the pointer is referring to.
		return reflect.ValueOf(objT).Pointer()
	}

	return &validatingTsPool{*newValidatingPool(t, new, id, nil)}
}

// get returns a time series object from the pool, it must be returned to the pool before validateUsage() is called.
func (v *validatingTsPool) get() *mimirpb.TimeSeries {
	return v.validatingPool.get().(*mimirpb.TimeSeries)
}

// put returns a time series object to the pool, it  must have been created by the pool and it must only be returned once.
func (v *validatingTsPool) put(obj *mimirpb.TimeSeries) {
	v.validatingPool.put(obj)
}

type validatingTsSlicePool struct {
	validatingPool
}

func newValidatingTsSlicePool(t *testing.T, initialCap int, putTs func(*mimirpb.TimeSeries)) *validatingTsSlicePool {
	interfaceToType := func(obj interface{}) []mimirpb.PreallocTimeseries {
		switch obj := obj.(type) {
		case []mimirpb.PreallocTimeseries:
			return obj
		default:
			t.Fatalf("Object of invalid type given: %s", reflect.TypeOf(obj))
			return nil // Just for linter.
		}
	}

	new := func() interface{} {
		return make([]mimirpb.PreallocTimeseries, 0, initialCap)
	}

	id := func(obj interface{}) interface{} {
		objT := interfaceToType(obj)

		// We uniquely identify objects of type *TimeSeries by the address of the underlying data array.
		return (*reflect.SliceHeader)(unsafe.Pointer(&objT)).Data
	}

	prepareForPut := func(obj interface{}) interface{} {
		objT := interfaceToType(obj)

		for _, ts := range objT {
			// When returning a slice of PreallocTimeseries to the pool we first need to return the contained
			// TimeSeries objects to their pool, the original methods in the mimirpb package do the same.
			putTs(ts.TimeSeries)
		}

		return obj
	}

	return &validatingTsSlicePool{*newValidatingPool(t, new, id, prepareForPut)}
}

// get returns a slice of time series from the pool, it must be returned to the pool before validateUsage() is called.
func (v *validatingTsSlicePool) get() []mimirpb.PreallocTimeseries {
	return v.validatingPool.get().([]mimirpb.PreallocTimeseries)
}

// put returns a slice of time series to the pool, it  must have been created by the pool and it must only be returned once.
func (v *validatingTsSlicePool) put(obj []mimirpb.PreallocTimeseries) {
	v.validatingPool.put(obj)
}
