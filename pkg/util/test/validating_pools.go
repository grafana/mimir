// SPDX-License-Identifier: AGPL-3.0-only

package test

import (
	"reflect"
	"sync"
	"unsafe"

	"github.com/stretchr/testify/require"
)

type TestingT interface {
	Errorf(format string, args ...any)
	Logf(format string, args ...any)
	FailNow()
	Helper()
}

// ValidatingPool is a pool of objects that validates that it is used correctly by keeping track of a unique ID
// of each object it instantiates and whether the object has been returned to the pool.
type ValidatingPool[T any] struct {
	t TestingT

	// name uniquely identifies the pool in log messages.
	name string

	// checkReset indicates whether the pool should check that the objects that get returned to it have been reset properly.
	checkReset bool

	// newAttempts indicates that if the object returned by new has the same id, it should be re-instantiated newAttempts before failing.
	newAttempts int

	// objsInstantiated is keyed by the ids of the objects that have been instantiated by the pool,
	// the bool value indicates whether this object has already been returned to the pool.
	objsInstantiated    map[int]bool
	objsInstantiatedMtx sync.Mutex
	objsReturned        []T

	// new is the function to instantiate a new object.
	new func() T

	// id is the function to generate a unique id for a given object.
	id func(T) int

	// prepareForPut is an optional function which is called on a given object when it is returned to the pool.
	prepareForPut func(T) T
}

func NewValidatingPoolForPointer[T any](t TestingT, name string, checkReset bool, new func() T, prepareForPut func(T) T) *ValidatingPool[T] {
	return NewValidatingPool[T](t, name, checkReset, new, func(obj T) int {
		// We uniquely identify pooled objects by the address which the given pointer is referring to.
		return int(reflect.ValueOf(obj).Pointer())
	}, prepareForPut)
}

func NewValidatingPoolForSlice[T any](t TestingT, name string, checkReset bool, new func() []T, prepareForPut func([]T) []T) *ValidatingPool[[]T] {
	return NewValidatingPool(t, name, checkReset, new, func(obj []T) int {
		// We uniquely identify pooled slice objects by the address of the underlying data array.
		return int(uintptr(unsafe.Pointer(unsafe.SliceData(obj))))
	}, prepareForPut)
}

func NewValidatingPool[T any](t TestingT, name string, checkReset bool, new func() T, id func(T) int, prepareForPut func(T) T) *ValidatingPool[T] {
	return &ValidatingPool[T]{
		t:                t,
		name:             name,
		checkReset:       checkReset,
		objsInstantiated: make(map[int]bool),
		new:              new,
		id:               id,
		prepareForPut:    prepareForPut,
		newAttempts:      50,
	}
}

// Get returns an object from the pool, the object must be returned to the pool before validateUsage() is called.
func (v *ValidatingPool[T]) Get() any {
	v.t.Helper()

	v.objsInstantiatedMtx.Lock()
	defer v.objsInstantiatedMtx.Unlock()

	att := v.newAttempts
retry:
	obj := v.new()
	id := v.id(obj)

	_, ok := v.objsInstantiated[id]
	if ok && att > 0 {
		v.t.Logf("object with id %d has already been instantiated in pool %s, retrying", id, v.name)
		att--
		goto retry
	}
	require.False(v.t, ok, "object with id %d has already been instantiated in pool %s", id, v.name)
	v.objsInstantiated[id] = false

	v.t.Logf("got object with id %d from pool %s", id, v.name)

	return obj
}

func (v *ValidatingPool[T]) GetTyped() T {
	return v.Get().(T)
}

// Put returns an object to the pool, the object must have been created by the pool and it must only be returned once.
func (v *ValidatingPool[T]) Put(objUntyped any) {
	v.t.Helper()

	obj, ok := objUntyped.(T)
	require.True(v.t, ok, "object put into pool %s is not of the expected type %T", v.name, v.new())

	if v.prepareForPut != nil {
		obj = v.prepareForPut(obj)
	}

	id := v.id(obj)

	if v.checkReset {
		require.EqualValuesf(v.t, v.new(), obj, "object with id %d has not been reset before returning it to the pool %s", id, v.name)
	}

	v.objsInstantiatedMtx.Lock()
	defer v.objsInstantiatedMtx.Unlock()

	returned, ok := v.objsInstantiated[id]
	//require.True(v.t, ok, "object is not from this pool %s", v.name)
	require.False(v.t, returned, "this object has already been returned to the pool %s", v.name)
	v.objsInstantiated[id] = true

	// We need to keep a reference to the returned obj to ensure that the next call to get() cannot return a new
	// object which happens to have the same id because this would lead to a collision in objsInstantiated.
	v.objsReturned = append(v.objsReturned, obj)

	v.t.Logf("returned object with id %d to pool %s; performed reset check %t", id, v.name, v.checkReset)
}

func (v *ValidatingPool[T]) PutTyped(obj T) {
	v.Put(obj)
}

// ValidateUsage validates that all the objects created by the pool have been returned to it.
func (v *ValidatingPool[T]) ValidateUsage() {
	v.t.Helper()

	v.objsInstantiatedMtx.Lock()
	defer v.objsInstantiatedMtx.Unlock()

	for id, returned := range v.objsInstantiated {
		require.True(v.t, returned, "object with id %d has not been returned to the pool %s", id, v.name)
	}
}

// AllObjectsReturned returns true if all the objects created by the pool have been returned to it.
func (v *ValidatingPool[T]) AllObjectsReturned() bool {
	v.objsInstantiatedMtx.Lock()
	defer v.objsInstantiatedMtx.Unlock()

	for _, returned := range v.objsInstantiated {
		if !returned {
			return false
		}
	}

	return true
}
