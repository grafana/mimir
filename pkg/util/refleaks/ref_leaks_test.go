// SPDX-License-Identifier: AGPL-3.0-only

package refleaks

import (
	"fmt"
	"runtime/debug"
	"testing"
	"unsafe"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestInstrumentRefLeaks(t *testing.T) {
	prev := debug.SetPanicOnFault(true)
	defer debug.SetPanicOnFault(prev)

	type Thingy struct {
		A, B, C int64
	}

	thingies, buf, ok := MaybeInstrument[Thingy](GlobalTracker, 10, 20)
	require.True(t, ok)
	require.Len(t, thingies, 10)
	require.Equal(t, 20, cap(thingies))
	bytesCap := 20 * 3 * int(unsafe.Sizeof(int64(0)))
	require.Equal(t, bytesCap, buf.Len())

	require.Equal(t, 1.0, testutil.ToFloat64(GlobalTracker.InstrumentedBuffersTotalMetric))
	require.NotZero(t, testutil.ToFloat64(GlobalTracker.InflightInstrumentedBytesMetric))

	leakingRef := &thingies[2].B

	buf.Free() // leakingRef becomes a leak here

	requireAccessPanics(t, func() any { return *leakingRef })

	require.Equal(t, 1.0, testutil.ToFloat64(GlobalTracker.InstrumentedBuffersTotalMetric))
	require.Zero(t, testutil.ToFloat64(GlobalTracker.InflightInstrumentedBytesMetric))

	// Check that a leak-free buffer is measured as instrumented.
	_, buf2, _ := MaybeInstrument[Thingy](GlobalTracker, 30, 40)
	require.Equal(t, 2.0, testutil.ToFloat64(GlobalTracker.InstrumentedBuffersTotalMetric))
	require.NotZero(t, testutil.ToFloat64(GlobalTracker.InflightInstrumentedBytesMetric))

	buf2.Free()

	require.Equal(t, 2.0, testutil.ToFloat64(GlobalTracker.InstrumentedBuffersTotalMetric))
	require.Zero(t, testutil.ToFloat64(GlobalTracker.InflightInstrumentedBytesMetric))
}

func requireAccessPanics(t *testing.T, access func() any) {
	t.Helper()
	var recovered any
	func() {
		defer func() {
			recovered = recover()
		}()
		t.Log(access()) // Force a read
	}()
	require.Equal(t, fmt.Sprint(recovered), "runtime error: invalid memory address or nil pointer dereference")
}
