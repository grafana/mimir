// SPDX-License-Identifier: AGPL-3.0-only

package mimirpb

import (
	"fmt"
	"runtime/debug"
	"testing"
	"unsafe"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestInstrumentRefLeaks(t *testing.T) {
	if !refLeaksInstrumentationSupported {
		t.Skip(errRefLeaksNotAvailable)
	}

	prev := debug.SetPanicOnFault(true)
	defer debug.SetPanicOnFault(prev)

	src := WriteRequest{Timeseries: []PreallocTimeseries{{TimeSeries: &TimeSeries{
		Labels:  []UnsafeMutableLabel{{Name: "labelName", Value: "labelValue"}},
		Samples: []Sample{{TimestampMs: 1234, Value: 1337}},
	}}}}
	buf, err := src.Marshal()
	require.NoError(t, err)

	var leakingLabelName UnsafeMutableString

	var req WriteRequest
	err = Unmarshal(buf, &req)
	require.NoError(t, err)

	codec, _ := globalCodec.(*codecV2)
	require.Equal(t, 1.0, testutil.ToFloat64(codec.instrumentedBuffersTotalMetric))
	require.NotZero(t, testutil.ToFloat64(codec.inflightInstrumentedBytesMetric))

	// Label names are UnsafeMutableStrings pointing to buf. They shouldn't outlive
	// the call to req.FreeBuffer.
	leakingLabelName = req.Timeseries[0].Labels[0].Name

	req.FreeBuffer() // leakingLabelName becomes a leak here

	var recovered any
	func() {
		defer func() {
			recovered = recover()
		}()
		t.Log(leakingLabelName) // Just forcing a read on leakingLabelName here
	}()
	require.Equal(t, fmt.Sprint(recovered), "runtime error: invalid memory address or nil pointer dereference")

	require.Equal(t, 1.0, testutil.ToFloat64(codec.instrumentedBuffersTotalMetric))
	require.Zero(t, testutil.ToFloat64(codec.inflightInstrumentedBytesMetric))

	// Check that a leak-free buffer is measured as instrumented.
	var req2 WriteRequest
	err = Unmarshal(buf, &req2)
	require.NoError(t, err)

	require.Equal(t, 2.0, testutil.ToFloat64(codec.instrumentedBuffersTotalMetric))
	require.NotZero(t, testutil.ToFloat64(codec.inflightInstrumentedBytesMetric))

	req2.FreeBuffer()

	require.Equal(t, 2.0, testutil.ToFloat64(codec.instrumentedBuffersTotalMetric))
	require.Zero(t, testutil.ToFloat64(codec.inflightInstrumentedBytesMetric))
}

// Content read without decoding it is instrumented as decoding it would be: a reference to it that outlives the
// release faults.
func TestInstrumentRefLeaksOfContent(t *testing.T) {
	if !refLeaksInstrumentationSupported {
		t.Skip(errRefLeaksNotAvailable)
	}

	prev := debug.SetPanicOnFault(true)
	defer debug.SetPanicOnFault(prev)

	content := []byte("a record's content")
	instrumented, release := InstrumentRefLeaks(content)
	require.Equal(t, content, instrumented)
	require.NotSame(t, &content[0], &instrumented[0], "a copy in pages of its own")
	leaking := unsafe.String(unsafe.SliceData(instrumented), len(instrumented))

	release()
	var recovered any
	func() {
		defer func() {
			recovered = recover()
		}()
		t.Log(leaking)
	}()
	require.Equal(t, "runtime error: invalid memory address or nil pointer dereference", fmt.Sprint(recovered))
}
