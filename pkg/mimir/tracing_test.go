// SPDX-License-Identifier: AGPL-3.0-only

package mimir

// func TestJaegerToOpenTelemetryTraceID(t *testing.T) {
// 	expected, _ := generateOpenTelemetryIDs()

// 	jaegerTraceID, err := jaeger.TraceIDFromString(expected.String())
// 	require.NoError(t, err)

// 	actual := jaegerToOpenTelemetryTraceID(jaegerTraceID)
// 	assert.Equal(t, expected, actual)
// }

// func TestJaegerToOpenTelemetrySpanID(t *testing.T) {
// 	_, expected := generateOpenTelemetryIDs()

// 	jaegerSpanID, err := jaeger.SpanIDFromString(expected.String())
// 	require.NoError(t, err)

// 	actual := jaegerToOpenTelemetrySpanID(jaegerSpanID)
// 	assert.Equal(t, expected, actual)
// }

// // generateOpenTelemetryIDs generated trace and span IDs. The implementation has been copied from open telemetry.
// func generateOpenTelemetryIDs() (trace.TraceID, trace.SpanID) {
// 	var rngSeed int64
// 	_ = binary.Read(crand.Reader, binary.LittleEndian, &rngSeed)
// 	randSource := rand.New(rand.NewSource(rngSeed))

// 	tid := trace.TraceID{}
// 	randSource.Read(tid[:])
// 	sid := trace.SpanID{}
// 	randSource.Read(sid[:])
// 	return tid, sid
// }
