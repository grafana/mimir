// SPDX-License-Identifier: AGPL-3.0-only

package tracing

import (
	"context"
	crand "crypto/rand"
	"encoding/binary"
	"errors"
	"math/rand"
	"testing"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/uber/jaeger-client-go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

func TestJaegerToOTelTraceID(t *testing.T) {
	expected, _ := generateOTelIDs()

	jaegerTraceID, err := jaeger.TraceIDFromString(expected.String())
	require.NoError(t, err)

	actual := jaegerToOTelTraceID(jaegerTraceID)
	assert.Equal(t, expected, actual)
}

func TestJaegerToOTelSpanID(t *testing.T) {
	_, expected := generateOTelIDs()

	jaegerSpanID, err := jaeger.SpanIDFromString(expected.String())
	require.NoError(t, err)

	actual := jaegerToOTelSpanID(jaegerSpanID)
	assert.Equal(t, expected, actual)
}

func TestJaegerFromOTelTraceID(t *testing.T) {
	traceID, _ := generateOTelIDs()

	expected, err := jaeger.TraceIDFromString(traceID.String())
	require.NoError(t, err)

	actual := jaegerFromOTelTraceID(traceID)
	assert.Equal(t, expected, actual)
}

func TestJaegerFromOTelSpanID(t *testing.T) {
	_, spanID := generateOTelIDs()

	expected, err := jaeger.SpanIDFromString(spanID.String())
	require.NoError(t, err)

	actual := jaegerFromOTelSpanID(spanID)
	assert.Equal(t, expected, actual)
}

// generateOTelIDs generated trace and span IDs. The implementation has been copied from open telemetry.
func generateOTelIDs() (trace.TraceID, trace.SpanID) {
	var rngSeed int64
	_ = binary.Read(crand.Reader, binary.LittleEndian, &rngSeed)
	randSource := rand.New(rand.NewSource(rngSeed))

	tid := trace.TraceID{}
	randSource.Read(tid[:])
	sid := trace.SpanID{}
	randSource.Read(sid[:])
	return tid, sid
}

func TestOpenTelemetrySpanBridge_RecordError(t *testing.T) {
	err := errors.New("my application error")

	tests := map[string]struct {
		attrs    []attribute.KeyValue
		expected []log.Field
	}{
		"no attributes": {
			expected: []log.Field{
				log.Error(err),
			},
		},
		"one attribute": {
			attrs: []attribute.KeyValue{
				attribute.String("first", "value"),
			},
			expected: []log.Field{
				log.Error(err),
				log.Object("first", "value"),
			},
		},
		"multiple attributes": {
			attrs: []attribute.KeyValue{
				attribute.String("first", "value"),
				attribute.Int("second", 123),
			},
			expected: []log.Field{
				log.Error(err),
				log.Object("first", "value"),
				log.Object("second", int64(123)),
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			m := &TracingSpanMock{}
			m.On("LogFields", mock.Anything).Return()

			s := NewOpenTelemetrySpanBridge(m, nil)
			s.recordError(err, testData.attrs)
			m.AssertCalled(t, "LogFields", testData.expected)
		})
	}
}

func TestOpenTelemetrySpanBridge_AddEvent(t *testing.T) {
	const eventName = "my application did something"

	tests := map[string]struct {
		attrs    []attribute.KeyValue
		expected []log.Field
	}{
		"no attributes": {
			expected: []log.Field{
				log.Event(eventName),
			},
		},
		"one attribute": {
			attrs: []attribute.KeyValue{
				attribute.String("first", "value"),
			},
			expected: []log.Field{
				log.Event(eventName),
				log.Object("first", "value"),
			},
		},
		"multiple attributes": {
			attrs: []attribute.KeyValue{
				attribute.String("first", "value"),
				attribute.Int("second", 123),
			},
			expected: []log.Field{
				log.Event(eventName),
				log.Object("first", "value"),
				log.Object("second", int64(123)),
			},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			m := &TracingSpanMock{}
			m.On("LogFields", mock.Anything).Return()

			s := NewOpenTelemetrySpanBridge(m, nil)
			s.addEvent(eventName, testData.attrs)
			m.AssertCalled(t, "LogFields", testData.expected)
		})
	}
}

type TracingSpanMock struct {
	mock.Mock
}

func (m *TracingSpanMock) Finish() {
	m.Called()
}

func (m *TracingSpanMock) FinishWithOptions(opts opentracing.FinishOptions) {
	m.Called(opts)
}

func (m *TracingSpanMock) Context() opentracing.SpanContext {
	args := m.Called()
	return args.Get(0).(opentracing.SpanContext)
}

func (m *TracingSpanMock) SetOperationName(operationName string) opentracing.Span {
	args := m.Called(operationName)
	return args.Get(0).(opentracing.Span)
}

func (m *TracingSpanMock) SetTag(key string, value interface{}) opentracing.Span {
	args := m.Called(key, value)
	return args.Get(0).(opentracing.Span)
}

func (m *TracingSpanMock) LogFields(fields ...log.Field) {
	m.Called(fields)
}

func (m *TracingSpanMock) LogKV(alternatingKeyValues ...interface{}) {
	m.Called(alternatingKeyValues)
}

func (m *TracingSpanMock) SetBaggageItem(restrictedKey, value string) opentracing.Span {
	args := m.Called(restrictedKey, value)
	return args.Get(0).(opentracing.Span)
}

func (m *TracingSpanMock) BaggageItem(restrictedKey string) string {
	args := m.Called(restrictedKey)
	return args.String(0)
}

func (m *TracingSpanMock) Tracer() opentracing.Tracer {
	args := m.Called()
	return args.Get(0).(opentracing.Tracer)
}

func (m *TracingSpanMock) LogEvent(event string) {
	m.Called(event)
}

func (m *TracingSpanMock) LogEventWithPayload(event string, payload interface{}) {
	m.Called(event, payload)
}

func (m *TracingSpanMock) Log(data opentracing.LogData) {
	m.Called(data)
}

func TestOpenTelemetryTracerBridge_Start(t *testing.T) {
	bridge := NewOpenTelemetryProviderBridge(opentracing.GlobalTracer())
	tracer := bridge.Tracer("")
	ctx, span := tracer.Start(context.Background(), "test")
	ctxSpan := trace.SpanFromContext(ctx)

	require.Same(t, span, ctxSpan, "returned context should contain span")
}

func TestOpenTelemetrySpanBridge_EndIdempotency(t *testing.T) {
	tests := map[string]struct {
		endCalls    int
		withOptions bool
		finishCalls int
		finishOpts  int
	}{
		"single end without options": {
			endCalls:    1,
			withOptions: false,
			finishCalls: 1,
			finishOpts:  0,
		},
		"multiple ends without options": {
			endCalls:    3,
			withOptions: false,
			finishCalls: 1,
			finishOpts:  0,
		},
		"single end with options": {
			endCalls:    1,
			withOptions: true,
			finishCalls: 0,
			finishOpts:  1,
		},
		"multiple ends with options": {
			endCalls:    3,
			withOptions: true,
			finishCalls: 0,
			finishOpts:  1,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			// Setup mock
			m := &TracingSpanMock{}
			m.On("Finish").Return()
			m.On("FinishWithOptions", mock.Anything).Return()

			// Create bridge
			s := NewOpenTelemetrySpanBridge(m, nil)

			// Call End multiple times
			for i := 0; i < testData.endCalls; i++ {
				if testData.withOptions {
					s.End(trace.WithTimestamp(time.Now()))
				} else {
					s.End()
				}
			}

			// Assert expectations
			if testData.finishCalls > 0 {
				m.AssertNumberOfCalls(t, "Finish", testData.finishCalls)
			}
			if testData.finishOpts > 0 {
				m.AssertNumberOfCalls(t, "FinishWithOptions", testData.finishOpts)
			}
		})
	}
}
