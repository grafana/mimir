// SPDX-License-Identifier: AGPL-3.0-only

package testutils

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestFixedInMemoryExporter(t *testing.T) {
	t.Run("exporter has room", func(t *testing.T) {
		stub := tracetest.SpanStub{Name: "test 1"}
		span := stub.Snapshot()

		exporter := NewFixedInMemoryExporter(4)

		require.NoError(t, exporter.ExportSpans(context.Background(), []trace.ReadOnlySpan{span}))
		exported := exporter.GetSpans()
		require.Len(t, exported, 1)
		require.Equal(t, exported[0], stub)
	})

	t.Run("exporter wraps around", func(t *testing.T) {
		var stubs []tracetest.SpanStub
		var spans []trace.ReadOnlySpan

		for i := 0; i < 6; i++ {
			stub := tracetest.SpanStub{Name: fmt.Sprintf("test %d", i+1)}
			stubs = append(stubs, stub)
			spans = append(spans, stub.Snapshot())
		}

		exporter := NewFixedInMemoryExporter(4)

		require.NoError(t, exporter.ExportSpans(context.Background(), spans))
		exported := exporter.GetSpans()
		require.Len(t, exported, 4)
		require.Equal(t, exported[0], stubs[2])
		require.Equal(t, exported[1], stubs[3])
		require.Equal(t, exported[2], stubs[4])
		require.Equal(t, exported[3], stubs[5])
	})

	t.Run("reset", func(t *testing.T) {
		stub := tracetest.SpanStub{Name: "test 1"}
		span := stub.Snapshot()

		exporter := NewFixedInMemoryExporter(4)

		require.NoError(t, exporter.ExportSpans(context.Background(), []trace.ReadOnlySpan{span}))
		exported := exporter.GetSpans()
		require.Len(t, exported, 1)

		exporter.Reset()
		exported = exporter.GetSpans()
		require.Empty(t, exported)
	})
}
