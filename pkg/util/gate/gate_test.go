// SPDX-License-Identifier: AGPL-3.0-only

package gate

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestRejectingGate(t *testing.T) {
	t.Run("not at concurrency limit", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		g := NewRejecting(1)
		err1 := g.Start(ctx)
		require.NoError(t, err1)

		g.Done()
		err2 := g.Start(ctx)
		require.NoError(t, err2)
	})

	t.Run("at concurrency limit", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		g := NewRejecting(1)
		err1 := g.Start(ctx)
		err2 := g.Start(ctx)

		require.NoError(t, err1)
		require.ErrorIs(t, err2, ErrMaxConcurrent)
	})
}

func TestBlockingGate(t *testing.T) {
	t.Run("not at concurrency limit", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		g := NewBlocking(1)
		err1 := g.Start(ctx)
		require.NoError(t, err1)

		g.Done()
		err2 := g.Start(ctx)
		require.NoError(t, err2)
	})

	t.Run("at concurrency limit", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()

		g := NewBlocking(1)
		err1 := g.Start(ctx)
		err2 := g.Start(ctx)

		require.NoError(t, err1)
		require.ErrorIs(t, err2, context.DeadlineExceeded)
	})
}

func TestInstrumentedGate(t *testing.T) {
	t.Run("max concurrency", func(t *testing.T) {
		concurrency := 1
		reg := prometheus.NewPedanticRegistry()
		_ = NewInstrumented(reg, concurrency, NewNoop())

		require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
			# HELP gate_queries_concurrent_max Number of maximum concurrent queries allowed.
			# TYPE gate_queries_concurrent_max gauge
			gate_queries_concurrent_max 1
		`), "gate_queries_concurrent_max"))
	})

	t.Run("inflight requests", func(t *testing.T) {
		concurrency := 1
		reg := prometheus.NewPedanticRegistry()
		g := NewInstrumented(reg, concurrency, NewNoop())

		require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
			# HELP gate_queries_in_flight Number of queries that are currently in flight.
			# TYPE gate_queries_in_flight gauge
			gate_queries_in_flight 0
		`), "gate_queries_in_flight"))

		require.NoError(t, g.Start(context.Background()))

		require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
			# HELP gate_queries_in_flight Number of queries that are currently in flight.
			# TYPE gate_queries_in_flight gauge
			gate_queries_in_flight 1
		`), "gate_queries_in_flight"))
	})
}
