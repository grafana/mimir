// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"context"
	"fmt"
	"math"
	"sync"
	"testing"

	"github.com/grafana/dskit/gate"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storegateway/storepb"
)

// countingGate wraps a gate and records how many times it was acquired and the peak number of
// simultaneous holders, so a test can assert both that every block goes through the gate and
// that the gate actually bounds concurrency.
type countingGate struct {
	delegate gate.Gate

	mtx      sync.Mutex
	inflight int
	peak     int
	starts   int
}

func newCountingGate(maxConcurrent int) *countingGate {
	delegate := gate.NewNoop()
	if maxConcurrent > 0 {
		delegate = gate.NewBlocking(maxConcurrent)
	}
	return &countingGate{delegate: delegate}
}

func (g *countingGate) Start(ctx context.Context) error {
	if err := g.delegate.Start(ctx); err != nil {
		return err
	}

	g.mtx.Lock()
	defer g.mtx.Unlock()
	g.inflight++
	g.starts++
	g.peak = max(g.peak, g.inflight)

	return nil
}

func (g *countingGate) Done() {
	g.mtx.Lock()
	g.inflight--
	g.mtx.Unlock()

	g.delegate.Done()
}

func (g *countingGate) stats() (starts, peak int) {
	g.mtx.Lock()
	defer g.mtx.Unlock()
	return g.starts, g.peak
}

// TestBucketStore_BlockGate asserts that every per-block goroutine acquires the block gate, and
// that the gate bounds how many of them run at once. The store under test holds
// defaultPrepareStoreConfig's blocks, so a request that touches the whole time range fans out
// to more blocks than the limit allows.
func TestBucketStore_BlockGate(t *testing.T) {
	ctx := context.Background()

	endpoints := map[string]func(bs *BucketStore) error{
		"LabelNames": func(bs *BucketStore) error {
			_, err := bs.LabelNames(ctx, &storepb.LabelNamesRequest{End: math.MaxInt64})
			return err
		},
		"LabelValues": func(bs *BucketStore) error {
			_, err := bs.LabelValues(ctx, &storepb.LabelValuesRequest{Label: "a", End: math.MaxInt64})
			return err
		},
		"SearchLabelNames": func(bs *BucketStore) error {
			req := &storepb.SearchLabelNamesRequest{End: math.MaxInt64, Filter: &storepb.SearchFilter{}}
			return bs.SearchLabelNames(req, &mockSearchLabelNamesServer{ctx: ctx})
		},
		"SearchLabelValues": func(bs *BucketStore) error {
			req := &storepb.SearchLabelValuesRequest{Label: "a", End: math.MaxInt64, Filter: &storepb.SearchFilter{}}
			return bs.SearchLabelValues(req, &mockSearchLabelValuesServer{ctx: ctx})
		},
	}

	for name, call := range endpoints {
		t.Run(name, func(t *testing.T) {
			for _, maxConcurrent := range []int{1, 2} {
				t.Run(fmt.Sprintf("max concurrent blocks = %d", maxConcurrent), func(t *testing.T) {
					bs := prepareSearchTestStore(t)
					g := newCountingGate(maxConcurrent)
					bs.blockGate = g

					require.NoError(t, call(bs))

					starts, peak := g.stats()
					t.Logf("gate acquired %d times, peak concurrency %d", starts, peak)
					require.GreaterOrEqual(t, starts, 2, "the gate should be acquired once per block, not once per request")
					require.LessOrEqual(t, peak, maxConcurrent, "the block gate should bound concurrent per-block work")
				})
			}
		})
	}
}

// TestBucketStore_BlockGate_DefaultIsNoop asserts the gate is a no-op unless configured, so that
// enabling nothing changes nothing.
func TestBucketStore_BlockGate_DefaultIsNoop(t *testing.T) {
	bs := prepareSearchTestStore(t)
	require.Equal(t, gate.NewNoop(), bs.blockGate)
}
