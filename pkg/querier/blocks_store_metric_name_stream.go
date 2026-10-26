// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/grafana/mimir/pkg/storegateway/storegatewaypb"
	"github.com/grafana/mimir/pkg/util/limiter"
)

// nameTable is the metric-name table of one block range, as the open
// streams of the store-gateways that counted it: each stream is sorted by
// name and holds each name once, so the table is their merge by name, with
// the counts of a name added up. Only one batch per stream is held at a
// time, so merging it holds memory for the streams' current batches, not
// for the table. It must be closed.
type nameTable struct {
	streams []*metricNameStream
	cancel  context.CancelFunc
	tracker *limiter.MemoryConsumptionTracker
}

// close ends every stream and releases the batches they hold.
func (t *nameTable) close() {
	t.cancel()
	for _, s := range t.streams {
		s.release(t.tracker)
	}
}

// metricNameStream is one store-gateway call's name table.
type metricNameStream struct {
	addr  string
	recv  func() (*storegatewaypb.MetricNameCountsResponse, error)
	batch []*storegatewaypb.MetricNameCount
	// charged is what batch is charged to the memory tracker.
	charged uint64
}

// hold makes batch the stream's current batch and charges it.
func (s *metricNameStream) hold(tracker *limiter.MemoryConsumptionTracker, batch []*storegatewaypb.MetricNameCount) error {
	s.batch = batch
	if tracker == nil {
		return nil
	}
	var b uint64
	for _, nc := range batch {
		b += uint64(estimateGroupBytes + len(nc.Name))
	}
	if err := tracker.IncreaseMemoryConsumption(b, limiter.CardinalityEstimateGroups); err != nil {
		return refusedEstimate("the metric-name batches this estimate holds in the querier: %w", err)
	}
	s.charged = b
	return nil
}

// release gives back the current batch's charge.
func (s *metricNameStream) release(tracker *limiter.MemoryConsumptionTracker) {
	if tracker != nil && s.charged > 0 {
		tracker.DecreaseMemoryConsumption(s.charged, limiter.CardinalityEstimateGroups)
	}
	s.charged = 0
}

// fill reads batches until the current one holds a name, and reports false
// at the end of the stream.
func (s *metricNameStream) fill(tracker *limiter.MemoryConsumptionTracker) (bool, error) {
	for len(s.batch) == 0 {
		s.release(tracker)
		resp, err := s.recv()
		if errors.Is(err, io.EOF) {
			return false, nil
		}
		if err != nil {
			return false, classifyStoreGatewayError(s.addr, err)
		}
		if err := s.hold(tracker, resp.Counts); err != nil {
			return false, err
		}
	}
	return true, nil
}

// openNameTable sends the index-header name count for parts, which the
// caller has checked to be one block range of series-disjoint blocks, and
// reads each call's first message, which says which blocks it counted.
// Uncounted blocks are retried as for metricNameCounts. The calls that
// answered stay open on the returned table; nothing past their first batch
// is read until the table is merged.
func (q *BlocksStoreQueryable) openNameTable(ctx context.Context, tenantID string, parts []estimatePart, maxIndexBytes int64) (*nameTable, MetricNameCountsResult, error) {
	tracker, _ := limiter.MemoryConsumptionTrackerFromContext(ctx)
	// The streams outlive the calls that open them, so they are opened on
	// their own context rather than on the attempt's, which ends with it.
	streamCtx, cancel := context.WithCancel(ctx)
	t := &nameTable{cancel: cancel, tracker: tracker}
	var mtx sync.Mutex
	var table int64
	cov, err := q.runEstimateCalls(ctx, tenantID, parts, 1, maxIndexBytes, func(_ context.Context, c estimateCall, perCall int64, calls int) (estimateCallResult, error) {
		addr := c.client.RemoteAddress()
		req := &storegatewaypb.MetricNameCountsRequest{BlockIds: convertULIDsToString(c.ids), MaxNames: q.estimateCfg.MaxMetricNames, MaxIndexBytes: perCall}
		stream, err := c.client.MetricNameCounts(grpcContextWithBucketStoreRequestMeta(streamCtx, tenantID, c.part.indexMeta), req)
		if err != nil {
			return estimateCallResult{}, nameTableCallError(addr, err, maxIndexBytes, perCall, calls)
		}
		first, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return estimateCallResult{}, nil
		}
		if err != nil {
			return estimateCallResult{}, nameTableCallError(addr, err, maxIndexBytes, perCall, calls)
		}
		counted, err := parseBlockIDs(addr, first.BlockIds)
		if err != nil {
			return estimateCallResult{}, err
		}
		s := &metricNameStream{addr: addr, recv: stream.Recv}
		mtx.Lock()
		defer mtx.Unlock()
		if err := s.hold(tracker, first.Counts); err != nil {
			return estimateCallResult{}, err
		}
		t.streams = append(t.streams, s)
		table += first.PostingsOffsetsTableBytes
		return estimateCallResult{counted: counted, indexBytes: first.PostingsOffsetsTableBytes}, nil
	})
	if err != nil {
		t.close()
		return nil, MetricNameCountsResult{}, err
	}
	res := MetricNameCountsResult{PostingsOffsetsTableBytes: table}
	if err := res.setCoverage(parts, cov); err != nil {
		t.close()
		return nil, MetricNameCountsResult{}, err
	}
	return t, res, nil
}

// merge calls emit for every name of the table in name order, with its
// count over every stream.
func (t *nameTable) merge(ctx context.Context, emit func(name string, count int64) error) error {
	h := make(nameStreamHeap, 0, len(t.streams))
	for _, s := range t.streams {
		ok, err := s.fill(t.tracker)
		if err != nil {
			return err
		}
		if ok {
			h = append(h, s)
		}
	}
	heap.Init(&h)
	for n := 0; h.Len() > 0; n++ {
		if n%1024 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		name := h[0].batch[0].Name
		var count int64
		for h.Len() > 0 && h[0].batch[0].Name == name {
			s := h[0]
			count += s.batch[0].Count
			s.batch = s.batch[1:]
			ok, err := s.fill(t.tracker)
			if err != nil {
				return err
			}
			if !ok {
				heap.Pop(&h)
				continue
			}
			// The merge is only right for streams sorted by name with each
			// name once, as store-gateways send them.
			if next := s.batch[0].Name; next <= name {
				return withSourceAddr(s.addr, fmt.Errorf("a store-gateway sent metric name %q after %q, out of order", next, name))
			}
			heap.Fix(&h, 0)
		}
		if err := emit(name, count); err != nil {
			return err
		}
	}
	return nil
}

// nameStreamHeap orders streams by their current name.
type nameStreamHeap []*metricNameStream

func (h nameStreamHeap) Len() int           { return len(h) }
func (h nameStreamHeap) Less(i, j int) bool { return h[i].batch[0].Name < h[j].batch[0].Name }
func (h nameStreamHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *nameStreamHeap) Push(x any)        { *h = append(*h, x.(*metricNameStream)) }
func (h *nameStreamHeap) Pop() any {
	old := *h
	s := old[len(old)-1]
	*h = old[:len(old)-1]
	return s
}

// topGroups keeps the n largest groups by count, ties broken by value, as
// the response orders them, charging each kept group to tracker.
type topGroups struct {
	n       int
	h       topGroupHeap
	tracker *limiter.MemoryConsumptionTracker
	// seen is how many groups were offered.
	seen int
}

// offer considers one group.
func (t *topGroups) offer(g estimateGroup) error {
	t.seen++
	if len(t.h) == t.n && !t.h.less(t.h[0], g) {
		return nil
	}
	if t.tracker != nil {
		if err := t.tracker.IncreaseMemoryConsumption(uint64(estimateGroupBytes+len(g.Value)), limiter.CardinalityEstimateGroups); err != nil {
			return refusedEstimate("the groups this estimate holds in the querier: %w", err)
		}
	}
	heap.Push(&t.h, g)
	if len(t.h) > t.n {
		dropped := heap.Pop(&t.h).(estimateGroup)
		if t.tracker != nil {
			t.tracker.DecreaseMemoryConsumption(uint64(estimateGroupBytes+len(dropped.Value)), limiter.CardinalityEstimateGroups)
		}
	}
	return nil
}

// sorted returns the kept groups largest first, and whether any was left
// out.
func (t *topGroups) sorted() ([]estimateGroup, bool) {
	out := make([]estimateGroup, len(t.h))
	for i := len(out) - 1; i >= 0; i-- {
		out[i] = heap.Pop(&t.h).(estimateGroup)
	}
	return out, t.seen > len(out)
}

// topGroupHeap is a min-heap of groups in response order: its root is the
// group a larger one replaces first.
type topGroupHeap []estimateGroup

// less reports whether a sorts after b in the response: a smaller count, or
// the same count and a larger value.
func (topGroupHeap) less(a, b estimateGroup) bool {
	if a.Count != b.Count {
		return a.Count < b.Count
	}
	return a.Value > b.Value
}

func (h topGroupHeap) Len() int           { return len(h) }
func (h topGroupHeap) Less(i, j int) bool { return h.less(h[i], h[j]) }
func (h topGroupHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *topGroupHeap) Push(x any)        { *h = append(*h, x.(estimateGroup)) }
func (h *topGroupHeap) Pop() any {
	old := *h
	g := old[len(old)-1]
	*h = old[:len(old)-1]
	return g
}
