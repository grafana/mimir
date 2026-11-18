// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"cmp"
	"container/heap"
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"sync"

	"github.com/grafana/dskit/concurrency"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/tenant"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/mimir/pkg/cardinality"
	ingester_client "github.com/grafana/mimir/pkg/ingester/client"
)

// errLabelNamesStreamOutOfOrder is returned when an ingester breaks the LabelNamesAndValues
// ordering contract. It is not recoverable: by the time a regression is observed the merge has
// already sealed and released earlier label names, so the only correct response is to fail.
var errLabelNamesStreamOutOfOrder = errors.New("ingester streamed label names out of order")

// LabelNamesCardinalityResult is the fully reduced result of a label names cardinality query.
// LabelNamesCount and LabelValuesCountTotal are exact and unaffected by the limit; Items holds at
// most limit entries, ordered by LabelValuesCount descending then LabelName ascending.
type LabelNamesCardinalityResult struct {
	LabelNamesCount       int
	LabelValuesCountTotal int
	Items                 []LabelNameCardinality
}

// LabelNameCardinality is the number of distinct values observed for a single label name.
type LabelNameCardinality struct {
	LabelName        string
	LabelValuesCount int
}

// LabelNamesCardinality returns the number of distinct values per label name, for series matching
// the input label matchers, keeping only the limit label names with the most values.
//
// The actual series considered eligible depend on countMethod:
//   - inmemory: in-memory series in ingesters.
//   - active: in-memory series in ingesters which are also tracked as active ones.
//
// Ingester responses are merged incrementally rather than accumulated, so peak memory is bounded by
// the number of streams open plus the distinct values of the single widest label name, rather than
// by the tenant's whole label/value universe. This relies on each ingester streaming label names in
// ascending order; see the LabelNamesAndValues RPC contract in ingester.proto.
func (d *Distributor) LabelNamesCardinality(ctx context.Context, matchers []*labels.Matcher, countMethod cardinality.CountMethod, limit int) (LabelNamesCardinalityResult, error) {
	replicationSets, err := d.getIngesterReplicationSetsForQuery(ctx, matchers)
	if err != nil {
		return LabelNamesCardinalityResult{}, err
	}

	req, err := toLabelNamesCardinalityRequest(matchers, countMethod)
	if err != nil {
		return LabelNamesCardinalityResult{}, err
	}

	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return LabelNamesCardinalityResult{}, err
	}
	sizeLimitBytes := d.limits.LabelNamesAndValuesResultsMaxSizeBytes(userID)

	cursors, err := d.openLabelNamesAndValuesStreams(ctx, replicationSets, req)
	if err != nil {
		return LabelNamesCardinalityResult{}, err
	}
	defer func() {
		for _, c := range cursors {
			_ = c.Close()
		}
	}()

	res, err := mergeLabelNamesCardinality(cursors, limit, sizeLimitBytes)
	if err != nil {
		if errors.Is(err, errLabelNamesStreamOutOfOrder) {
			d.labelNamesAndValuesOutOfOrderStreams.Inc()
		}
		return LabelNamesCardinalityResult{}, err
	}
	return res, nil
}

// mergeLabelNamesCardinality merges the cursors, which must each yield label names in ascending
// order, and reduces them to at most limit entries.
//
// Because every cursor is ascending, the smallest label name across all cursor heads is complete
// once every cursor holding it has moved past it: no cursor can produce it again. That is the seal
// point at which the name's value set is counted and released, which is what bounds memory.
// sizeLimitBytes caps the bytes held for any single label name.
func mergeLabelNamesCardinality(cursors []*labelNamesAndValuesCursor, limit, sizeLimitBytes int) (LabelNamesCardinalityResult, error) {
	h := make(labelNamesAndValuesCursorHeap, 0, len(cursors))
	for _, c := range cursors {
		if !c.exhausted() {
			h = append(h, c)
		}
	}
	heap.Init(&h)

	var res LabelNamesCardinalityResult
	// Non-nil even when limit is 0, so the JSON response keeps rendering an empty array.
	top := make(labelNameCardinalityHeap, 0, limit)
	// Reused across label names. Go maps retain their buckets after clear, so this settles at the
	// width of the largest label name, which is the documented bound.
	values := make(map[string]struct{})

	for h.Len() > 0 {
		name := h[0].name()
		clear(values)
		sizeBytes := len(name)

		// Drain every cursor currently sitting on this label name.
		for h.Len() > 0 && h[0].name() == name {
			c := h[0]
			for _, v := range c.values() {
				if _, ok := values[v]; ok {
					continue
				}
				sizeBytes += len(v)
				if sizeBytes > sizeLimitBytes {
					return LabelNamesCardinalityResult{}, fmt.Errorf("size of distinct values for label name %s is %d bytes which exceeds limit %d bytes: %w", name, sizeBytes, sizeLimitBytes, ErrResponseTooLarge)
				}
				values[v] = struct{}{}
			}
			if err := c.advance(); err != nil {
				return LabelNamesCardinalityResult{}, err
			}
			if c.exhausted() {
				heap.Pop(&h)
			} else {
				heap.Fix(&h, 0)
			}
		}

		// Seal: the name is complete, so count it and release its values.
		count := len(values)
		res.LabelNamesCount++
		res.LabelValuesCountTotal += count
		top.tryAdd(LabelNameCardinality{LabelName: name, LabelValuesCount: count}, limit)
	}

	res.Items = top
	slices.SortFunc(res.Items, compareLabelNameCardinality)
	return res, nil
}

// compareLabelNameCardinality orders by LabelValuesCount descending, then LabelName ascending. This
// must stay identical to the ordering the querier applied before the limit was pushed down here.
func compareLabelNameCardinality(a, b LabelNameCardinality) int {
	if a.LabelValuesCount != b.LabelValuesCount {
		return cmp.Compare(b.LabelValuesCount, a.LabelValuesCount)
	}
	return strings.Compare(a.LabelName, b.LabelName)
}

// labelNameCardinalityHeap is a min-heap under the inverse of the output order, so its root is the
// entry that loses its place first. That keeps the retained set at limit entries.
type labelNameCardinalityHeap []LabelNameCardinality

func (h labelNameCardinalityHeap) Len() int { return len(h) }
func (h labelNameCardinalityHeap) Less(i, j int) bool {
	return compareLabelNameCardinality(h[i], h[j]) > 0
}
func (h labelNameCardinalityHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *labelNameCardinalityHeap) Push(x any)   { *h = append(*h, x.(LabelNameCardinality)) }
func (h *labelNameCardinalityHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

// tryAdd admits item if there is room, or if it outranks the current worst entry.
func (h *labelNameCardinalityHeap) tryAdd(item LabelNameCardinality, limit int) {
	if limit <= 0 {
		return
	}
	if h.Len() < limit {
		heap.Push(h, item)
		return
	}
	if compareLabelNameCardinality(item, (*h)[0]) < 0 {
		(*h)[0] = item
		heap.Fix(h, 0)
	}
}

// labelNamesAndValuesCursorHeap orders cursors by the label name at their head.
type labelNamesAndValuesCursorHeap []*labelNamesAndValuesCursor

func (h labelNamesAndValuesCursorHeap) Len() int           { return len(h) }
func (h labelNamesAndValuesCursorHeap) Less(i, j int) bool { return h[i].name() < h[j].name() }
func (h labelNamesAndValuesCursorHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *labelNamesAndValuesCursorHeap) Push(x any) {
	*h = append(*h, x.(*labelNamesAndValuesCursor))
}
func (h *labelNamesAndValuesCursorHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return x
}

// labelNamesAndValuesStream is the Recv surface of Ingester_LabelNamesAndValuesClient.
type labelNamesAndValuesStream interface {
	Recv() (*ingester_client.LabelNamesAndValuesResponse, error)
}

// labelNamesAndValuesCursor is one ingester stream positioned at a single item, holding at most one
// in-flight wire message. Recv is called only when the merge has consumed the buffered message,
// which is what makes gRPC flow control the backpressure mechanism: a fast ingester fills its send
// window and blocks. Not safe for concurrent use.
type labelNamesAndValuesCursor struct {
	stream       labelNamesAndValuesStream
	streamCancel context.CancelCauseFunc
	dscCancel    context.CancelCauseFunc

	msg *ingester_client.LabelNamesAndValuesResponse
	idx int

	prevName string
	seenName bool
	done     bool
	closed   bool
}

// newLabelNamesAndValuesCursor requires streamCancel and dscCancel to either both be non-nil (the
// production path, see openLabelNamesAndValuesStreams) or both be nil (the in-process test path,
// where there is no gRPC stream or ring resource to release). Close uses streamCancel as the guard
// for both, so a mismatched pair would panic on Close.
func newLabelNamesAndValuesCursor(stream labelNamesAndValuesStream, streamCancel, dscCancel context.CancelCauseFunc) *labelNamesAndValuesCursor {
	return &labelNamesAndValuesCursor{stream: stream, streamCancel: streamCancel, dscCancel: dscCancel}
}

func (c *labelNamesAndValuesCursor) exhausted() bool { return c.done }

// name returns the label name at the cursor head. Only valid while the cursor is not exhausted.
func (c *labelNamesAndValuesCursor) name() string { return c.msg.Items[c.idx].LabelName }

// values returns the chunk of values at the cursor head. A label name's values may be split across
// several items and messages, so this is a chunk rather than the complete set.
func (c *labelNamesAndValuesCursor) values() []string { return c.msg.Items[c.idx].Values }

// prime reads the first message, so that an ingester which fails immediately is discovered while
// quorum is still being decided rather than after the participant set has been frozen.
func (c *labelNamesAndValuesCursor) prime() error { return c.fill() }

// advance moves to the next item, reading another message if the current one is used up.
func (c *labelNamesAndValuesCursor) advance() error {
	if c.done {
		return nil
	}
	c.idx++
	if c.idx < len(c.msg.Items) {
		return c.checkOrder()
	}
	return c.fill()
}

// fill reads messages until one carries an item, or the stream ends.
func (c *labelNamesAndValuesCursor) fill() error {
	for {
		msg, err := c.stream.Recv()
		if errors.Is(err, io.EOF) {
			c.done = true
			c.msg = nil
			return nil
		}
		if err != nil {
			return err
		}
		if len(msg.Items) == 0 {
			continue
		}
		c.msg = msg
		c.idx = 0
		return c.checkOrder()
	}
}

// checkOrder enforces the ascending label name contract. A violation means an ingester acknowledged
// an ordering it did not honour, which would silently corrupt the counts, so it is surfaced rather
// than worked around.
func (c *labelNamesAndValuesCursor) checkOrder() error {
	name := c.name()
	if c.seenName && name < c.prevName {
		return fmt.Errorf("%w: received %s after %s", errLabelNamesStreamOutOfOrder, name, c.prevName)
	}
	c.prevName = name
	c.seenName = true
	return nil
}

// Close tears down the RPC and releases the ring resource. Skipping dscCancel leaks, per the dskit
// DoUntilQuorumWithoutSuccessfulContextCancellation contract.
func (c *labelNamesAndValuesCursor) Close() error {
	if c.closed {
		return nil
	}
	c.closed = true
	if c.streamCancel != nil {
		c.streamCancel(nil)
		c.dscCancel(nil)
	}
	return nil
}

// openLabelNamesAndValuesStreams fans the open-stream call out across the quorum-many ingesters and
// returns the surviving streams as cursors.
//
// The unit of work handed to DoUntilQuorum is opening (and priming) the stream, not consuming it.
// That is what makes the merge's seal condition well defined: the participant set is fixed before
// any merging starts. Consuming inside DoUntilQuorum, as the previous accumulate-everything merger
// did, means the set of contributing ingesters can shrink midway through - harmless for a union,
// but not for a merge that seals and releases a label name once every participant has passed it.
//
// The lifecycle rules mirror openIngesterSearchStreams in distributor_search.go, and for the same
// reasons: survivor streams MUST be rooted in the caller's ctx, because both ring.DoUntilQuorum and
// concurrency.ForEachJob cancel their internal contexts on return.
func (d *Distributor) openLabelNamesAndValuesStreams(
	ctx context.Context,
	replicationSets []ring.ReplicationSet,
	req *ingester_client.LabelNamesAndValuesRequest,
) ([]*labelNamesAndValuesCursor, error) {
	quorumConfig := d.queryQuorumConfigForReplicationSets(ctx, replicationSets)

	wrappedF := func(_ context.Context, ingester *ring.InstanceDesc, dscCancel context.CancelCauseFunc) (*labelNamesAndValuesCursor, error) {
		client, err := d.ingesterPool.GetClientForInstance(*ingester)
		if err != nil {
			dscCancel(err)
			return nil, err
		}
		// Survivor streams must root in the caller's ctx - see godoc.
		streamCtx, streamCancel := context.WithCancelCause(ctx)
		stream, err := client.(ingester_client.IngesterClient).LabelNamesAndValues(streamCtx, req)
		if err != nil {
			streamCancel(err)
			dscCancel(err)
			return nil, err
		}
		c := newLabelNamesAndValuesCursor(stream, streamCancel, dscCancel)
		if err := c.prime(); err != nil {
			_ = c.Close()
			return nil, err
		}
		return c, nil
	}

	cleanup := func(c *labelNamesAndValuesCursor) {
		_ = c.Close()
	}

	return collectOrCleanupCursors(ctx, len(replicationSets), func(jobCtx context.Context, idx int) ([]*labelNamesAndValuesCursor, error) {
		return ring.DoUntilQuorumWithoutSuccessfulContextCancellation(jobCtx, replicationSets[idx], quorumConfig, wrappedF, cleanup)
	})
}

// collectOrCleanupCursors runs n jobs in parallel, accumulating the cursors each returns. If any job
// fails, cursors already collected from completed jobs are closed before the error is returned:
// ForEachJob short-circuits and never returns accumulated results, and these streams are rooted in
// the caller's ctx, so its internal cancellation does not reach them.
//
// This mirrors collectOrCleanupSearchSets in distributor_search.go. The two are kept separate so
// that this file stands alone; consolidating them behind a generic helper is worth doing once the
// streaming search path and this one no longer need to be backported independently.
func collectOrCleanupCursors(
	ctx context.Context,
	n int,
	runJob func(jobCtx context.Context, idx int) ([]*labelNamesAndValuesCursor, error),
) ([]*labelNamesAndValuesCursor, error) {
	var (
		cursorsMx sync.Mutex
		cursors   []*labelNamesAndValuesCursor
		succeeded bool
	)
	defer func() {
		if succeeded {
			return
		}
		for _, c := range cursors {
			_ = c.Close()
		}
	}()

	err := concurrency.ForEachJob(ctx, n, 0, func(jobCtx context.Context, idx int) error {
		opened, jobErr := runJob(jobCtx, idx)
		if jobErr != nil {
			return jobErr
		}
		cursorsMx.Lock()
		cursors = append(cursors, opened...)
		cursorsMx.Unlock()
		return nil
	})
	if err != nil {
		return nil, err
	}
	succeeded = true
	return cursors, nil
}
