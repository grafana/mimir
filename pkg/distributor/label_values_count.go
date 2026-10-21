// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"sync"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/tenant"
	"github.com/prometheus/prometheus/model/labels"
	"go.uber.org/atomic"
	"golang.org/x/sync/errgroup"

	"github.com/grafana/mimir/pkg/cardinality"
	ingester_client "github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/globalerror"
	"github.com/grafana/mimir/pkg/util/spanlogger"
	"github.com/grafana/mimir/pkg/util/validation"
)

// maxLabelValuesCountStreamingRetries bounds how many times a request is retried after a stream fails after quorum.
// Every retry re-reads all streams from every ingester, so it must stay small even when the replication sets'
// error budget would allow more, for example with one replication set per partition.
const maxLabelValuesCountStreamingRetries = 2

// errLabelValuesOutOfOrder is returned when an ingester that confirmed sorting sends label values out of order.
// The streaming merge can't recover from it, because it has already counted the earlier values.
var errLabelValuesOutOfOrder = errors.New("ingester sent label names and values out of order")

// LabelValuesCountPerLabelName returns the number of distinct values of each label name for series matching the
// input label matchers. countMethod selects the eligible series, as for LabelNamesAndValues.
//
// When the tenant has the streaming merge enabled and every ingester supports sorting, distinct values are counted
// with a k-way merge of the sorted ingester streams, without holding them in memory. Otherwise, it merges all values
// in memory as LabelNamesAndValues does, and enforces the same size limit.
func (d *Distributor) LabelValuesCountPerLabelName(ctx context.Context, matchers []*labels.Matcher, countMethod cardinality.CountMethod) (map[string]int, error) {
	replicationSets, err := d.getIngesterReplicationSetsForQuery(ctx, matchers)
	if err != nil {
		return nil, err
	}

	req, err := toLabelNamesCardinalityRequest(matchers, countMethod)
	if err != nil {
		return nil, err
	}

	userID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, err
	}

	if d.limits.CardinalityAnalysisStreamingMergeEnabled(userID) {
		return d.labelValuesCountStreaming(ctx, replicationSets, req, userID)
	}

	d.labelNamesAndValuesMerges.WithLabelValues("map").Inc()
	merger, err := d.mergeLabelNamesAndValuesInMemory(ctx, replicationSets, req, userID)
	if err != nil {
		return nil, err
	}
	return merger.valuesCountPerLabelName(), nil
}

// labelValuesCountStreaming asks ingesters for sorted values and counts distinct values with a streaming merge.
// If any ingester doesn't confirm sorting, it falls back to the in-memory merge on the same streams.
//
// Quorum only covers opening the streams, so a stream that fails afterwards would fail the request even when other
// replicas are healthy. In that case the request is retried without the failed replica, using up the replication
// sets' error budget, at most maxLabelValuesCountStreamingRetries times.
func (d *Distributor) labelValuesCountStreaming(ctx context.Context, replicationSets []ring.ReplicationSet, req *ingester_client.LabelNamesAndValuesRequest, userID string) (map[string]int, error) {
	spanLog, ctx := spanlogger.New(ctx, d.log, tracer, "Distributor.labelValuesCountStreaming")
	defer spanLog.Finish()

	req.SortValues = true
	req.TargetMessageSizeBytes = uint64(min(max(16*1024, d.limits.CardinalityAnalysisStreamingMergeMessageSize(userID)), 256*1024))

	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	budget := &labelNamesAndValuesReceiveBudget{
		limit:  d.limits.CardinalityAnalysisStreamingMergeMaxReceivedBytes(userID),
		cancel: cancel,
	}
	budget.err = fmt.Errorf("%s: %w", globalerror.CardinalityAnalysisStreamingMergeReceivedBytesExceeded(budget.limit), ErrResponseTooLarge)
	defer func() { spanLog.SetTag("received_bytes", budget.received.Load()) }()

	for retries := 0; ; retries++ {
		counts, err := d.labelValuesCountStreamingAttempt(ctx, spanLog, replicationSets, req, userID, budget)
		// Cancellation can race with another stream error. Always preserve the budget error.
		if budget.isExceeded() {
			level.Warn(spanLog).Log("msg", "label names cardinality receive budget exceeded", "received_bytes", budget.received.Load(), "limit_bytes", budget.limit)
			return nil, budget.err
		}
		var streamErr *labelNamesAndValuesStreamError
		if !errors.As(err, &streamErr) {
			return counts, err
		}
		if ctx.Err() != nil {
			return nil, streamErr.err
		}
		if retries >= maxLabelValuesCountStreamingRetries {
			level.Warn(spanLog).Log("msg", "an ingester stream failed after quorum and the retry limit is reached, not retrying", "ingester", streamErr.instance.Addr, "zone", streamErr.instance.Zone, "retries", retries, "err", streamErr.err)
			return nil, streamErr.err
		}
		next, ok := excludeFailedReplica(replicationSets, streamErr.instance)
		if !ok {
			level.Warn(spanLog).Log("msg", "an ingester stream failed after quorum and no replica can be excluded, not retrying", "ingester", streamErr.instance.Addr, "zone", streamErr.instance.Zone, "retries", retries, "err", streamErr.err)
			return nil, streamErr.err
		}

		level.Warn(spanLog).Log("msg", "an ingester stream failed after quorum, retrying the request without it", "ingester", streamErr.instance.Addr, "zone", streamErr.instance.Zone, "retries", retries+1, "err", streamErr.err)
		spanLog.SetTag("retries", retries+1)
		d.labelNamesAndValuesStreamingRetries.Inc()
		replicationSets = next
	}
}

// labelNamesAndValuesStreamError marks an error that an ingester's stream returned after quorum, which a retry
// without that ingester may avoid.
type labelNamesAndValuesStreamError struct {
	instance ring.InstanceDesc
	err      error
}

func (e *labelNamesAndValuesStreamError) Error() string { return e.err.Error() }
func (e *labelNamesAndValuesStreamError) Unwrap() error { return e.err }

// streamFailure wraps err from the instance's stream as a labelNamesAndValuesStreamError, unless a retry can't help:
// limit errors and out-of-order errors are returned as they are.
func streamFailure(instance ring.InstanceDesc, err error) error {
	if err == nil || validation.IsLimitError(err) || errors.Is(err, errLabelValuesOutOfOrder) {
		return err
	}
	return &labelNamesAndValuesStreamError{instance: instance, err: err}
}

// excludeFailedReplica returns a copy of the replication sets without the failed instance, using up one unit of
// error budget in each set it changes.
//
// Without zone awareness, only the instance is excluded, from the sets that contain it. With zone awareness, the
// instance's whole zone is excluded, because a zone holds a full copy of the data only with all its instances.
// The zone is excluded from every zone-aware set that still has zone budget, not only the failed instance's: with
// ingest storage each partition is its own set, and a zone whose ingesters are failing would otherwise cost one
// retry per partition.
//
// It returns false if the instance isn't in any set, or a set that contains it has no error budget left.
func excludeFailedReplica(sets []ring.ReplicationSet, failed ring.InstanceDesc) ([]ring.ReplicationSet, bool) {
	out := make([]ring.ReplicationSet, len(sets))
	found := false
	for i, set := range sets {
		out[i] = set
		containsFailed := set.Includes(failed.Addr)
		found = found || containsFailed

		if set.ZoneAwarenessEnabled {
			hasZone := slices.ContainsFunc(set.Instances, func(inst ring.InstanceDesc) bool { return inst.Zone == failed.Zone })
			if !hasZone {
				continue
			}
			if set.MaxUnavailableZones <= 0 {
				if containsFailed {
					return nil, false
				}
				// This set can't lose the zone, and its instances haven't failed.
				continue
			}
			set.MaxUnavailableZones--
			set.Instances = slices.DeleteFunc(slices.Clone(set.Instances), func(inst ring.InstanceDesc) bool { return inst.Zone == failed.Zone })
			out[i] = set
			continue
		}

		if !containsFailed {
			continue
		}
		if set.MaxErrors <= 0 {
			return nil, false
		}
		set.MaxErrors--
		set.Instances = slices.DeleteFunc(slices.Clone(set.Instances), func(inst ring.InstanceDesc) bool { return inst.Addr == failed.Addr })
		out[i] = set
	}
	return out, found
}

// labelValuesCountStreamingAttempt opens the streams through quorum and merges them once.
// Errors from streams after quorum are wrapped with streamFailure.
func (d *Distributor) labelValuesCountStreamingAttempt(ctx context.Context, spanLog *spanlogger.SpanLogger, replicationSets []ring.ReplicationSet, req *ingester_client.LabelNamesAndValuesRequest, userID string, budget *labelNamesAndValuesReceiveBudget) (map[string]int, error) {
	// Each call keeps its stream open after it returns, so that the merge can read the rest of it.
	// The stream owns the call's context and cancels it when closed, as DoMultiUntilQuorumWithoutSuccessfulContextCancellation requires.
	openStream := func(ctx context.Context, ing *ring.InstanceDesc, cancel context.CancelCauseFunc) (*labelNamesAndValuesStream, error) {
		s, err := d.openLabelNamesAndValuesStream(ctx, ing, req, cancel, budget)
		if err != nil {
			cancel(err)
			return nil, err
		}
		return s, nil
	}
	quorumConfig := d.queryQuorumConfigForReplicationSets(ctx, replicationSets)
	quorumConfig.IsTerminalError = validation.IsLimitError

	streams, err := ring.DoMultiUntilQuorumWithoutSuccessfulContextCancellation(ctx, replicationSets, quorumConfig, openStream, (*labelNamesAndValuesStream).close)
	if err != nil {
		return nil, err
	}
	defer func() {
		for _, s := range streams {
			s.close()
		}
	}()

	var unsorted []string
	for _, s := range streams {
		if s.first != nil && !s.first.ValuesSorted {
			unsorted = append(unsorted, s.instance.Addr)
		}
	}
	spanLog.SetTag("streams", len(streams))
	spanLog.SetTag("fallback", len(unsorted) > 0)

	if len(unsorted) > 0 {
		level.Info(spanLog).Log("msg", "some ingesters don't support sorted label values, falling back to the in-memory merge", "streams", len(streams), "unsorted_ingesters", unsorted)
		d.labelNamesAndValuesMerges.WithLabelValues("map_fallback").Inc()
		return mergeLabelNamesAndValuesStreamsInMemory(streams, d.limits.LabelNamesAndValuesResultsMaxSizeBytes(userID))
	}

	d.labelNamesAndValuesMerges.WithLabelValues("streaming").Inc()
	counts, stats, err := countDistinctLabelValues(streams)
	spanLog.SetTag("label_names", len(counts))
	spanLog.SetTag("distinct_values", stats.distinctValues)
	spanLog.SetTag("attempt_received_bytes", stats.receivedBytes)
	if err != nil {
		return nil, err
	}
	return counts, nil
}

// labelNamesAndValuesStream is an open LabelNamesAndValues stream with its first message already received.
type labelNamesAndValuesStream struct {
	instance ring.InstanceDesc
	stream   ingester_client.Ingester_LabelNamesAndValuesClient
	cancel   context.CancelCauseFunc
	// first is the first message, or nil if the stream ended without any message.
	first *ingester_client.LabelNamesAndValuesResponse

	logger    log.Logger
	closeOnce sync.Once
}

// openLabelNamesAndValuesStream opens the stream and receives the first message, so that connection errors
// are handled by the quorum logic and the caller can check if the ingester supports sorting.
func (d *Distributor) openLabelNamesAndValuesStream(ctx context.Context, ing *ring.InstanceDesc, req *ingester_client.LabelNamesAndValuesRequest, cancel context.CancelCauseFunc, budget *labelNamesAndValuesReceiveBudget) (*labelNamesAndValuesStream, error) {
	c, err := d.ingesterPool.GetClientForInstance(*ing)
	if err != nil {
		return nil, err
	}
	stream, err := c.(ingester_client.IngesterClient).LabelNamesAndValues(ctx, req)
	if err != nil {
		return nil, err
	}
	stream = &budgetedLabelNamesAndValuesStream{Ingester_LabelNamesAndValuesClient: stream, budget: budget}
	first, err := stream.Recv()
	if errors.Is(err, io.EOF) {
		// The ingester has no data for the tenant.
		first = nil
	} else if err != nil {
		cancel(err)
		closeLabelNamesAndValuesStream(stream)
		return nil, err
	}
	return &labelNamesAndValuesStream{instance: *ing, stream: stream, cancel: cancel, first: first, logger: d.log}, nil
}

// close cancels the stream's context and releases the stream. It's safe to call more than once, but not while
// another goroutine is receiving from the stream.
func (s *labelNamesAndValuesStream) close() {
	s.closeOnce.Do(func() {
		// Cancel first, so that draining the stream doesn't read the rest of it.
		s.cancel(errStreamClosed)
		closeLabelNamesAndValuesStream(s.stream)
	})
}

// mergeLabelNamesAndValuesStreamsInMemory merges all values of the streams in memory, as LabelNamesAndValues does,
// and returns the number of distinct values of each label name.
func mergeLabelNamesAndValuesStreamsInMemory(streams []*labelNamesAndValuesStream, sizeLimitBytes int) (map[string]int, error) {
	merger := newLabelNamesAndValuesResponseMerger(sizeLimitBytes)

	// The first error is recorded before the other streams are cancelled, otherwise one of them could return its
	// cancellation error first and hide the cause, for example turning ErrResponseTooLarge into a 500.
	var (
		firstErrMtx sync.Mutex
		firstErr    error
	)
	fail := func(err error) {
		firstErrMtx.Lock()
		if firstErr == nil {
			firstErr = err
		}
		firstErrMtx.Unlock()
		for _, s := range streams {
			s.cancel(err)
		}
	}

	g := errgroup.Group{}
	for _, s := range streams {
		if s.first == nil {
			continue
		}
		g.Go(func() error {
			err := merger.putItemsToMap(s.first)
			if err == nil {
				err = streamFailure(s.instance, merger.collectResponses(s.stream))
			}
			if err != nil {
				// Stop the other streams, there's no point in reading them.
				fail(err)
			}
			return err
		})
	}
	if g.Wait() != nil {
		firstErrMtx.Lock()
		defer firstErrMtx.Unlock()
		return nil, firstErr
	}
	return merger.valuesCountPerLabelName(), nil
}

type streamingMergeStats struct {
	distinctValues int
	receivedBytes  int64
}

// countDistinctLabelValues k-way merges streams that are each sorted by (label name, value) and counts the distinct
// values of each label name. Duplicates from other replicas or partitions come out of the merge back to back, so
// they're counted once. It only holds a few messages per stream at a time.
func countDistinctLabelValues(streams []*labelNamesAndValuesStream) (map[string]int, streamingMergeStats, error) {
	var (
		stats         streamingMergeStats
		receivedBytes atomic.Int64
		wg            sync.WaitGroup
		done          = make(chan struct{})
	)
	defer func() {
		// Stop the receiving goroutines before the caller closes the streams: a stream can't be received from concurrently.
		close(done)
		for _, s := range streams {
			s.cancel(errStreamClosed)
		}
		wg.Wait()
	}()

	cursors := make(labelValuesCursorHeap, 0, len(streams))
	for _, s := range streams {
		if s.first == nil {
			continue
		}
		receivedBytes.Add(int64(s.first.Size()))
		// Decoding runs on one goroutine per stream, so it stays parallel while the merge runs on this goroutine.
		messages := make(chan labelNamesAndValuesMessage, 1)
		wg.Add(1)
		go func() {
			defer wg.Done()
			receiveLabelNamesAndValues(s.stream, messages, done, &receivedBytes)
		}()

		c := &labelValuesCursor{instance: s.instance, msg: s.first, messages: messages, logger: s.logger}
		// The first message isn't needed anymore once the cursor holds it.
		s.first = nil
		ok, err := c.next()
		if err != nil {
			return nil, stats, err
		}
		if ok {
			cursors = append(cursors, c)
		}
	}
	heap.Init(&cursors)

	counts := map[string]int{}
	var (
		currName, currValue string
		currCount           int
	)
	for len(cursors) > 0 {
		c := cursors[0]
		switch {
		case currCount == 0 || c.name != currName:
			if currCount > 0 {
				counts[currName] = currCount
			}
			currName, currValue, currCount = c.name, c.value, 1
		case c.value != currValue:
			currValue = c.value
			currCount++
		}

		ok, err := c.next()
		if err != nil {
			return nil, stats, err
		}
		if ok {
			heap.Fix(&cursors, 0)
		} else {
			heap.Pop(&cursors)
		}
	}
	if currCount > 0 {
		counts[currName] = currCount
	}

	for _, n := range counts {
		stats.distinctValues += n
	}
	stats.receivedBytes = receivedBytes.Load()
	return counts, stats, nil
}

type labelNamesAndValuesMessage struct {
	msg *ingester_client.LabelNamesAndValuesResponse
	err error
}

// receiveLabelNamesAndValues receives messages from the stream until it ends, fails, or done is closed.
// It closes out when it returns.
func receiveLabelNamesAndValues(stream ingester_client.Ingester_LabelNamesAndValuesClient, out chan<- labelNamesAndValuesMessage, done <-chan struct{}, receivedBytes *atomic.Int64) {
	defer close(out)
	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return
		}
		if err == nil {
			receivedBytes.Add(int64(msg.Size()))
		}
		select {
		case out <- labelNamesAndValuesMessage{msg: msg, err: err}:
		case <-done:
			return
		}
		if err != nil {
			return
		}
	}
}

// labelValuesCursor iterates the (label name, value) pairs of one stream and checks that they're strictly increasing.
type labelValuesCursor struct {
	logger   log.Logger
	instance ring.InstanceDesc
	messages <-chan labelNamesAndValuesMessage

	msg              *ingester_client.LabelNamesAndValuesResponse
	itemIdx, valIdx  int
	name, value      string
	hasCurrentValues bool
}

// next moves to the next pair. It returns false at the end of the stream.
func (c *labelValuesCursor) next() (bool, error) {
	for {
		if c.msg != nil {
			for c.itemIdx < len(c.msg.Items) {
				item := c.msg.Items[c.itemIdx]
				if c.valIdx < len(item.Values) {
					name, value := item.LabelName, item.Values[c.valIdx]
					c.valIdx++
					if c.hasCurrentValues && name < c.name {
						return false, c.orderError(fmt.Sprintf("label %q followed label %q", name, c.name))
					}
					if c.hasCurrentValues && name == c.name && value <= c.value {
						return false, c.orderError(fmt.Sprintf("values of label %q are out of order", name))
					}
					c.name, c.value, c.hasCurrentValues = name, value, true
					return true, nil
				}
				c.itemIdx++
				c.valIdx = 0
			}
		}

		m, ok := <-c.messages
		if !ok {
			c.msg = nil
			return false, nil
		}
		if m.err != nil {
			return false, streamFailure(c.instance, m.err)
		}
		if !m.msg.ValuesSorted {
			return false, c.orderError("stream stopped confirming sorted values")
		}
		c.msg, c.itemIdx, c.valIdx = m.msg, 0, 0
	}
}

// labelValuesCursorHeap is a min-heap of cursors ordered by their current (label name, value).
type labelValuesCursorHeap []*labelValuesCursor

func (h labelValuesCursorHeap) Len() int { return len(h) }
func (h labelValuesCursorHeap) Less(i, j int) bool {
	if h[i].name != h[j].name {
		return h[i].name < h[j].name
	}
	return h[i].value < h[j].value
}
func (h labelValuesCursorHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *labelValuesCursorHeap) Push(x any)   { *h = append(*h, x.(*labelValuesCursor)) }
func (h *labelValuesCursorHeap) Pop() any {
	old := *h
	c := old[len(old)-1]
	old[len(old)-1] = nil
	*h = old[:len(old)-1]
	return c
}

// orderError keeps the ingester identity in internal logs, outside the returned HTTP error.
func (c *labelValuesCursor) orderError(detail string) error {
	err := fmt.Errorf("%w: %s", errLabelValuesOutOfOrder, detail)
	if c.logger != nil {
		level.Warn(c.logger).Log("msg", "ingester violated sorted label values contract", "ingester", c.instance.Addr, "zone", c.instance.Zone, "err", err)
	}
	return err
}

// labelNamesAndValuesReceiveBudget accounts for uncompressed protobuf bytes across all
// streams, quorum candidates, retries and fallback for a single request. In-flight
// receives may overshoot the limit; cancellation stops further processing.
type labelNamesAndValuesReceiveBudget struct {
	received atomic.Int64
	limit    int64
	cancel   context.CancelCauseFunc
	err      error
}

func (b *labelNamesAndValuesReceiveBudget) isExceeded() bool {
	return b.limit > 0 && b.received.Load() > b.limit
}

// budgetedLabelNamesAndValuesStream charges every message, including the first one
// read while opening streams through quorum, before passing it to either merger.
type budgetedLabelNamesAndValuesStream struct {
	ingester_client.Ingester_LabelNamesAndValuesClient
	budget *labelNamesAndValuesReceiveBudget
}

func (s *budgetedLabelNamesAndValuesStream) Recv() (*ingester_client.LabelNamesAndValuesResponse, error) {
	if s.budget.isExceeded() {
		return nil, s.budget.err
	}
	msg, err := s.Ingester_LabelNamesAndValuesClient.Recv()
	if err == nil {
		s.budget.received.Add(int64(msg.Size()))
	}
	if s.budget.isExceeded() {
		s.budget.cancel(s.budget.err)
		return nil, s.budget.err
	}
	return msg, err
}

// closeLabelNamesAndValuesStream drains the underlying gRPC stream after cancellation.
// An exhausted budget wrapper returns its own error without receiving, so draining
// the wrapper would prevent middleware from observing the stream's terminal status.
func closeLabelNamesAndValuesStream(stream ingester_client.Ingester_LabelNamesAndValuesClient) {
	if budgeted, ok := stream.(*budgetedLabelNamesAndValuesStream); ok {
		stream = budgeted.Ingester_LabelNamesAndValuesClient
	}
	_ = util.CloseAndExhaust[*ingester_client.LabelNamesAndValuesResponse](stream)
}
