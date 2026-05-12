// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"context"
	"errors"
	"io"

	"github.com/grafana/dskit/concurrency"
	"github.com/grafana/dskit/ring"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"

	ingester_client "github.com/grafana/mimir/pkg/ingester/client"
	mimirstorage "github.com/grafana/mimir/pkg/storage"
	"github.com/grafana/mimir/pkg/streaminglabelvalues"
)

// ingesterSearchPrefetchBuffer sizes the per-replica prefetch channel to
// match pkg/ingester.searchBatchSize: one wire batch's worth of results
// can sit in the channel while the producer is in its next gRPC Recv,
// hiding ~one round-trip per source. Above this yields no benefit (the
// upstream stream blocks at batch boundaries); below it leaves gaps where
// the merger waits on the network. Keep in lockstep with the ingester's
// batch size — kept as a literal to avoid pulling pkg/ingester into the
// distributor's import graph for a single constant.
const ingesterSearchPrefetchBuffer = 256

// SearchLabelNames opens a server-streaming SearchLabelNames RPC against the
// quorum-many ingesters in the replication set, wraps each stream as a
// pre-fetching SearchResultSet, and returns a k-way streaming merger across
// them.
//
// Score preservation: leaf-computed scores propagate end-to-end. The
// ingester applied req.Filter already; the merger does not re-score
// (Prometheus's Searcher contract requires Score determinism per
// (Value, Filter)).
//
// Streaming: results flow through the merger without materialising a
// full per-source slice. Per-replica memory is bounded by the prefetch
// channel (ingesterSearchPrefetchBuffer entries) plus one in-flight wire
// batch held by ingesterSearchResultSet; the merger holds at most one
// head per source. Closing the returned SearchResultSet propagates
// cancellation through every open stream.
//
// params is converted to a wire SearchFilter via paramsToProto and sent to
// each ingester, which builds its own filter chain from the wire shape.
// hints.OrderBy and hints.Limit are applied at the merge layer; hints.Filter
// is not read here — the leaf-side filter is reconstructed from params on
// each ingester.
//
// from/to bound the query time range; the caller supplies them from
// whichever upstream produced the request.
func (d *Distributor) SearchLabelNames(
	ctx context.Context,
	from, to model.Time,
	params *streaminglabelvalues.Params,
	hints *storage.SearchHints,
	matchers []*labels.Matcher,
) storage.SearchResultSet {
	replicationSets, err := d.getIngesterReplicationSetsForQuery(ctx)
	if err != nil {
		return storage.ErrSearchResultSet(err)
	}
	req, err := buildSearchLabelNamesRequest(from, to, params, hints, matchers)
	if err != nil {
		return storage.ErrSearchResultSet(err)
	}
	sources, err := d.openIngesterSearchStreams(ctx, replicationSets, func(rpcCtx context.Context, c ingester_client.IngesterClient) (searchStream, error) {
		return c.SearchLabelNames(rpcCtx, req)
	})
	if err != nil {
		return storage.ErrSearchResultSet(err)
	}
	return mimirstorage.NewMergingSearchResultSet(sources, hints)
}

// SearchLabelValues opens a server-streaming SearchLabelValues RPC against
// the quorum-many ingesters in the replication set, wraps each stream as a
// pre-fetching SearchResultSet, and returns a k-way streaming merger across
// them. Mirrors SearchLabelNames; differs only in the wire request shape
// (carries the Name field for the label whose values are being searched).
//
// See SearchLabelNames for the streaming / score-preservation / no-re-filter
// rationale and the params/hints/filter contract.
func (d *Distributor) SearchLabelValues(
	ctx context.Context,
	from, to model.Time,
	name string,
	params *streaminglabelvalues.Params,
	hints *storage.SearchHints,
	matchers []*labels.Matcher,
) storage.SearchResultSet {
	replicationSets, err := d.getIngesterReplicationSetsForQuery(ctx)
	if err != nil {
		return storage.ErrSearchResultSet(err)
	}
	req, err := buildSearchLabelValuesRequest(from, to, name, params, hints, matchers)
	if err != nil {
		return storage.ErrSearchResultSet(err)
	}
	sources, err := d.openIngesterSearchStreams(ctx, replicationSets, func(rpcCtx context.Context, c ingester_client.IngesterClient) (searchStream, error) {
		return c.SearchLabelValues(rpcCtx, req)
	})
	if err != nil {
		return storage.ErrSearchResultSet(err)
	}
	return mimirstorage.NewMergingSearchResultSet(sources, hints)
}

// searchStream is the common Recv-only surface of
// Ingester_SearchLabelNamesClient and Ingester_SearchLabelValuesClient.
// Both stream types return *ingester_client.SearchResultBatch from Recv,
// so a single iterator implementation handles both.
type searchStream interface {
	Recv() (*ingester_client.SearchResultBatch, error)
}

// openIngesterSearchStreams opens a server-streaming RPC against the
// quorum-many ingesters in each replication set, wraps each open stream as
// a pre-fetching storage.SearchResultSet, and returns the slice for
// feeding into a k-way merger.
//
// Lifecycle: the streams' gRPC contexts and the concurrent prefetcher's
// goroutine context are both derived from the CALLER's ctx, not from any
// context owned by ring.DoUntilQuorum or concurrency.ForEachJobMergeResults.
// Both of those helpers cancel their internal contexts when they return —
// which is correct for the open-stream phase (cancelling losers) but would
// be catastrophic for survivors: the prefetcher's `select { case ch <- r:
// case <-ctx.Done() }` can pick the cancellation branch and silently drop
// values. By rooting the survivor streams in the caller's ctx we ensure
// they live as long as the SearchResultSet the caller holds, and die only
// when the caller cancels or explicitly Closes.
//
// Each returned SearchResultSet's Close() does two things: signal the
// CancelCauseFunc supplied by ring.DoUntilQuorum (to satisfy that helper's
// resource-tracking contract — failing to call it leaks), AND cancel the
// stream-specific context to actually terminate the gRPC RPC.
// Non-quorum streams are closed via the cleanup callback before they ever
// reach the caller.
func (d *Distributor) openIngesterSearchStreams(
	ctx context.Context,
	replicationSets []ring.ReplicationSet,
	open func(rpcCtx context.Context, c ingester_client.IngesterClient) (searchStream, error),
) ([]storage.SearchResultSet, error) {
	quorumConfig := d.queryQuorumConfigForReplicationSets(ctx, replicationSets)

	wrappedF := func(_ context.Context, ingester *ring.InstanceDesc, dscCancel context.CancelCauseFunc) (storage.SearchResultSet, error) {
		client, err := d.ingesterPool.GetClientForInstance(*ingester)
		if err != nil {
			dscCancel(err)
			return nil, err
		}
		// Root the stream's ctx in the CALLER's ctx, deliberately ignoring
		// the per-instance ctx supplied by DoUntilQuorum. See the function
		// godoc for why.
		streamCtx, streamCancel := context.WithCancel(ctx)
		stream, err := open(streamCtx, client.(ingester_client.IngesterClient))
		if err != nil {
			streamCancel()
			dscCancel(err)
			return nil, err
		}
		// Close() on the SearchResultSet must (a) terminate the gRPC RPC
		// and (b) satisfy DoUntilQuorum's CancelCauseFunc contract.
		inner := newIngesterSearchResultSet(stream, func() {
			streamCancel()
			dscCancel(nil)
		})
		return mimirstorage.NewConcurrentSearchResultSet(ctx, inner, ingesterSearchPrefetchBuffer), nil
	}

	// cleanup is called for results that DoUntilQuorum produced but is
	// discarding (non-quorum or excess). Closing the SearchResultSet
	// cancels its stream context and releases the goroutine.
	cleanup := func(rs storage.SearchResultSet) {
		_ = rs.Close()
	}

	return concurrency.ForEachJobMergeResults[ring.ReplicationSet, storage.SearchResultSet](
		ctx, replicationSets, 0,
		func(jobCtx context.Context, set ring.ReplicationSet) ([]storage.SearchResultSet, error) {
			return ring.DoUntilQuorumWithoutSuccessfulContextCancellation(jobCtx, set, quorumConfig, wrappedF, cleanup)
		})
}

// ingesterSearchResultSet adapts a server-streaming ingester search RPC
// client to storage.SearchResultSet. Buffers one *SearchResultBatch at a
// time, indexes into it on Next/At, and pulls a fresh batch on exhaustion.
// Per-batch Warnings accumulate into the iterator's annotations across
// the full stream, including warning-only trailer batches.
//
// Concurrency: not safe for concurrent use. Each goroutine that drains a
// stream must own its iterator.
type ingesterSearchResultSet struct {
	stream searchStream
	cancel func()

	batch *ingester_client.SearchResultBatch
	idx   int

	warnings annotations.Annotations
	err      error
	done     bool
}

// newIngesterSearchResultSet wraps a stream client as a SearchResultSet.
// cancel is invoked from Close() to tear down the underlying RPC (typically
// the context.CancelCauseFunc supplied by
// ring.DoUntilQuorumWithoutSuccessfulContextCancellation).
func newIngesterSearchResultSet(stream searchStream, cancel func()) *ingesterSearchResultSet {
	return &ingesterSearchResultSet{stream: stream, cancel: cancel}
}

func (s *ingesterSearchResultSet) Next() bool {
	if s.done || s.err != nil {
		return false
	}
	if s.batch != nil && s.idx < len(s.batch.Results) {
		return true
	}
	for {
		batch, err := s.stream.Recv()
		if errors.Is(err, io.EOF) {
			s.done = true
			return false
		}
		if err != nil {
			s.err = err
			return false
		}
		for _, w := range batch.Warnings {
			s.warnings.Add(errors.New(w))
		}
		if len(batch.Results) > 0 {
			s.batch = batch
			s.idx = 0
			return true
		}
		// Warning-only batch — keep pulling.
	}
}

func (s *ingesterSearchResultSet) At() storage.SearchResult {
	r := s.batch.Results[s.idx]
	s.idx++
	return storage.SearchResult{Value: r.Value, Score: r.Score}
}

func (s *ingesterSearchResultSet) Warnings() annotations.Annotations { return s.warnings }
func (s *ingesterSearchResultSet) Err() error                        { return s.err }
func (s *ingesterSearchResultSet) Close() error {
	if s.cancel != nil {
		s.cancel()
		s.cancel = nil
	}
	return nil
}

func buildSearchLabelNamesRequest(from, to model.Time, params *streaminglabelvalues.Params, hints *storage.SearchHints, matchers []*labels.Matcher) (*ingester_client.SearchLabelNamesRequest, error) {
	wireMatchers, err := ingester_client.ToLabelMatchers(matchers)
	if err != nil {
		return nil, err
	}
	req := &ingester_client.SearchLabelNamesRequest{
		StartTimestampMs: int64(from),
		EndTimestampMs:   int64(to),
		Matchers:         wireMatchers,
		Filter:           paramsToProto(params),
		Ordering:         orderingToProto(hints),
	}
	if hints != nil {
		req.Limit = int64(hints.Limit)
	}
	return req, nil
}

func buildSearchLabelValuesRequest(from, to model.Time, name string, params *streaminglabelvalues.Params, hints *storage.SearchHints, matchers []*labels.Matcher) (*ingester_client.SearchLabelValuesRequest, error) {
	wireMatchers, err := ingester_client.ToLabelMatchers(matchers)
	if err != nil {
		return nil, err
	}
	req := &ingester_client.SearchLabelValuesRequest{
		Name:             name,
		StartTimestampMs: int64(from),
		EndTimestampMs:   int64(to),
		Matchers:         wireMatchers,
		Filter:           paramsToProto(params),
		Ordering:         orderingToProto(hints),
	}
	if hints != nil {
		req.Limit = int64(hints.Limit)
	}
	return req, nil
}

// paramsToProto converts the wire-decoupled streaminglabelvalues.Params into
// the ingester SearchFilter proto. Returns nil when p is nil or has no
// terms — the ingester then scores every value at 1.0.
func paramsToProto(p *streaminglabelvalues.Params) *ingester_client.SearchFilter {
	if p == nil || len(p.Terms) == 0 {
		return nil
	}
	wf := &ingester_client.SearchFilter{
		Terms:           p.Terms,
		CaseInsensitive: !p.CaseSensitive,
		FuzzThreshold:   int32(p.FuzzThreshold),
	}
	switch p.FuzzAlg {
	case streaminglabelvalues.FuzzAlgJaroWinkler:
		wf.FuzzAlg = ingester_client.FUZZ_ALG_JARO_WINKLER
	default:
		wf.FuzzAlg = ingester_client.FUZZ_ALG_SUBSEQUENCE
	}
	return wf
}

// orderingToProto translates hints.OrderBy into the wire enum. Defaults to
// ORDER_BY_VALUE_ASC when hints is nil — same default applied by
// mimirstorage.NewMergingSearchResultSet at the merge side, keeping leaf-
// side and merge-side ordering aligned.
func orderingToProto(hints *storage.SearchHints) ingester_client.SearchOrdering {
	if hints == nil {
		return ingester_client.ORDER_BY_VALUE_ASC
	}
	switch hints.OrderBy {
	case storage.OrderByValueDesc:
		return ingester_client.ORDER_BY_VALUE_DESC
	case storage.OrderByScoreDesc:
		return ingester_client.ORDER_BY_SCORE_DESC
	default:
		return ingester_client.ORDER_BY_VALUE_ASC
	}
}
