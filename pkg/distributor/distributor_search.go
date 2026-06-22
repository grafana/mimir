// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"context"
	"errors"
	"io"
	"sync"

	"github.com/grafana/dskit/concurrency"
	"github.com/grafana/dskit/ring"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/metadata"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"

	ingester_client "github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	mimirstorage "github.com/grafana/mimir/pkg/storage"
	"github.com/grafana/mimir/pkg/streaminglabelvalues"
)

type searchWarning string

func (w searchWarning) Error() string { return string(w) }

// ingesterSearchPrefetchBuffer sizes the per-replica prefetch channel to
// match pkg/ingester.searchBatchSize — kept as a literal to avoid pulling
// pkg/ingester into the distributor's import graph.
const ingesterSearchPrefetchBuffer = 256

// SearchLabelNames fans the search RPC out across the quorum-many ingesters,
// wraps each open stream in a prefetcher, and returns a k-way streaming
// merger. Per-replica memory is bounded (prefetch channel + one in-flight
// wire batch); the merger does not re-score. params is converted to the
// wire SearchFilter via paramsToProto; hints.Filter is not read here, only
// hints.OrderBy and hints.Limit at the merge layer.
func (d *Distributor) SearchLabelNames(
	ctx context.Context,
	from, to model.Time,
	params *streaminglabelvalues.Params,
	hints *storage.SearchHints,
	matchers []*labels.Matcher,
) storage.SearchResultSet {
	replicationSets, err := d.getIngesterReplicationSetsForQuery(ctx, matchers)
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
	return storage.MergeSearchResultSets(sources, hints)
}

// SearchLabelValues mirrors SearchLabelNames; the wire request additionally
// carries the label name whose values are being searched.
func (d *Distributor) SearchLabelValues(
	ctx context.Context,
	from, to model.Time,
	name string,
	params *streaminglabelvalues.Params,
	hints *storage.SearchHints,
	matchers []*labels.Matcher,
) storage.SearchResultSet {
	replicationSets, err := d.getIngesterReplicationSetsForQuery(ctx, matchers)
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
	return storage.MergeSearchResultSets(sources, hints)
}

// searchStream is the Recv surface shared by Ingester_SearchLabelNamesClient
// and Ingester_SearchLabelValuesClient.
type searchStream interface {
	Recv() (*ingester_client.SearchResultBatch, error)
}

// openIngesterSearchStreams fans the open-stream call across the quorum-many
// ingesters and returns each as a pre-fetching SearchResultSet.
//
// Critical lifecycle invariant: survivor streams MUST be rooted in the
// caller's ctx, not in any context owned by ring.DoUntilQuorum or
// concurrency.ForEachJob — both helpers cancel their internal contexts on
// return, and a cancelled ctx racing the prefetcher's
// `select { case ch <- r: case <-ctx.Done() }` can silently drop values.
// Close() on the returned set cancels the per-stream ctx (terminating the
// gRPC RPC) AND signals DoUntilQuorum's CancelCauseFunc (resource tracking
// — failing to call it leaks).
//
// Cross-set cleanup: ForEachJob short-circuits on the first job error and
// never returns accumulated results. Survivor streams opened by earlier
// jobs would otherwise leak (their context is the caller's, so ForEachJob's
// internal cancellation does not reach them). The deferred cleanup closes
// anything we have already collected when the function returns an error.
//
// We do not use ring.DoMultiUntilQuorumWithoutSuccessfulContextCancellation
// (as pkg/distributor/query.go does) because it drops the per-set result
// slice when a sibling set errors, with no cleanup hook — which would leak
// the gRPC streams rooted in the caller's ctx. ForEachJob plus a per-set
// DoUntilQuorum plus collectOrCleanupSearchSets gives us the cross-set
// teardown the multi variant lacks.
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
		// Survivor streams must root in the caller's ctx — see godoc.
		streamCtx, streamCancel := context.WithCancelCause(ctx)
		stream, err := open(streamCtx, client.(ingester_client.IngesterClient))
		if err != nil {
			streamCancel(err)
			dscCancel(err)
			return nil, err
		}
		// Embed the two cancel funcs directly on the result-set struct so
		// the per-ingester closure that used to capture them no longer
		// allocates.
		inner := newIngesterSearchResultSet(stream, streamCancel, dscCancel)
		return mimirstorage.NewConcurrentSearchResultSet(ctx, inner, ingesterSearchPrefetchBuffer), nil
	}

	cleanup := func(rs storage.SearchResultSet) {
		_ = rs.Close()
	}

	return collectOrCleanupSearchSets(ctx, len(replicationSets), func(jobCtx context.Context, idx int) ([]storage.SearchResultSet, error) {
		return ring.DoUntilQuorumWithoutSuccessfulContextCancellation(jobCtx, replicationSets[idx], quorumConfig, wrappedF, cleanup)
	})
}

// collectOrCleanupSearchSets runs n jobs in parallel via concurrency.ForEachJob,
// accumulating each job's returned SearchResultSets into a single slice. On
// success it returns the merged slice; if any job errors, every SearchResultSet
// accumulated from already-completed jobs is Close()d before the error returns
// to the caller — preventing stranded gRPC streams when one fan-out branch
// fails after another has already opened streams rooted in the caller's ctx.
func collectOrCleanupSearchSets(
	ctx context.Context,
	n int,
	runJob func(jobCtx context.Context, idx int) ([]storage.SearchResultSet, error),
) ([]storage.SearchResultSet, error) {
	var (
		sourcesMx sync.Mutex
		sources   []storage.SearchResultSet
		succeeded bool
	)
	defer func() {
		if succeeded {
			return
		}
		for _, rs := range sources {
			_ = rs.Close()
		}
	}()

	err := concurrency.ForEachJob(ctx, n, 0, func(jobCtx context.Context, idx int) error {
		sets, jobErr := runJob(jobCtx, idx)
		if jobErr != nil {
			return jobErr
		}
		sourcesMx.Lock()
		sources = append(sources, sets...)
		sourcesMx.Unlock()
		return nil
	})
	if err != nil {
		return nil, err
	}
	succeeded = true
	return sources, nil
}

// ingesterSearchResultSet adapts an ingester search stream client to
// storage.SearchResultSet. Per-batch Warnings accumulate across the full
// stream, including warning-only trailer batches. Not safe for concurrent
// use; cancel runs on Close to tear down the RPC.
type ingesterSearchResultSet struct {
	stream       searchStream
	streamCancel context.CancelCauseFunc
	dscCancel    context.CancelCauseFunc

	batch *ingester_client.SearchResultBatch
	idx   int
	cur   storage.SearchResult

	warnings annotations.Annotations
	err      error
	done     bool
}

// newIngesterSearchResultSet requires streamCancel and dscCancel to
// either both be non-nil (the production path: see
// openIngesterSearchStreams above) or both be nil (the in-process test
// path, where there's no gRPC stream or ring resource to release).
// Close uses streamCancel as the guard for both, so a mismatched pair
// would NPE on Close.
func newIngesterSearchResultSet(stream searchStream, streamCancel, dscCancel context.CancelCauseFunc) *ingesterSearchResultSet {
	return &ingesterSearchResultSet{stream: stream, streamCancel: streamCancel, dscCancel: dscCancel}
}

// Next advances and caches the result in s.cur so At is idempotent (the
// SearchResultSet contract requires multiple At calls between Next calls
// to return the same value).
func (s *ingesterSearchResultSet) Next() bool {
	if s.done || s.err != nil {
		return false
	}
	if s.batch != nil && s.idx < len(s.batch.Results) {
		r := s.batch.Results[s.idx]
		s.cur = convertToStorageResult(r)
		s.idx++
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
			s.warnings.Add(searchWarning(w))
		}
		if len(batch.Results) > 0 {
			s.batch = batch
			s.cur = convertToStorageResult(batch.Results[0])
			s.idx = 1
			return true
		}
		// Warning-only batch — keep pulling.
	}
}

func convertToStorageResult(r ingester_client.SearchResultBatch_Result) storage.SearchResult {
	res := storage.SearchResult{Value: r.Value, Score: r.Score}
	if r.Metadata != nil {
		res.Metadata = &metadata.Metadata{
			Type: mimirpb.MetricMetadataMetricTypeToMetricType(r.Metadata.Type),
			Help: r.Metadata.Help,
			Unit: r.Metadata.Unit,
		}
	}
	return res
}

func (s *ingesterSearchResultSet) At() storage.SearchResult { return s.cur }

func (s *ingesterSearchResultSet) Warnings() annotations.Annotations { return s.warnings }
func (s *ingesterSearchResultSet) Err() error                        { return s.err }
func (s *ingesterSearchResultSet) Close() error {
	if s.streamCancel != nil {
		s.streamCancel(nil)
		s.dscCancel(nil)
		s.streamCancel = nil
		s.dscCancel = nil
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
		Ordering:         orderingToProto(hints),
	}
	if f := paramsToProto(params); f != nil {
		req.Filter = *f
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
		Ordering:         orderingToProto(hints),
		IncludeMetadata:  params != nil && params.IncludeMetadata,
	}
	if f := paramsToProto(params); f != nil {
		req.Filter = *f
	}
	if hints != nil {
		req.Limit = int64(hints.Limit)
	}
	return req, nil
}

// paramsToProto returns nil for nil/empty Params — the ingester treats a
// nil filter as accept-all.
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

// orderingToProto defaults nil hints to ORDER_BY_VALUE_ASC
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
