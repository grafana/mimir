// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"context"
	"io"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/tenant"
	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/annotations"
	"golang.org/x/sync/errgroup"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
	"github.com/grafana/mimir/pkg/streaminglabelvalues"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

// errMissingSearchHeader is returned when a store-gateway opens a Search* stream
// but does not send the mandatory header batch (a batch carrying
// response_hints.queried_blocks) as its first message. The consistency tracker
// cannot run without it, so we treat this as a non-retriable protocol violation.
var errMissingSearchHeader = errors.New("store-gateway omitted the header batch on the Search* stream (response_hints.queried_blocks must be the first message)")

// errUnexpectedResultsInSearchHeader is returned when a store-gateway sends a
// header batch (one carrying response_hints.queried_blocks) that also populates
// results. The header batch must be header-only: readSGSearchHeader consumes
// it and constructs the live source over the remainder of the stream, so any
// results on the header batch would be silently dropped.
var errUnexpectedResultsInSearchHeader = errors.New("store-gateway sent results on the Search* header batch (the header batch must carry only response_hints.queried_blocks)")

// searchWarning lifts a wire warning string to an error without the per-call
// allocation of errors.New. Used to feed annotations.Annotations.Add.
type searchWarning string

func (w searchWarning) Error() string { return string(w) }

// SearchLabelNames fans out across the store-gateways owning the relevant
// blocks, reads each stream's header batch synchronously to capture the
// queried-block set for the consistency check, and returns a k-way streaming
// merger over the live per-SG sources. Per-SG memory is bounded to one wire
// batch in flight. Leaf scores propagate verbatim — no re-filter at the merge
// layer.
func (q *blocksStoreQuerier) SearchLabelNames(
	ctx context.Context,
	params *streaminglabelvalues.Params,
	hints *storage.SearchHints,
	matchers ...*labels.Matcher,
) storage.SearchResultSet {
	spanLog, ctx := spanlogger.New(ctx, q.logger, tracer, "blocksStoreQuerier.SearchLabelNames")
	defer spanLog.Finish()

	tenantID, err := tenant.TenantID(ctx)
	if err != nil {
		return storage.ErrSearchResultSet(err)
	}

	minT, maxT := q.minT, q.maxT
	spanLog.DebugLog("start", util.TimeFromMillis(minT).UTC().String(), "end",
		util.TimeFromMillis(maxT).UTC().String(), "matchers", util.MatchersStringer(matchers))

	// Clamp minT to MaxLabelsQueryLength; mirrors blocksStoreQuerier.LabelNames.
	maxQueryLength := q.limits.MaxLabelsQueryLength(tenantID)
	if maxQueryLength != 0 {
		minT = clampToMaxLabelQueryLength(spanLog, minT, maxT, time.Now().UnixMilli(), maxQueryLength.Milliseconds())
	}

	var (
		mtx               sync.Mutex
		resSources        []storage.SearchResultSet
		convertedMatchers = convertMatchersToLabelMatcher(matchers)
	)

	queryF := func(clients map[BlocksStoreClient][]ulid.ULID, qMinT, qMaxT int64, indexMeta *bucketindex.Metadata) ([]ulid.ULID, error) {
		sources, queriedBlocks, err := q.fetchSearchLabelNamesFromStore(ctx, clients, qMinT, qMaxT, tenantID, params, hints, convertedMatchers, indexMeta)
		if err != nil {
			return nil, err
		}
		mtx.Lock()
		resSources = append(resSources, sources...)
		mtx.Unlock()
		return queriedBlocks, nil
	}

	if err := q.queryWithConsistencyCheck(ctx, spanLog, minT, maxT, tenantID, nil, queryF); err != nil {
		closeSearchResultSets(resSources)
		return storage.ErrSearchResultSet(err)
	}

	return storage.MergeSearchResultSets(resSources, hints)
}

// SearchLabelValues mirrors SearchLabelNames; the wire request additionally
// carries the label whose values are being searched.
func (q *blocksStoreQuerier) SearchLabelValues(
	ctx context.Context,
	name string,
	params *streaminglabelvalues.Params,
	hints *storage.SearchHints,
	matchers ...*labels.Matcher,
) storage.SearchResultSet {
	spanLog, ctx := spanlogger.New(ctx, q.logger, tracer, "blocksStoreQuerier.SearchLabelValues")
	defer spanLog.Finish()

	tenantID, err := tenant.TenantID(ctx)
	if err != nil {
		return storage.ErrSearchResultSet(err)
	}

	minT, maxT := q.minT, q.maxT
	spanLog.DebugLog("name", name, "start", util.TimeFromMillis(minT).UTC().String(), "end",
		util.TimeFromMillis(maxT).UTC().String(), "matchers", util.MatchersStringer(matchers))

	// Clamp minT to MaxLabelsQueryLength; mirrors blocksStoreQuerier.LabelValues.
	maxQueryLength := q.limits.MaxLabelsQueryLength(tenantID)
	if maxQueryLength != 0 {
		minT = clampToMaxLabelQueryLength(spanLog, minT, maxT, time.Now().UnixMilli(), maxQueryLength.Milliseconds())
	}

	var (
		mtx               sync.Mutex
		resSources        []storage.SearchResultSet
		convertedMatchers = convertMatchersToLabelMatcher(matchers)
	)

	queryF := func(clients map[BlocksStoreClient][]ulid.ULID, qMinT, qMaxT int64, indexMeta *bucketindex.Metadata) ([]ulid.ULID, error) {
		sources, queriedBlocks, err := q.fetchSearchLabelValuesFromStore(ctx, name, clients, qMinT, qMaxT, tenantID, params, hints, convertedMatchers, indexMeta)
		if err != nil {
			return nil, err
		}
		mtx.Lock()
		resSources = append(resSources, sources...)
		mtx.Unlock()
		return queriedBlocks, nil
	}

	if err := q.queryWithConsistencyCheck(ctx, spanLog, minT, maxT, tenantID, nil, queryF); err != nil {
		closeSearchResultSets(resSources)
		return storage.ErrSearchResultSet(err)
	}

	return storage.MergeSearchResultSets(resSources, hints)
}

// fetchSearchLabelNamesFromStore opens one stream per store-gateway client in
// parallel, reads each stream's mandatory header batch to capture the
// queried-block set, and returns one live storage.SearchResultSet per SG
// wrapping the rest of the stream. The caller's ctx (not the errgroup's gCtx)
// roots every per-stream context so streams survive errgroup teardown and stay
// alive across consistency-check attempts until the merger consumes them or
// the caller closes them.
//
// Retriable open- or header-Recv errors drop the SG's contribution (the
// consistency tracker will see the un-credited blocks as still-missing and
// retry against another SG). Non-retriable errors short-circuit the errgroup;
// every source already collected is closed before returning.
func (q *blocksStoreQuerier) fetchSearchLabelNamesFromStore(
	ctx context.Context,
	clients map[BlocksStoreClient][]ulid.ULID,
	minT int64,
	maxT int64,
	tenantID string,
	params *streaminglabelvalues.Params,
	hints *storage.SearchHints,
	wireMatchers []storepb.LabelMatcher,
	indexMeta *bucketindex.Metadata,
) ([]storage.SearchResultSet, []ulid.ULID, error) {
	reqCtx := grpcContextWithBucketStoreRequestMeta(ctx, tenantID, indexMeta)
	spanLog := spanlogger.FromContext(ctx, q.logger)

	var (
		g, gCtx       = errgroup.WithContext(reqCtx)
		mtx           sync.Mutex
		sources       []storage.SearchResultSet
		queriedBlocks []ulid.ULID
	)

	for c, blockIDs := range clients {
		g.Go(func() error {
			source, myBlocks, retriable, err := q.openSearchLabelNamesStream(reqCtx, gCtx, c, blockIDs, minT, maxT, params, hints, wireMatchers)
			if err != nil {
				if retriable {
					level.Warn(spanLog).Log("msg", "failed to open search label names stream; error is retriable", "remote", c.RemoteAddress(), "err", err)
					return nil
				}
				return errors.Wrapf(err, "non-retriable error while fetching search label names from store %s", c.RemoteAddress())
			}

			spanLog.DebugLog("msg", "received header from store-gateway",
				"instance", c,
				"requested blocks", strings.Join(convertULIDsToString(blockIDs), " "),
				"queried blocks", strings.Join(convertULIDsToString(myBlocks), " "))

			mtx.Lock()
			sources = append(sources, source)
			queriedBlocks = append(queriedBlocks, myBlocks...)
			mtx.Unlock()
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		closeSearchResultSets(sources)
		return nil, nil, err
	}
	return sources, queriedBlocks, nil
}

// fetchSearchLabelValuesFromStore mirrors fetchSearchLabelNamesFromStore.
func (q *blocksStoreQuerier) fetchSearchLabelValuesFromStore(
	ctx context.Context,
	name string,
	clients map[BlocksStoreClient][]ulid.ULID,
	minT int64,
	maxT int64,
	tenantID string,
	params *streaminglabelvalues.Params,
	hints *storage.SearchHints,
	wireMatchers []storepb.LabelMatcher,
	indexMeta *bucketindex.Metadata,
) ([]storage.SearchResultSet, []ulid.ULID, error) {
	reqCtx := grpcContextWithBucketStoreRequestMeta(ctx, tenantID, indexMeta)
	spanLog := spanlogger.FromContext(ctx, q.logger)

	var (
		g, gCtx       = errgroup.WithContext(reqCtx)
		mtx           sync.Mutex
		sources       []storage.SearchResultSet
		queriedBlocks []ulid.ULID
	)

	for c, blockIDs := range clients {
		g.Go(func() error {
			source, myBlocks, retriable, err := q.openSearchLabelValuesStream(reqCtx, gCtx, c, name, blockIDs, minT, maxT, params, hints, wireMatchers)
			if err != nil {
				if retriable {
					level.Warn(spanLog).Log("msg", "failed to open search label values stream; error is retriable", "remote", c.RemoteAddress(), "err", err)
					return nil
				}
				return errors.Wrapf(err, "non-retriable error while fetching search label values from store %s", c.RemoteAddress())
			}

			spanLog.DebugLog("msg", "received header from store-gateway",
				"instance", c,
				"label", name,
				"requested blocks", strings.Join(convertULIDsToString(blockIDs), " "),
				"queried blocks", strings.Join(convertULIDsToString(myBlocks), " "))

			mtx.Lock()
			sources = append(sources, source)
			queriedBlocks = append(queriedBlocks, myBlocks...)
			mtx.Unlock()
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		closeSearchResultSets(sources)
		return nil, nil, err
	}
	return sources, queriedBlocks, nil
}

// openSearchLabelNamesStream opens the SG stream, reads the mandatory header
// batch, and returns a live source over the remainder. The per-stream ctx is
// rooted in the caller's ctx so the stream survives the errgroup that opened
// it. The retriable bool tells the caller to drop this SG's contribution and
// let the consistency tracker observe the missing blocks (rather than failing
// the query). The stream is always either handed to the caller as a live
// source or cancelled before this function returns.
func (q *blocksStoreQuerier) openSearchLabelNamesStream(
	ctx context.Context,
	gCtx context.Context,
	c BlocksStoreClient,
	blockIDs []ulid.ULID,
	minT, maxT int64,
	params *streaminglabelvalues.Params,
	hints *storage.SearchHints,
	wireMatchers []storepb.LabelMatcher,
) (storage.SearchResultSet, []ulid.ULID, bool, error) {
	streamCtx, streamCancel := context.WithCancelCause(ctx)
	req := buildSGSearchLabelNamesRequest(minT, maxT, blockIDs, params, hints, wireMatchers)
	stream, err := c.SearchLabelNames(streamCtx, req)
	if err != nil {
		streamCancel(err)
		return nil, nil, shouldRetry(err), err
	}
	source, myBlocks, retriable, err := readSGSearchHeader(gCtx, stream, streamCancel)
	if err != nil {
		return nil, nil, retriable, err
	}
	return source, myBlocks, false, nil
}

// openSearchLabelValuesStream mirrors openSearchLabelNamesStream.
func (q *blocksStoreQuerier) openSearchLabelValuesStream(
	ctx context.Context,
	gCtx context.Context,
	c BlocksStoreClient,
	name string,
	blockIDs []ulid.ULID,
	minT, maxT int64,
	params *streaminglabelvalues.Params,
	hints *storage.SearchHints,
	wireMatchers []storepb.LabelMatcher,
) (storage.SearchResultSet, []ulid.ULID, bool, error) {
	streamCtx, streamCancel := context.WithCancelCause(ctx)
	req := buildSGSearchLabelValuesRequest(minT, maxT, name, blockIDs, params, hints, wireMatchers)
	stream, err := c.SearchLabelValues(streamCtx, req)
	if err != nil {
		streamCancel(err)
		return nil, nil, shouldRetry(err), err
	}
	source, myBlocks, retriable, err := readSGSearchHeader(gCtx, stream, streamCancel)
	if err != nil {
		return nil, nil, retriable, err
	}
	return source, myBlocks, false, nil
}

// readSGSearchHeader blocks on a single Recv to consume the SG's mandatory
// header batch, then constructs a live sgSearchResultSet over the remainder.
// On any error before the source is constructed, the stream's ctx is cancelled
// so the goroutine does not leak.
//
// A nil response_hints field is treated as a non-retriable protocol violation
// — the consistency tracker has no way to credit (or correctly classify as
// missing) the SG's blocks without it. A non-empty results field on the
// header batch is likewise rejected as a non-retriable protocol violation:
// the header batch must be header-only because it is consumed before the
// live source is constructed, so any results on it would be silently
// dropped. A header batch may itself carry warnings, which are seeded into
// the adapter so they are still surfaced to the merger's Warnings().
//
// gCtx is the errgroup ctx. The pre-Recv check bails without reading when a
// peer goroutine has already errored. A watcher goroutine then propagates any
// in-flight gCtx cancellation onto streamCancel: the SG only sends the header
// after its full per-block scan completes, so without this watcher a
// peer-goroutine failure would leave every surviving SG's Recv blocked for
// the duration of its own scan, holding up g.Wait() and wasting SG work whose
// results would be discarded. On the success path the deferred close exits
// the watcher before the live source is handed to the caller, so a healthy
// stream is never cancelled by the watcher.
func readSGSearchHeader(gCtx context.Context, stream sgSearchStream, streamCancel context.CancelCauseFunc) (storage.SearchResultSet, []ulid.ULID, bool, error) {
	if err := gCtx.Err(); err != nil {
		streamCancel(err)
		return nil, nil, false, err
	}
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-gCtx.Done():
			streamCancel(gCtx.Err())
		case <-stop:
		}
	}()
	header, err := stream.Recv()
	if err != nil {
		streamCancel(err)
		return nil, nil, shouldRetry(err), err
	}
	if header.ResponseHints == nil {
		streamCancel(errMissingSearchHeader)
		return nil, nil, false, errMissingSearchHeader
	}
	if len(header.Results) > 0 {
		streamCancel(errUnexpectedResultsInSearchHeader)
		return nil, nil, false, errUnexpectedResultsInSearchHeader
	}
	myBlocks, err := convertBlockHintsToULIDs(header.ResponseHints.QueriedBlocks)
	if err != nil {
		streamCancel(err)
		return nil, nil, false, errors.Wrap(err, "failed to parse queried block IDs from search header")
	}
	source := newSGSearchResultSet(stream, func() { streamCancel(nil) }, headerWarnings(header))
	return source, myBlocks, false, nil
}

// headerWarnings translates the optional warnings on the header batch into
// annotations so the per-SG adapter can surface them through Warnings(). Most
// SGs will not attach warnings to the header but the proto allows it.
func headerWarnings(header *storepb.SearchResultBatch) annotations.Annotations {
	if len(header.Warnings) == 0 {
		return nil
	}
	out := make(annotations.Annotations, len(header.Warnings))
	for _, w := range header.Warnings {
		out.Add(searchWarning(w))
	}
	return out
}

// closeSearchResultSets closes each source, ignoring per-source errors. Used
// to tear down already-opened streams on the error path so we don't leak gRPC
// resources when one fan-out branch fails after another has opened streams.
func closeSearchResultSets(sources []storage.SearchResultSet) {
	for _, s := range sources {
		_ = s.Close()
	}
}

// sgSearchResultSet adapts a store-gateway search stream client to
// storage.SearchResultSet. Holds at most one wire batch in memory at a time;
// per-batch warnings accumulate across the full stream, including
// warning-only trailer batches. Not safe for concurrent use; cancel runs on
// Close to tear down the RPC.
type sgSearchResultSet struct {
	stream sgSearchStream
	cancel func()

	batch *storepb.SearchResultBatch
	idx   int
	cur   storage.SearchResult

	warnings annotations.Annotations
	err      error
	done     bool
}

func newSGSearchResultSet(stream sgSearchStream, cancel func(), warnings annotations.Annotations) *sgSearchResultSet {
	return &sgSearchResultSet{stream: stream, cancel: cancel, warnings: warnings}
}

// Next advances and caches the result in s.cur so At is idempotent. Mirrors
// the contract of pkg/distributor/distributor_search.go::ingesterSearchResultSet.
func (s *sgSearchResultSet) Next() bool {
	if s.done || s.err != nil {
		return false
	}
	if s.batch != nil && s.idx < len(s.batch.Results) {
		r := s.batch.Results[s.idx]
		s.cur = storage.SearchResult{Value: r.Value, Score: r.Score}
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
			r := batch.Results[0]
			s.cur = storage.SearchResult{Value: r.Value, Score: r.Score}
			s.idx = 1
			return true
		}
		// Warning-only batch — keep pulling.
	}
}

func (s *sgSearchResultSet) At() storage.SearchResult          { return s.cur }
func (s *sgSearchResultSet) Warnings() annotations.Annotations { return s.warnings }
func (s *sgSearchResultSet) Err() error                        { return s.err }
func (s *sgSearchResultSet) Close() error {
	if s.cancel != nil {
		s.cancel()
		s.cancel = nil
	}
	return nil
}

// buildSGSearchLabelNamesRequest assembles the wire request, scoping the SG
// search to the blocks assigned by queryWithConsistencyCheck via request_hints
// so the SG does not re-scan every block it owns on every replica attempt.
func buildSGSearchLabelNamesRequest(
	minT, maxT int64,
	blockIDs []ulid.ULID,
	params *streaminglabelvalues.Params,
	hints *storage.SearchHints,
	wireMatchers []storepb.LabelMatcher,
) *storepb.SearchLabelNamesRequest {
	req := &storepb.SearchLabelNamesRequest{
		Start:        minT,
		End:          maxT,
		Matchers:     wireMatchers,
		Filter:       paramsToSGProto(params),
		Ordering:     orderingToSGProto(hints),
		RequestHints: &storepb.SearchLabelNamesRequestHints{BlockMatchers: blockIDsToBlockMatchers(blockIDs)},
	}
	if hints != nil {
		req.Limit = int64(hints.Limit)
	}
	return req
}

// buildSGSearchLabelValuesRequest mirrors buildSGSearchLabelNamesRequest
// with the additional Label field.
func buildSGSearchLabelValuesRequest(
	minT, maxT int64,
	name string,
	blockIDs []ulid.ULID,
	params *streaminglabelvalues.Params,
	hints *storage.SearchHints,
	wireMatchers []storepb.LabelMatcher,
) *storepb.SearchLabelValuesRequest {
	req := &storepb.SearchLabelValuesRequest{
		Start:        minT,
		End:          maxT,
		Label:        name,
		Matchers:     wireMatchers,
		Filter:       paramsToSGProto(params),
		Ordering:     orderingToSGProto(hints),
		RequestHints: &storepb.SearchLabelValuesRequestHints{BlockMatchers: blockIDsToBlockMatchers(blockIDs)},
	}
	if hints != nil {
		req.Limit = int64(hints.Limit)
	}
	return req
}

// blockIDsToBlockMatchers builds the per-RPC block_matchers hint. Returns nil
// for an empty slice so the SG's BlockMatchers branch is not taken when no
// scoping is requested (matches LabelNames/LabelValues semantics).
//
// regexp.QuoteMeta is a no-op for ULIDs today (they're Crockford base32 — no
// regex metacharacters), but we escape defensively so future ID-format changes
// don't silently turn into a broken matcher.
func blockIDsToBlockMatchers(blockIDs []ulid.ULID) []storepb.LabelMatcher {
	if len(blockIDs) == 0 {
		return nil
	}
	ids := convertULIDsToString(blockIDs)
	for i, id := range ids {
		ids[i] = regexp.QuoteMeta(id)
	}
	return []storepb.LabelMatcher{
		{
			Type:  storepb.LabelMatcher_RE,
			Name:  block.BlockIDLabel,
			Value: strings.Join(ids, "|"),
		},
	}
}

func paramsToSGProto(p *streaminglabelvalues.Params) *storepb.SearchFilter {
	if p == nil || len(p.Terms) == 0 {
		return nil
	}
	wf := &storepb.SearchFilter{
		Terms:           p.Terms,
		CaseInsensitive: !p.CaseSensitive,
		FuzzThreshold:   int32(p.FuzzThreshold),
	}
	switch p.FuzzAlg {
	case streaminglabelvalues.FuzzAlgJaroWinkler:
		wf.FuzzAlg = storepb.FUZZ_ALG_JARO_WINKLER
	default:
		wf.FuzzAlg = storepb.FUZZ_ALG_SUBSEQUENCE
	}
	return wf
}

// orderingToSGProto defaults nil hints to ORDER_BY_VALUE_ASC, matching
// NewMergingSearchResultSet at the merge side.
func orderingToSGProto(hints *storage.SearchHints) storepb.SearchOrdering {
	if hints == nil {
		return storepb.ORDER_BY_VALUE_ASC
	}
	switch hints.OrderBy {
	case storage.OrderByValueDesc:
		return storepb.ORDER_BY_VALUE_DESC
	case storage.OrderByScoreDesc:
		return storepb.ORDER_BY_SCORE_DESC
	default:
		return storepb.ORDER_BY_VALUE_ASC
	}
}

// sgSearchStream is the Recv surface shared by both SG search streams.
type sgSearchStream interface {
	Recv() (*storepb.SearchResultBatch, error)
}
