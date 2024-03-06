// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"sync"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/tenant"
	"github.com/grafana/dskit/user"
	jsoniter "github.com/json-iterator/go"
	"github.com/klauspost/compress/s2"
	"github.com/opentracing/opentracing-go"
	otlog "github.com/opentracing/opentracing-go/log"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"golang.org/x/sync/errgroup"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/storage/sharding"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

const encodingTypeSnappyFramed = "x-snappy-framed"

var (
	errShardCountTooLow = errors.New("shard count too low")

	jsoniterMaxBufferSize = os.Getpagesize()
	jsoniterBufferPool    = sync.Pool{
		New: func() any {
			buf := make([]byte, jsoniterMaxBufferSize)
			return &buf
		},
	}

	labelBuilderPool = sync.Pool{
		New: func() any {
			return labels.NewBuilder(labels.EmptyLabels())
		},
	}
)

var snappyWriterPool sync.Pool

func getSnappyWriter(w io.Writer) *s2.Writer {
	sw := snappyWriterPool.Get()
	if sw == nil {
		return s2.NewWriter(w)
	}
	enc := sw.(*s2.Writer)
	enc.Reset(w)
	return enc
}

type shardActiveSeriesMiddleware struct {
	upstream http.RoundTripper
	limits   Limits
	logger   log.Logger
}

func newShardActiveSeriesMiddleware(upstream http.RoundTripper, limits Limits, logger log.Logger) http.RoundTripper {
	return &shardActiveSeriesMiddleware{
		upstream: upstream,
		limits:   limits,
		logger:   logger,
	}
}

func (s *shardActiveSeriesMiddleware) RoundTrip(r *http.Request) (*http.Response, error) {
	spanLog, ctx := spanlogger.NewWithLogger(r.Context(), s.logger, "shardActiveSeries.RoundTrip")
	defer spanLog.Finish()

	tenantID, err := tenant.TenantID(ctx)
	if err != nil {
		return nil, apierror.New(apierror.TypeBadData, err.Error())
	}

	defaultShardCount := s.limits.QueryShardingTotalShards(tenantID)
	shardCount := setShardCountFromHeader(defaultShardCount, r, spanLog)

	if shardCount < 2 {
		spanLog.DebugLog("msg", "query sharding disabled for request")
		return s.upstream.RoundTrip(r)
	}

	if maxShards := s.limits.QueryShardingMaxShardedQueries(tenantID); shardCount > maxShards {
		return nil, apierror.New(
			apierror.TypeBadData,
			fmt.Sprintf("shard count %d exceeds allowed maximum (%d)", shardCount, maxShards),
		)
	}

	selector, err := parseSelector(r)
	if err != nil {
		return nil, apierror.New(apierror.TypeBadData, err.Error())
	}

	spanLog.DebugLog(
		"msg", "sharding active series query",
		"shardCount", shardCount, "selector", selector.String(),
	)

	reqs, err := buildShardedRequests(ctx, r, shardCount, selector)
	if err != nil {
		return nil, apierror.New(apierror.TypeInternal, err.Error())
	}

	resp, err := doShardedRequests(ctx, reqs, s.upstream)
	if err != nil {
		if errors.Is(err, errShardCountTooLow) {
			return nil, apierror.New(apierror.TypeBadData, fmt.Errorf("%w: try increasing the requested shard count", err).Error())
		}
		return nil, apierror.New(apierror.TypeInternal, err.Error())
	}

	return s.mergeResponses(ctx, resp, r.Header.Get("Accept-Encoding")), nil
}

func setShardCountFromHeader(origShardCount int, r *http.Request, spanLog *spanlogger.SpanLogger) int {
	for _, value := range r.Header.Values(totalShardsControlHeader) {
		shards, err := strconv.ParseInt(value, 10, 32)
		if err != nil {
			continue
		}
		if shards >= 0 {
			spanLog.DebugLog("msg", fmt.Sprintf("using shard count from header %s: %d", totalShardsControlHeader, shards))
			return int(shards)
		}
	}
	return origShardCount
}

func parseSelector(req *http.Request) (*parser.VectorSelector, error) {
	values, err := util.ParseRequestFormWithoutConsumingBody(req)
	if err != nil {
		return nil, err
	}
	valSelector := values.Get("selector")
	if valSelector == "" {
		return nil, errors.New("selector parameter is required")
	}
	parsed, err := parser.ParseExpr(valSelector)
	if err != nil {
		return nil, fmt.Errorf("invalid selector: %w", err)
	}
	selector, ok := parsed.(*parser.VectorSelector)
	if !ok {
		return nil, fmt.Errorf("invalid selector: %w", err)
	}

	return selector, nil
}

func buildShardedRequests(ctx context.Context, req *http.Request, numRequests int, selector parser.Expr) ([]*http.Request, error) {
	reqs := make([]*http.Request, numRequests)
	for i := 0; i < numRequests; i++ {
		r, err := http.NewRequestWithContext(ctx, http.MethodGet, req.URL.Path, http.NoBody)
		if err != nil {
			return nil, err
		}

		sharded, err := shardedSelector(numRequests, i, selector)
		if err != nil {
			return nil, err
		}

		vals := url.Values{}
		vals.Set("selector", sharded.String())
		r.URL.RawQuery = vals.Encode()
		// This is the field read by httpgrpc.FromHTTPRequest, so we need to populate it
		// here to ensure the request parameter makes it to the querier.
		r.RequestURI = r.URL.String()

		if err := user.InjectOrgIDIntoHTTPRequest(ctx, r); err != nil {
			return nil, err
		}

		reqs[i] = r
	}

	return reqs, nil
}

func doShardedRequests(ctx context.Context, upstreamRequests []*http.Request, next http.RoundTripper) ([]*http.Response, error) {
	resps := make([]*http.Response, len(upstreamRequests))

	g, ctx := errgroup.WithContext(ctx)
	queryStats := stats.FromContext(ctx)
	for i, req := range upstreamRequests {
		i, r := i, req
		g.Go(func() error {
			partialStats, childCtx := stats.ContextWithEmptyStats(ctx)
			partialStats.AddShardedQueries(1)

			var span opentracing.Span
			span, childCtx = opentracing.StartSpanFromContext(childCtx, "shardActiveSeries.doShardedRequest")
			defer span.Finish()

			resp, err := next.RoundTrip(r.WithContext(childCtx))
			if err != nil {
				span.LogFields(otlog.Error(err))
				return err
			}

			if resp.StatusCode != http.StatusOK {
				span.LogFields(otlog.Int("statusCode", resp.StatusCode))
				if resp.StatusCode == http.StatusRequestEntityTooLarge {
					return errShardCountTooLow
				}
				var body []byte
				if resp.Body != nil {
					body, _ = io.ReadAll(resp.Body)
				}
				return fmt.Errorf("received unexpected response from upstream: status %d, body: %s", resp.StatusCode, string(body))
			}

			resps[i] = resp

			span.LogFields(otlog.Uint64("seriesCount", partialStats.LoadFetchedSeries()))
			queryStats.Merge(partialStats)

			return nil
		})
	}

	err := g.Wait()
	if err != nil {
		// If there was an error, we need to read and close all response bodies.
		for _, resp := range resps {
			if resp != nil {
				_, _ = io.ReadAll(resp.Body)
				_ = resp.Body.Close()
			}
		}
	}

	return resps, err
}

func shardedSelector(shardCount, currentShard int, expr parser.Expr) (parser.Expr, error) {
	originalSelector, ok := expr.(*parser.VectorSelector)
	if !ok {
		return nil, errors.New("invalid selector")
	}

	shardMatcher, err := labels.NewMatcher(
		labels.MatchEqual, sharding.ShardLabel,
		sharding.ShardSelector{ShardIndex: uint64(currentShard), ShardCount: uint64(shardCount)}.LabelValue(),
	)
	if err != nil {
		return nil, err
	}

	return &parser.VectorSelector{
		Name:          originalSelector.Name,
		LabelMatchers: append([]*labels.Matcher{shardMatcher}, originalSelector.LabelMatchers...),
	}, nil
}

func (s *shardActiveSeriesMiddleware) mergeResponses(ctx context.Context, responses []*http.Response, encoding string) *http.Response {
	reader, writer := io.Pipe()

	items := make(chan *labels.Builder, len(responses))

	g := new(errgroup.Group)
	for _, res := range responses {
		if res == nil {
			continue
		}
		r := res
		g.Go(func() error {
			defer func(body io.ReadCloser) {
				// drain body reader
				_, _ = io.Copy(io.Discard, body)
				_ = body.Close()
			}(r.Body)

			bufPtr := jsoniterBufferPool.Get().(*[]byte)
			defer jsoniterBufferPool.Put(bufPtr)

			it := jsoniter.ConfigFastest.BorrowIterator(*bufPtr)
			it.Reset(r.Body)
			defer func() {
				jsoniter.ConfigFastest.ReturnIterator(it)
			}()

			// Iterate over fields until we find data or error fields
			foundDataField := false
			for it.Error == nil {
				field := it.ReadObject()
				if field == "error" {
					return fmt.Errorf("error in partial response: %s", it.ReadString())
				}
				if field == "data" {
					foundDataField = true
					break
				}
				// If the field is neither data nor error, we skip it.
				it.ReadAny()
			}
			if !foundDataField {
				return fmt.Errorf("expected data field at top level, found %s", it.CurrentBuffer())
			}

			if it.WhatIsNext() != jsoniter.ArrayValue {
				err := errors.New("expected data field to contain an array")
				return err
			}

			for it.ReadArray() {
				if err := ctx.Err(); err != nil {
					if cause := context.Cause(ctx); cause != nil {
						return fmt.Errorf("aborted streaming because context was cancelled: %w", cause)
					}
					return ctx.Err()
				}

				item := labelBuilderPool.Get().(*labels.Builder)
				it.ReadMapCB(func(iterator *jsoniter.Iterator, s string) bool {
					item.Set(s, iterator.ReadString())
					return true
				})
				items <- item
			}

			return it.Error
		})
	}

	go func() {
		// We ignore the error from the errgroup because it will be checked again later.
		_ = g.Wait()
		close(items)
	}()

	resp := &http.Response{Body: reader, StatusCode: http.StatusOK, Header: http.Header{}}
	resp.Header.Set("Content-Type", "application/json")
	if encoding == encodingTypeSnappyFramed {
		resp.Header.Set("Content-Encoding", encodingTypeSnappyFramed)
	}

	go s.writeMergedResponse(ctx, g.Wait, writer, items, encoding)

	return resp
}

func (s *shardActiveSeriesMiddleware) writeMergedResponse(ctx context.Context, check func() error, w io.WriteCloser, items <-chan *labels.Builder, encoding string) {
	defer w.Close()

	span, _ := opentracing.StartSpanFromContext(ctx, "shardActiveSeries.writeMergedResponse")
	defer span.Finish()

	var out io.Writer = w
	if encoding == encodingTypeSnappyFramed {
		span.LogFields(otlog.String("encoding", encodingTypeSnappyFramed))
		enc := getSnappyWriter(w)
		out = enc
		defer func() {
			enc.Close()
			// Reset the encoder before putting it back to pool to avoid it to hold the writer.
			enc.Reset(nil)
			snappyWriterPool.Put(enc)
		}()
	} else {
		span.LogFields(otlog.String("encoding", "none"))
	}

	stream := jsoniter.ConfigFastest.BorrowStream(out)
	defer func(stream *jsoniter.Stream) {
		_ = stream.Flush()

		if cap(stream.Buffer()) > jsoniterMaxBufferSize {
			return
		}
		jsoniter.ConfigFastest.ReturnStream(stream)
	}(stream)

	stream.WriteObjectStart()
	stream.WriteObjectField("data")
	stream.WriteArrayStart()
	firstItem := true
	for item := range items {
		if firstItem {
			firstItem = false
		} else {
			stream.WriteMore()
		}
		stream.WriteObjectStart()
		firstField := true

		item.Range(func(l labels.Label) {
			if firstField {
				firstField = false
			} else {
				stream.WriteMore()
			}
			stream.WriteObjectField(l.Name)
			stream.WriteString(l.Value)
		})
		stream.WriteObjectEnd()

		item.Reset(labels.EmptyLabels())
		labelBuilderPool.Put(item)

		// Flush the stream buffer if it's getting too large.
		if stream.Buffered() > jsoniterMaxBufferSize {
			_ = stream.Flush()
		}
	}
	stream.WriteArrayEnd()

	if err := check(); err != nil {
		level.Error(s.logger).Log("msg", "error merging partial responses", "err", err.Error())
		span.LogFields(otlog.Error(err))
		stream.WriteMore()
		stream.WriteObjectField("status")
		stream.WriteString("error")
		stream.WriteMore()
		stream.WriteObjectField("error")
		stream.WriteString(fmt.Sprintf("error merging partial responses: %s", err.Error()))
	}

	stream.WriteObjectEnd()
}
