// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"

	"github.com/go-kit/log"
	jsoniter "github.com/json-iterator/go"
	"github.com/opentracing/opentracing-go"
	otlog "github.com/opentracing/opentracing-go/log"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"golang.org/x/sync/errgroup"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/distributor"
	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/storage/sharding"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

type shardActiveSeriesMiddleware struct {
	upstream http.RoundTripper
	logger   log.Logger
}

func newShardActiveSeriesMiddleware(upstream http.RoundTripper, logger log.Logger) http.RoundTripper {
	return &shardActiveSeriesMiddleware{
		upstream: upstream,
		logger:   logger,
	}
}

func (s *shardActiveSeriesMiddleware) RoundTrip(r *http.Request) (*http.Response, error) {
	const defaultNumShards = 1

	spanLog, ctx := spanlogger.NewWithLogger(r.Context(), s.logger, "shardActiveSeries.RoundTrip")
	defer spanLog.Finish()

	numShards := setShardCountFromHeader(defaultNumShards, r, spanLog)

	if numShards < 2 {
		spanLog.DebugLog("msg", "query sharding disabled for request")
		return s.upstream.RoundTrip(r)
	}

	values, err := util.ParseRequestFormWithoutConsumingBody(r)
	if err != nil {
		return nil, apierror.New(apierror.TypeBadData, err.Error())
	}

	valSelector := values.Get("selector")
	if valSelector == "" {
		return nil, apierror.New(apierror.TypeBadData, "selector parameter is required")
	}

	parsed, err := parser.ParseExpr(valSelector)
	if err != nil {
		return nil, apierror.New(apierror.TypeBadData, "invalid selector")
	}

	selector, ok := parsed.(*parser.VectorSelector)
	if !ok {
		return nil, apierror.New(apierror.TypeBadData, "invalid selector")
	}

	spanLog.DebugLog(
		"msg", "sharding active series query",
		"shardCount", numShards, "selector", selector.String(),
	)

	reqs, err := buildShardedRequests(ctx, r, numShards, selector)
	if err != nil {
		return nil, apierror.New(apierror.TypeInternal, err.Error())
	}

	resp, err := doShardedRequests(ctx, reqs, s.upstream)
	if err != nil {
		if errors.Is(err, distributor.ErrResponseTooLarge) {
			return nil, apierror.New(apierror.TypeBadData, err.Error())
		}
		return nil, apierror.New(apierror.TypeInternal, err.Error())
	}

	return s.mergeResponses(resp), nil
}

func setShardCountFromHeader(origShardCount int, r *http.Request, spanLog *spanlogger.SpanLogger) int {
	for _, value := range r.Header.Values(totalShardsControlHeader) {
		shards, err := strconv.ParseInt(value, 10, 32)
		if err != nil {
			continue
		}
		if shards > 0 {
			spanLog.DebugLog(
				"msg",
				fmt.Sprintf("using shard count from header %s: %d", totalShardsControlHeader, shards),
			)
			return int(shards)
		}
	}
	return origShardCount
}

func buildShardedRequests(ctx context.Context, req *http.Request, numRequests int, selector parser.Expr) ([]*http.Request, error) {
	reqs := make([]*http.Request, numRequests)
	for i := 0; i < numRequests; i++ {
		reqs[i] = req.Clone(ctx)

		sharded, err := shardedSelector(numRequests, i, selector)
		if err != nil {
			return nil, err
		}

		vals := url.Values{}
		vals.Set("selector", sharded.String())

		reqs[i].Header.Set("Content-Type", "application/x-www-form-urlencoded")
		reqs[i].Header.Del(totalShardsControlHeader)
		reqs[i].Body = io.NopCloser(strings.NewReader(vals.Encode()))
	}

	return reqs, nil
}

func doShardedRequests(ctx context.Context, upstreamRequests []*http.Request, next http.RoundTripper) ([]*http.Response, error) {
	mtx := sync.Mutex{}
	var resps []*http.Response

	g, ctx := errgroup.WithContext(ctx)
	queryStats := stats.FromContext(ctx)
	for _, req := range upstreamRequests {
		r := req
		g.Go(func() error {
			partialStats, childCtx := stats.ContextWithEmptyStats(ctx)
			partialStats.AddShardedQueries(1)

			var span opentracing.Span
			span, childCtx = opentracing.StartSpanFromContext(childCtx, "shardActiveSeries.doRequests")
			defer span.Finish()

			resp, err := next.RoundTrip(r.WithContext(childCtx))
			if err != nil {
				span.LogFields(otlog.Error(err))
				return err
			}

			queryStats.Merge(partialStats)

			mtx.Lock()
			resps = append(resps, resp)
			mtx.Unlock()

			return nil
		})
	}

	return resps, g.Wait()
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

func (s *shardActiveSeriesMiddleware) mergeResponses(responses []*http.Response) *http.Response {
	reader, writer := io.Pipe()

	json = jsoniter.ConfigCompatibleWithStandardLibrary
	items := make(chan any)

	g := new(errgroup.Group)
	for _, res := range responses {
		r := res
		g.Go(func() error {
			defer func(Body io.ReadCloser) {
				_ = Body.Close()
			}(r.Body)

			it := jsoniter.Parse(jsoniter.ConfigFastest, r.Body, 512)

			field := it.ReadObject()
			if field != "data" {
				err := errors.New("expected data field at top level")
				return err
			}

			if it.WhatIsNext() != jsoniter.ArrayValue {
				err := errors.New("expected data field to contain an array")
				return err
			}

			for it.ReadArray() {
				items <- it.Read()
			}

			return it.Error
		})
	}

	go s.writeMergedResponse(g, writer, items)

	return &http.Response{Body: reader, StatusCode: http.StatusOK}
}

func (s *shardActiveSeriesMiddleware) writeMergedResponse(g *errgroup.Group, w io.WriteCloser, series chan any) {
	defer func(w io.Closer) {
		_ = w.Close()
	}(w)

	stream := jsoniter.NewStream(jsoniter.ConfigFastest, w, 512)
	defer func(stream *jsoniter.Stream) {
		_ = stream.Flush()
	}(stream)

	stream.WriteObjectStart()
	stream.WriteObjectField("data")

	stream.WriteArrayStart()

	doneWriting := make(chan struct{})
	go func() {
		firstItem := true
		for {
			item, ok := <-series
			if !ok {
				doneWriting <- struct{}{}
				return
			}
			if firstItem {
				firstItem = false
			} else {
				stream.WriteMore()
			}
			stream.WriteVal(item)
		}
	}()

	err := g.Wait()
	close(series)
	<-doneWriting

	stream.WriteArrayEnd()

	if err != nil {
		stream.WriteMore()
		stream.WriteObjectField("status")
		stream.WriteString("error")
		stream.WriteMore()
		stream.WriteObjectField("error")
		stream.WriteString(err.Error())
	}

	stream.WriteObjectEnd()
}
