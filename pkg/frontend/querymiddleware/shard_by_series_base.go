// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/tenant"
	"github.com/grafana/dskit/user"
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

type shardBySeriesBase struct {
	upstream http.RoundTripper
	limits   Limits
	logger   log.Logger
}

func (s *shardBySeriesBase) shardBySeriesSelector(ctx context.Context, spanLog *spanlogger.SpanLogger, r *http.Request, mergeFn func(ctx context.Context, reponses []*http.Response, encoding string) *http.Response) (*http.Response, error) {
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
			return nil, apierror.New(apierror.TypeTooLargeEntry, fmt.Errorf("%w: try increasing the requested shard count", err).Error())
		}
		return nil, apierror.New(apierror.TypeInternal, err.Error())
	}
	acceptEncoding := r.Header.Get("Accept-Encoding")

	return mergeFn(ctx, resp, acceptEncoding), nil
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
			span, childCtx = opentracing.StartSpanFromContext(childCtx, "shardBySeries.doShardedRequest")
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
