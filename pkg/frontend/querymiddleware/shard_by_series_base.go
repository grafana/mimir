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
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"

	apierror "github.com/grafana/mimir/pkg/api/error"
	querierapi "github.com/grafana/mimir/pkg/querier/api"
	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/storage/sharding"
	"github.com/grafana/mimir/pkg/streamingpromql/requestoptions"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/promqlext"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

type shardBySeriesBase struct {
	upstream http.RoundTripper
	limits   Limits
	logger   log.Logger

	// maxConcurrency bounds how many shards are in flight concurrently. 0 means
	// no limit.
	maxConcurrency int

	// requestFramedResponses makes the sharded sub-requests advertise, via the
	// Accept header, that they can consume the length-delimited response format.
	requestFramedResponses bool
}

func (s *shardBySeriesBase) shardBySeriesSelector(ctx context.Context, spanLog *spanlogger.SpanLogger, r *http.Request, mergeFn func(ctx context.Context, reqs []*http.Request, encoding string) (*http.Response, error)) (*http.Response, error) {
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

	maxShards := s.limits.QueryShardingMaxShardedQueries(tenantID)
	if cardinalityMaxShards := s.limits.CardinalityShardingMaxShardedQueries(tenantID); cardinalityMaxShards > 0 {
		maxShards = cardinalityMaxShards
	}
	if maxShards > 0 && shardCount > maxShards {
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

	reqs, err := buildShardedRequests(ctx, r, shardCount, selector, s.requestFramedResponses)
	if err != nil {
		return nil, apierror.New(apierror.TypeInternal, err.Error())
	}

	acceptEncoding := r.Header.Get("Accept-Encoding")

	resp, err := mergeFn(ctx, reqs, acceptEncoding)
	if err != nil {
		if errors.Is(err, errShardCountTooLow) {
			return nil, apierror.New(apierror.TypeTooLargeEntry, fmt.Errorf("%w: try increasing the requested shard count", err).Error())
		}
		return nil, apierror.New(apierror.TypeInternal, err.Error())
	}

	return resp, nil
}

func setShardCountFromHeader(origShardCount int, r *http.Request, spanLog *spanlogger.SpanLogger) int {
	for _, value := range r.Header.Values(requestoptions.TotalShardsControlHeader) {
		shards, err := strconv.ParseInt(value, 10, 32)
		if err != nil {
			continue
		}
		if shards >= 0 {
			spanLog.DebugLog("msg", fmt.Sprintf("using shard count from header %s: %d", requestoptions.TotalShardsControlHeader, shards))
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
	parsed, err := promqlext.NewPromQLParser().ParseExpr(valSelector)
	if err != nil {
		return nil, fmt.Errorf("invalid selector: %w", err)
	}
	selector, ok := parsed.(*parser.VectorSelector)
	if !ok {
		return nil, fmt.Errorf("invalid selector: %w", err)
	}

	return selector, nil
}

func buildShardedRequests(ctx context.Context, req *http.Request, numRequests int, selector parser.Expr, requestFramedResponses bool) ([]*http.Request, error) {
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

		if requestFramedResponses {
			r.Header.Set("Accept", querierapi.ContentTypeActiveSeriesFramed)
		}

		if err := user.InjectOrgIDIntoHTTPRequest(ctx, r); err != nil {
			return nil, err
		}

		reqs[i] = r
	}

	return reqs, nil
}

func (s *shardBySeriesBase) processShardedRequests(ctx context.Context, reqs []*http.Request, handle func(ctx context.Context, resp *http.Response) error) error {
	g, ctx := errgroup.WithContext(ctx)
	if s.maxConcurrency > 0 {
		g.SetLimit(s.maxConcurrency)
	}
	queryStats := stats.FromContext(ctx)
	for _, req := range reqs {
		r := req
		g.Go(func() error {
			partialStats, childCtx := stats.ContextWithEmptyStats(ctx)
			partialStats.AddShardedQueries(1)

			var span trace.Span
			childCtx, span = tracer.Start(childCtx, "shardBySeries.doShardedRequest")
			defer span.End()

			resp, err := s.upstream.RoundTrip(r.WithContext(childCtx))
			if err != nil {
				span.RecordError(err)
				return err
			}

			if resp.StatusCode != http.StatusOK {
				span.SetAttributes(attribute.Int("statusCode", resp.StatusCode))
				defer func() {
					_, _ = io.Copy(io.Discard, resp.Body)
					_ = resp.Body.Close()
				}()
				if resp.StatusCode == http.StatusRequestEntityTooLarge {
					return errShardCountTooLow
				}
				body, _ := io.ReadAll(resp.Body)
				return fmt.Errorf("received unexpected response from upstream: status %d, body: %s", resp.StatusCode, string(body))
			}

			if err := handle(childCtx, resp); err != nil {
				return mergeShardResponseError{err}
			}

			span.SetAttributes(attribute.Int64("seriesCount", int64(partialStats.LoadFetchedSeries())))
			queryStats.Merge(partialStats)

			return nil
		})
	}

	return g.Wait()
}

type mergeShardResponseError struct{ err error }

func (e mergeShardResponseError) Error() string { return e.err.Error() }
func (e mergeShardResponseError) Unwrap() error { return e.err }

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
