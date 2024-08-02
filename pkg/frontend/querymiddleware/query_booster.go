package querymiddleware

import (
	"context"
	"fmt"
	"net/http"
	"net/url"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/mimir/pkg/util/spanlogger"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/promql/parser"
)

const (
	queryBoosterMetric    = "QUERY_BOOSTER"
	boostedQueryLabelName = "__boosted_query__"
)

type queryBoosterMiddleware struct {
	downstream http.RoundTripper
	logger     log.Logger

	next MetricsQueryHandler
}

func newQueryBoosterMiddleware(downstream http.RoundTripper, logger log.Logger) MetricsQueryMiddleware {
	return MetricsQueryMiddlewareFunc(func(next MetricsQueryHandler) MetricsQueryHandler {
		return &queryBoosterMiddleware{
			downstream: downstream,
			logger:     logger,
			next:       next,
		}
	})
}

func (q *queryBoosterMiddleware) Do(ctx context.Context, req MetricsQueryRequest) (Response, error) {
	log, ctx := spanlogger.NewWithLogger(ctx, q.logger, "queryBoosterMiddleware.Do")
	defer log.Span.Finish()

	queryOriginal := req.GetQuery()
	expr, err := parser.ParseExpr(queryOriginal)
	if err != nil {
		level.Warn(log).Log("msg", "failed to parse query, running next middleware", "query", queryOriginal, "err", err)
		// This isn't a valid query, so it won't be served. Let the next middleware handle it.
		return q.next.Do(ctx, req)
	}

	queryCleaned := expr.String()
	isQueryBoosted, err := q.isQueryBoosted(ctx, queryCleaned, req.GetHeaders())
	if err != nil {
		level.Warn(log).Log("msg", "failed to check if query is boosted, running next middleware", "query", queryCleaned, "err", err)
		return q.next.Do(ctx, req)
	}

	if !isQueryBoosted {
		level.Info(log).Log("msg", "query is not boosted", "query", queryCleaned)
		return q.next.Do(ctx, req)
	}
	level.Info(log).Log("msg", "query is boosted", "query", queryCleaned)

	queryRewritten := q.queryBoostedMetric(queryCleaned)
	req, err = req.WithQuery(queryRewritten)
	if err != nil {
		return nil, errors.Wrap(err, "failed to rewrite boosted query")
	}
	level.Debug(log).Log("msg", "rewrote boosted query", "original", queryOriginal, "rewritten", queryRewritten)

	return q.next.Do(ctx, req)
}

func (q *queryBoosterMiddleware) isQueryBoosted(ctx context.Context, query string, headers []*PrometheusHeader) (bool, error) {
	log, _ := spanlogger.NewWithLogger(ctx, q.logger, "queryBoosterMiddleware.isQueryBoosted")
	defer log.Span.Finish()

	boostedQueryResultsMetricMatcher := make(url.Values)
	boostedQueryResultsMetricMatcher.Add("match[]", "__name__=\""+queryBoosterMetric+"\"")
	boostedQueryResultsMetricMatcher.Add("match[]", boostedQueryLabelName+"=\""+query+"\"")

	req := &http.Request{
		Method: http.MethodGet,
		URL:    &url.URL{Path: "/api/v1/label/" + boostedQueryLabelName + "/values"},
		Form:   boostedQueryResultsMetricMatcher,
	}

	for _, header := range headers {
		for _, value := range header.Values {
			req.Header.Add(header.Name, value)
		}
	}

	respEncoded, err := q.downstream.RoundTrip(req)
	if err != nil {
		return false, err
	}

	var respDecoded IsQueryBoostedResponse
	if err := json.NewDecoder(respEncoded.Body).Decode(&respDecoded); err != nil {
		return false, errors.Wrap(err, "failed to decode isQueryBoosted response")
	}

	if respDecoded.Status != "success" {
		return false, errors.New("no success when querying whether isQueryBoosted")
	}

	if len(respDecoded.Data) > 0 && respDecoded.Data[0] == query {
		return true, nil
	}

	return false, nil
}

type IsQueryBoostedResponse struct {
	Status    string   `json:"status"`
	Data      []string `json:"data,omitempty"`
	ErrorType string   `json:"errorType,omitempty"`
	Error     string   `json:"error,omitempty"`
	Warnings  []string `json:"warnings,omitempty"`
	Infos     []string `json:"infos,omitempty"`
}

func (q *queryBoosterMiddleware) queryBoostedMetric(query string) string {
	return fmt.Sprintf(boostedQueryInternalQuery, queryBoosterMetric, boostedQueryLabelName, query, boostedQueryLabelName)
}

const boostedQueryInternalQuery = `
label_replace(
  label_replace(
    %s{%s="%s"},
    "__name__",
    "",
    "",
    "",
  ),
  "%s",
  "",
  "",
  "",
)
`
