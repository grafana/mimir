package querymiddleware

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/user"
	"github.com/grafana/mimir/pkg/util/spanlogger"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/promql/parser"
)

const (
	queryBoosterMetric    = "QUERY_BOOSTER"
	boostedQueryLabelName = "__boosted_query__"
)

type queryBoosterMiddleware struct {
	logger     log.Logger
	qfeAddress string

	next MetricsQueryHandler
}

func newQueryBoosterMiddleware(cfg Config, logger log.Logger) MetricsQueryMiddleware {
	return MetricsQueryMiddlewareFunc(func(next MetricsQueryHandler) MetricsQueryHandler {
		return &queryBoosterMiddleware{
			logger:     logger,
			next:       next,
			qfeAddress: fmt.Sprintf("%s:%d", "localhost", cfg.QueryFrontendHttpListenPort),
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
	isQueryBoosted, err := q.isQueryBoosted(ctx, queryCleaned)
	if err != nil {
		level.Warn(log).Log("msg", "failed to check if query is boosted, running next middleware", "query", queryCleaned, "err", err)
		return q.next.Do(ctx, req)
	}

	if !isQueryBoosted {
		level.Info(log).Log("msg", "query is not boosted", "query", queryCleaned)
		return q.next.Do(ctx, req)
	}
	level.Info(log).Log("msg", "query is boosted", "query", queryCleaned)

	queryRewritten := queryBoostedMetric(queryCleaned)
	req, err = req.WithQuery(queryRewritten)
	if err != nil {
		return nil, errors.Wrap(err, "failed to rewrite boosted query")
	}
	level.Debug(log).Log("msg", "rewrote boosted query", "original", queryOriginal, "rewritten", queryRewritten)

	return q.next.Do(ctx, req)
}

func (q *queryBoosterMiddleware) isQueryBoosted(ctx context.Context, query string) (bool, error) {
	log, _ := spanlogger.NewWithLogger(ctx, q.logger, "queryBoosterMiddleware.isQueryBoosted")
	defer log.Span.Finish()

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		fmt.Sprintf("http://%s/prometheus/api/v1/labels", q.qfeAddress),
		http.NoBody,
	)
	if err != nil {
		return false, errors.Wrap(err, "failed to create isQueryBoosted request")
	}

	params := req.URL.Query()
	params.Add("match[]", fmt.Sprintf(`{__name__="%s", %s="%s"}`, queryBoosterMetric, boostedQueryLabelName, strings.ReplaceAll(query, `"`, `\"`)))
	req.URL.RawQuery = params.Encode()

	if err := user.InjectOrgIDIntoHTTPRequest(ctx, req); err != nil {
		return false, errors.Wrap(err, "failed to inject org ID into isQueryBoosted request")
	}

	respEncoded, err := http.DefaultClient.Do(req)
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

	// The matchers in the labels request already target the boosted series, so if we
	// get a non-empty response we know the metric has a booster recording rule.
	if len(respDecoded.Data) > 0 {
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

func queryBoostedMetric(query string) string {
	return fmt.Sprintf(boostedQueryInternalQuery, queryBoosterMetric, boostedQueryLabelName, strings.ReplaceAll(query, `"`, `\"`), boostedQueryLabelName)
}

const boostedQueryInternalQuery = `
label_replace(
  label_replace(
    %s{%s="%s"},
    "__name__",
    "",
    "",
    ""
  ),
  "%s",
  "",
  "",
  ""
)
`
