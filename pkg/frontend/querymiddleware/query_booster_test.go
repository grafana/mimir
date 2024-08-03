package querymiddleware

import (
	"fmt"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_queryBoosterMiddleware_queryBoostedMetric(t *testing.T) {
	q := queryBoostedMetric(`sum(rate(cortex_query_frontend_queries_total{container="query-frontend"}[1m]))`)
	fmt.Println(q)
	_, err := parser.ParseExpr(q)
	assert.NoError(t, err)
}
