package ruler

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_ruleGroupForQuery(t *testing.T) {
	query := `sum( rate(metric{job="a"} [1m])) `

	rg, err := ruleGroupForQuery(BoostQueryRequest{query})
	require.NoError(t, err)

	assert.Equal(t, 1, len(rg.Rules))
	assert.Equal(t, "QUERY_BOOSTER", rg.Rules[0].Record.Value)

	expectExpr := `label_replace(sum(rate(metric{job="a"}[1m])), "__boosted_query__", "sum(rate(metric{job=\"a\"}[1m]))", "", "")`
	assert.Equal(t, expectExpr, rg.Rules[0].Expr.Value)
}
