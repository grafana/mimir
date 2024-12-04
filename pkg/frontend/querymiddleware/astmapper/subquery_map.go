package astmapper

import (
	"strings"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
)

const (
	// AggregatedSubqueryMetricName is a reserved metric name denoting a special metric which contains aggregated subqueries.
	// The actual subquery is stored in a label with the same key as the metric name.
	AggregatedSubqueryMetricName = "__aggregated_subquery__"
)

type SubqueryMapperStats struct {
	spunOffSubqueries int
}

func NewSubqueryMapperStats() *SubqueryMapperStats {
	return &SubqueryMapperStats{}
}

func (s *SubqueryMapperStats) AddSpunOffSubqueries(num int) {
	s.spunOffSubqueries += num
}

func (s *SubqueryMapperStats) GetSpunOffSubqueries() int {
	return s.spunOffSubqueries
}

type subqueryMapper struct {
	stats *SubqueryMapperStats
}

func NewSubqueryMapper(stats *SubqueryMapperStats) ASTMapper {
	return NewASTExprMapper(&subqueryMapper{stats: stats})
}

func (s *subqueryMapper) MapExpr(expr parser.Expr) (mapped parser.Expr, finished bool, err error) {
	switch e := expr.(type) {
	case *parser.Call:
		if strings.HasSuffix(e.Func.Name, "_over_time") && len(e.Args) == 1 {
			sq, ok := e.Args[0].(*parser.SubqueryExpr)
			if !ok {
				return expr, false, nil
			}
			selector := &parser.VectorSelector{
				Name: AggregatedSubqueryMetricName,
				LabelMatchers: []*labels.Matcher{
					labels.MustNewMatcher(labels.MatchEqual, AggregatedSubqueryMetricName, sq.String()),
				},
			}

			e.Args[0] = &parser.MatrixSelector{
				VectorSelector: selector,
				Range:          sq.Range,
			}

			s.stats.AddSpunOffSubqueries(1)
			return e, true, nil
		}
	}
	return expr, false, nil
}
