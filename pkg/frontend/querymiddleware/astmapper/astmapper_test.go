// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/astmapper/astmapper_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package astmapper

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/stretchr/testify/require"
)

func TestCloneExpr_ExplicitTestCases(t *testing.T) {
	testCases := []parser.Expr{

		&parser.BinaryExpr{
			Op:  parser.ADD,
			LHS: &parser.NumberLiteral{Val: 1, PosRange: posrange.PositionRange{Start: 10, End: 11}},
			RHS: &parser.NumberLiteral{Val: 1, PosRange: posrange.PositionRange{Start: 14, End: 15}},
		},

		&parser.AggregateExpr{
			Op:      parser.SUM,
			Without: true,
			Expr: &parser.VectorSelector{
				Name: "some_metric",
				LabelMatchers: []*labels.Matcher{
					labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, "some_metric"),
				},
				PosRange: posrange.PositionRange{Start: 119, End: 130},
			},
			Grouping: []string{"foo"},
			PosRange: posrange.PositionRange{Start: 100, End: 131},
		},

		&parser.StepInvariantExpr{
			Expr: &parser.NumberLiteral{
				Val:      123,
				PosRange: posrange.PositionRange{Start: 10, End: 14},
			},
		},
	}

	for i, originalExpression := range testCases {
		t.Run(fmt.Sprintf("%d: %s", i, originalExpression.String()), func(t *testing.T) {
			clonedExpression, err := cloneExpr(originalExpression)
			require.NoError(t, err)
			require.Equal(t, originalExpression, clonedExpression)
			require.NotSame(t, clonedExpression, originalExpression, "cloneExpr should return a new expression")
			requireNoSharedPointers(t, originalExpression, clonedExpression)
		})
	}
}

func TestCloneExpr(t *testing.T) {
	oldDurationExpressions := parser.ExperimentalDurationExpr
	oldExperimentalFunctions := parser.EnableExperimentalFunctions
	parser.ExperimentalDurationExpr = true
	parser.EnableExperimentalFunctions = true
	t.Cleanup(func() {
		parser.ExperimentalDurationExpr = oldDurationExpressions
		parser.EnableExperimentalFunctions = oldExperimentalFunctions
	})

	testCases := []string{
		// Vector selectors
		`foo`,
		`foo{env="bar"}`,
		`foo{env="bar"} offset 3m`,
		`foo{env="bar"} offset (3m+5m)`,
		`foo{env="bar"} @ start()`,
		`foo{env="bar"} @ end()`,
		`foo{env="bar"} @ 1234`,

		// Matrix selector
		`foo[1m]`,
		`foo{env="bar"}[1m]`,
		`foo{env="bar"}[1m] offset 3m`,
		`foo{env="bar"}[1m] offset (3m+5m)`,
		`foo{env="bar"}[1m] @ start()`,
		`foo{env="bar"}[1m] @ end()`,
		`foo{env="bar"}[1m] @ 1234`,
		`foo{env="bar"}[1m+3m]`,

		// Literals
		`123`,
		`-123`,
		`"foo"`,

		// Unary expressions
		`-foo`,
		`+foo`,

		// Parentheses
		`(foo)`,
		`(((foo)))`,
		`(1)`,

		// Aggregations
		`sum(foo)`,
		`sum by (env, region) (foo)`,
		`sum without (cluster, pod) (foo)`,
		`topk(10, foo)`,

		// Functions
		`abs(foo)`,
		`label_replace(foo, "bar", "$1", "foo", "(.*)")`,
		`info(foo, {env="prod"})`, // Test VectorSelector.BypassEmptyMatcherCheck used by info().

		// Subqueries
		`foo[1m:10s]`,
		`foo[1m+2m:10s]`,
		`foo[1m:10s+3s]`,
		`foo[1m:10s] offset 3m`,
		`foo[1m:10s] offset (3m+5m)`,
		`foo[1m:10s] @ start()`,
		`foo[1m:10s] @ end()`,
		`foo[1m:10s] @ 1234`,

		// Binary expressions
		`1 + 2`,
		`foo + bar`,
		`foo + on (env, region) bar`,
		`foo + ignoring (cluster, pod) bar`,
		`foo + on (env, region) group_left(version) bar`,
		`foo + on (env, region) group_right(version) bar`,
		`foo and bar`,
		`foo == bar`,
		`foo == bool bar`,
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("%d: %s", i, tc), func(t *testing.T) {
			originalExpression, err := parser.ParseExpr(tc)
			require.NoError(t, err)
			clonedExpression, err := cloneExpr(originalExpression)
			require.NoError(t, err)
			require.Equal(t, originalExpression, clonedExpression)
			require.Equal(t, originalExpression.String(), clonedExpression.String())
			requireNoSharedPointers(t, originalExpression, clonedExpression)

		})
	}
}

func requireNoSharedPointers(t *testing.T, objA, objB any) {
	if isTypeSafeToShare(objA) {
		return
	}

	typA := reflect.TypeOf(objA)
	typB := reflect.TypeOf(objB)
	require.Equal(t, typA, typB, "types should be the same")

	valueA := reflect.ValueOf(objA)
	valueB := reflect.ValueOf(objB)

	switch typA.Kind() {
	case reflect.Bool:
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
	case reflect.Float32, reflect.Float64:
	case reflect.Complex64, reflect.Complex128:
	case reflect.String:
		// Value types with no nested values, nothing to check.
		return

	case reflect.Pointer:
		require.NotSame(t, objA, objB, "shared pointer detected")
		requireNoSharedPointers(t, valueA.Elem().Interface(), valueB.Elem().Interface())

	case reflect.Struct:
		for fieldIdx := range typA.NumField() {
			field := typA.Field(fieldIdx)
			fieldA := valueA.Field(fieldIdx)
			fieldB := valueB.Field(fieldIdx)

			switch field.Type.Kind() {
			case reflect.Interface:
			case reflect.Pointer:
				if fieldA.IsNil() && fieldB.IsNil() {
					continue
				}

				requireNoSharedPointers(t, fieldA.Interface(), fieldB.Interface())

			case reflect.Slice:
				if fieldA.IsNil() && fieldB.IsNil() {
					continue
				}

				require.NotEqualf(t, fieldA.Pointer(), fieldB.Pointer(), "shared slice detected for field %v", field.Name)
				require.Equal(t, fieldA.Len(), fieldB.Len(), "slice lengths should be the same for field %v", field.Name)

				for i := range fieldA.Len() {
					requireNoSharedPointers(t, fieldA.Index(i).Interface(), fieldB.Index(i).Interface())
				}

			default:
				requireNoSharedPointers(t, fieldA.Interface(), fieldB.Interface())
			}
		}

	default:
		require.Failf(t, "requireNoSharedPointers: don't know how to check value kind", "value kind %v", typA.Kind())
	}
}

func isTypeSafeToShare(o any) bool {
	switch o.(type) {
	case *parser.Function:
		// parser.Function stores information about a function like its name and return type.
		// It is safe to share this object between cloned parser.Call instances.
		return true
	default:
		return false
	}
}

func TestSharding_BinaryExpressionsDontTakeExponentialTime(t *testing.T) {
	const expressions = 30
	const timeout = 10 * time.Second

	query := `vector(1)`
	// On 11th Gen Intel(R) Core(TM) i7-11700K @ 3.60GHz:
	// This was taking 3s for 20 expressions, and doubled the time for each extra one.
	// So checking for 30 expressions would take an hour if processing time is exponential.
	for i := 2; i <= expressions; i++ {
		query += fmt.Sprintf("or vector(%d)", i)
	}
	expr, err := parser.ParseExpr(query)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	shardCount := 2
	summer := NewQueryShardSummer(shardCount, false, EmbeddedQueriesSquasher, log.NewNopLogger(), NewMapperStats())
	_, err = summer.Map(ctx, expr)
	require.NoError(t, err)
}

func TestASTMapperContextCancellation(t *testing.T) {
	// Create a simple ExprMapper that doesn't have context cancellation check
	testMapper := &testExprMapper{}
	astMapper := NewASTExprMapper(testMapper)

	// Create a cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Attempt to map with cancelled context
	expr, err := parser.ParseExpr("test{label=\"value\"}")
	require.NoError(t, err)

	// The Map function should detect the cancellation and return the error
	_, err = astMapper.Map(ctx, expr)
	require.Error(t, err)
	require.True(t, errors.Is(err, context.Canceled))

	// Verify the ExprMapper wasn't called
	require.Equal(t, 0, testMapper.called)
}

// testExprMapper is a simple ExprMapper that counts calls to MapExpr
type testExprMapper struct {
	called int
}

func (m *testExprMapper) MapExpr(_ context.Context, expr parser.Expr) (parser.Expr, bool, error) {
	m.called++
	return expr, false, nil
}
