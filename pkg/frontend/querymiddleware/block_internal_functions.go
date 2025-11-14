// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/promql/parser"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/frontend/querymiddleware/astmapper"
)

type blockInternalFunctionsMiddleware struct {
	next             MetricsQueryHandler
	functionsToBlock FunctionNamesSet
	logger           log.Logger
}

func newBlockInternalFunctionsMiddleware(functionsToBlock FunctionNamesSet, logger log.Logger) MetricsQueryMiddleware {
	return MetricsQueryMiddlewareFunc(func(next MetricsQueryHandler) MetricsQueryHandler {
		return &blockInternalFunctionsMiddleware{
			next:             next,
			functionsToBlock: functionsToBlock,
			logger:           logger,
		}
	})
}

func (b *blockInternalFunctionsMiddleware) Do(ctx context.Context, request MetricsQueryRequest) (Response, error) {
	expr, err := astmapper.CloneExpr(request.GetParsedQuery())
	if err != nil {
		return nil, err
	}

	forbiddenFunctionName := ""
	containsInternalFunction := astmapper.AnyNode(expr, func(node parser.Node) bool {
		if call, isCall := node.(*parser.Call); isCall {
			if b.functionsToBlock.Contains(call.Func.Name) {
				forbiddenFunctionName = call.Func.Name
				return true
			}
		}
		return false
	})

	if containsInternalFunction {
		return nil, apierror.Newf(apierror.TypeBadData, "expression contains internal function '%s' not permitted in queries", forbiddenFunctionName)
	}

	return b.next.Do(ctx, request)
}

type FunctionNamesSet map[string]struct{}

func (s FunctionNamesSet) Add(name string) {
	s[name] = struct{}{}
}

func (s FunctionNamesSet) Contains(name string) bool {
	_, ok := s[name]
	return ok
}
