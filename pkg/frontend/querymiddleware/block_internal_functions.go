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
	expr, err := parser.ParseExpr(request.GetQuery())
	if err != nil {
		return nil, err
	}

	containsInternalFunction, err := astmapper.AnyNode(expr, b.isInternalFunctionCall)
	if err != nil {
		return nil, err
	}

	if containsInternalFunction {
		return nil, apierror.Newf(apierror.TypeBadData, "expression contains an internal function not permitted in queries")
	}

	return b.next.Do(ctx, request)
}

func (b *blockInternalFunctionsMiddleware) isInternalFunctionCall(node parser.Node) (bool, error) {
	call, isCall := node.(*parser.Call)
	if !isCall {
		return false, nil
	}

	return b.functionsToBlock.Contains(call.Func.Name), nil
}

type FunctionNamesSet map[string]struct{}

func (s FunctionNamesSet) Add(name string) {
	s[name] = struct{}{}
}

func (s FunctionNamesSet) Contains(name string) bool {
	_, ok := s[name]
	return ok
}
