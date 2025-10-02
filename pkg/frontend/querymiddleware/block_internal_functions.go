// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/promql/parser"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/frontend/querymiddleware/astmapper"
)

var InternalFunctionNames = map[string]struct{}{
	// Internal functions are registered by the packages that implement them.
}

type blockInternalFunctionsMiddleware struct {
	next   MetricsQueryHandler
	logger log.Logger
}

func newBlockInternalFunctionsMiddleware(logger log.Logger) MetricsQueryMiddleware {
	return MetricsQueryMiddlewareFunc(func(next MetricsQueryHandler) MetricsQueryHandler {
		return &blockInternalFunctionsMiddleware{
			next:   next,
			logger: logger,
		}
	})
}

func (b *blockInternalFunctionsMiddleware) Do(ctx context.Context, request MetricsQueryRequest) (Response, error) {
	expr, err := parser.ParseExpr(request.GetQuery())
	if err != nil {
		return nil, err
	}

	containsInternalFunction, err := astmapper.AnyNode(expr, isInternalFunctionCall)

	if containsInternalFunction {
		return nil, apierror.Newf(apierror.TypeBadData, "expression contains an internal function not permitted in queries")
	}

	return b.next.Do(ctx, request)
}

func isInternalFunctionCall(node parser.Node) (bool, error) {
	call, isCall := node.(*parser.Call)
	if !isCall {
		return false, nil
	}

	_, internalFunction := InternalFunctionNames[call.Func.Name]

	return internalFunction, nil
}
