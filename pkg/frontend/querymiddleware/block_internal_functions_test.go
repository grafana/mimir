// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/require"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/mimirpb"
)

func TestBlockInternalFunctionsMiddleware(t *testing.T) {
	blockedFunctions := FunctionNamesSet{}
	blockedFunctions.Add("sin")

	innerResponse := &PrometheusResponse{
		Status: statusSuccess,
		Data: &PrometheusData{
			ResultType: model.ValVector.String(),
			Result: []SampleStream{
				{
					Labels: mimirpb.FromLabelsToLabelAdapters(labels.FromStrings("source", "inner_response")),
				},
			},
		},
	}

	inner := mockHandlerWith(innerResponse, nil)
	middleware := newBlockInternalFunctionsMiddleware(blockedFunctions, log.NewNopLogger())
	handler := middleware.Wrap(inner)
	ctx := context.Background()

	req := createTestRequest(t, "abs(foo)")
	resp, err := handler.Do(ctx, req)
	require.NoError(t, err)
	require.Equal(t, resp, innerResponse)

	req = createTestRequest(t, "sin(foo)")
	resp, err = handler.Do(ctx, req)
	require.Equal(t, apierror.New(apierror.TypeBadData, "expression contains internal function 'sin' not permitted in queries"), err)
	require.Nil(t, resp)
}

func createTestRequest(t *testing.T, qs string) *PrometheusInstantQueryRequest {
	expr, err := parser.ParseExpr(qs)
	require.NoError(t, err)

	return NewPrometheusInstantQueryRequest("/", nil, timestamp.FromTime(time.Now()), 5*time.Minute, expr, Options{}, nil, "")
}
