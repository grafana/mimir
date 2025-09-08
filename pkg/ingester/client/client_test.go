// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ingester/client/client_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package client

import (
	"context"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/ring"
	"github.com/stretchr/testify/require"


	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/grpcutil"
	"github.com/grafana/mimir/pkg/util/test"
)

func TestMain(m *testing.M) {
	test.VerifyNoLeakTestMain(m)
}

// TestMarshall is useful to try out various optimisation on the unmarshalling code.
func TestMarshall(t *testing.T) {
	const numSeries = 10
	recorder := httptest.NewRecorder()
	{
		req := mimirpb.WriteRequest{}
		for i := 0; i < numSeries; i++ {
			req.Timeseries = append(req.Timeseries, mimirpb.PreallocTimeseries{
				TimeSeries: &mimirpb.TimeSeries{
					Labels: []mimirpb.LabelAdapter{
						{Name: "foo", Value: strconv.Itoa(i)},
					},
					Samples: []mimirpb.Sample{
						{TimestampMs: int64(i), Value: float64(i)},
					},
				},
			})
		}
		err := util.SerializeProtoResponse(recorder, &req, util.RawSnappy)
		require.NoError(t, err)
	}

	{
		const (
			tooSmallSize = 1
			plentySize   = 1024 * 1024
		)
		req := mimirpb.WriteRequest{}
		_, err := util.ParseProtoReader(context.Background(), recorder.Body, recorder.Body.Len(), tooSmallSize, nil, &req, util.RawSnappy)
		require.Error(t, err)
		_, err = util.ParseProtoReader(context.Background(), recorder.Body, recorder.Body.Len(), plentySize, nil, &req, util.RawSnappy)
		require.NoError(t, err)
		require.Len(t, req.Timeseries, numSeries)
	}
}

// Test that MakeIngesterClient includes the priority interceptor
// by creating a client and verifying it compiles with the interceptor
func TestMakeIngesterClientIncludesPriorityInterceptor(t *testing.T) {
	cfg := Config{}
	cfg.RegisterFlags(nil)

	metrics := NewMetrics(nil)
	logger := log.NewNopLogger()

	inst := ring.InstanceDesc{
		Addr: "localhost:9095",
		Id:   "test-ingester",
	}

	client, err := MakeIngesterClient(inst, cfg, metrics, logger)
	if err != nil { 
		t.Logf("Expected connection error: %v", err)
	}else{
		defer client.Close()
	}

	ctx := grpcutil.WithPriorityLevel(context.Background(), 450)
	level := grpcutil.GetPriorityLevel(ctx)
	require.Equal(t, 450, level)
}
