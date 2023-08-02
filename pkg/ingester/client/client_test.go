// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ingester/client/client_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package client

import (
	"context"
	"errors"
	"net/http/httptest"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/grafana/dskit/flagext"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/server"
	"github.com/weaveworks/common/user"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util"
	util_math "github.com/grafana/mimir/pkg/util/math"
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
		require.Equal(t, numSeries, len(req.Timeseries))
	}
}

func BenchmarkIngesterClient_ConcurrentStreams(b *testing.B) {
	serverCfg := server.Config{}
	flagext.DefaultValues(&serverCfg)
	require.NoError(b, serverCfg.LogLevel.Set("error"))
	serverCfg.GPRCServerMaxConcurrentStreams = 100
	//serverCfg.GPRCServerMaxConcurrentStreams = 10000
	serverCfg.Registerer = prometheus.NewRegistry()
	server, err := server.New(serverCfg)
	require.NoError(b, err)
	b.Cleanup(server.Shutdown)
	go func() {
		_ = server.Run()
	}()
	b.Cleanup(server.Stop)

	ingServ := &IngesterServerMock{}
	RegisterIngesterServer(server.GRPC, ingServ)
	noopLock := &sync.Mutex{}
	ingServ.On("Push", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		bytes := make([]byte, 1024)
		noopLock.Lock()
		noopLock.Unlock()
		bytes[4] = byte(time.Now().Second()) // use the slice to retain the memory during the Sleep
	}).Return(&mimirpb.WriteResponse{}, errors.New("overwhelmed"))

	client, err := MakeIngesterClient(server.GRPCListenAddr().String(), Config{}, NewMetrics(prometheus.NewRegistry()))
	require.NoError(b, err)
	b.Cleanup(func() { _ = client.Close() })

	wg := &sync.WaitGroup{}
	reportingWG := &sync.WaitGroup{}
	ctx, err := user.InjectIntoGRPCRequest(user.InjectOrgID(context.Background(), "test"))
	require.NoError(b, err)
	b.ResetTimer()

	for j := 0; j < b.N; j++ {
		var maxHeap uint64
		stopReporting := make(chan struct{})
		reportingWG.Add(1)
		go func() {
			memStats := &runtime.MemStats{}
			defer reportingWG.Done()
			t := time.NewTicker(time.Millisecond)
			defer t.Stop()
			for {
				select {
				case <-stopReporting:
					return
				case <-t.C:
					runtime.ReadMemStats(memStats)
					maxHeap = util_math.Max(memStats.HeapInuse, maxHeap)
				}
			}
		}()
		for i := 0; i < 100_000; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err := client.Push(ctx, &mimirpb.WriteRequest{})
				assert.ErrorContains(b, err, "overwhelmed")
			}()
		}
		wg.Wait()
		close(stopReporting)
		reportingWG.Wait()
		b.ReportMetric(float64(maxHeap), "max-heap")
	}
}
