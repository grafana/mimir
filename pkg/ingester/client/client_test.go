// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ingester/client/client_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package client

import (
	"bytes"
	"context"
	"net/http/httptest"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/grafana/dskit/flagext"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/logging"
	"github.com/weaveworks/common/server"

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
	serverCfg.ExcludeRequestInLog = true
	serverCfg.GPRCServerMaxConcurrentStreams = 10000
	serverCfg.Log = logging.NewGoKit(serverCfg.LogLevel)
	serverCfg.Registerer = prometheus.NewRegistry()
	server, err := server.New(serverCfg)
	require.NoError(b, err)
	b.Cleanup(server.Shutdown)
	go func() {
		_ = server.Run()
	}()
	b.Cleanup(server.Stop)

	RegisterIngesterServer(server.GRPC, &UnimplementedIngesterServer{})

	var maxRss int
	reportingWG := &sync.WaitGroup{}
	reportingWG.Add(1)
	stopReporting := make(chan struct{})
	go func() {
		// Record the max RSS of the benchmark process using ps.
		defer reportingWG.Done()
		t := time.NewTicker(time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-stopReporting:
				return
			case <-t.C:
				out, err := exec.Command("ps", "-o", "rss", "-x", strconv.Itoa(os.Getpid())).CombinedOutput()
				if err != nil {
					panic(err.Error() + string(out))
				}
				rss, err := strconv.Atoi(strings.TrimSpace(strings.Split(string(out), "\n")[1]))
				if err != nil {
					panic(err)
				}
				maxRss = util_math.Max(rss, maxRss)
			}
		}
	}()
	b.ResetTimer()
	generatorOutput := &bytes.Buffer{}
	for j := 0; j < b.N; j++ {
		// Send requests in a separate process, so we can record the RSS of this process.
		sendRequestsCmd := exec.Command("go", "run", "testdata/main.go", server.GRPCListenAddr().String(), strconv.Itoa(100_000))
		sendRequestsCmd.Stdout = generatorOutput
		sendRequestsCmd.Stderr = generatorOutput
		require.NoError(b, sendRequestsCmd.Start(), generatorOutput.String())
		require.NoError(b, sendRequestsCmd.Wait(), generatorOutput.String())
	}
	close(stopReporting)
	reportingWG.Wait()
	b.ReportMetric(float64(maxRss)*1024, "max-rss-bytes")
}
