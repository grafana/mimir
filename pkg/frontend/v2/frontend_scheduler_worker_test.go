package v2

import (
	"context"
	"net"
	"runtime"
	"testing"
	"time"

	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/stretchr/testify/require"
	"github.com/weaveworks/common/logging"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	"github.com/grafana/mimir/pkg/frontend/v2/frontendv2pb"
	"github.com/grafana/mimir/pkg/scheduler"
	"github.com/grafana/mimir/pkg/scheduler/schedulerpb"
	util_log "github.com/grafana/mimir/pkg/util/log"
)

func TestSchedulerWorker_ConnectionInterrupts(t *testing.T) {
	const (
		maxConnectionAge      = 1000 * time.Millisecond
		maxConnectionAgeGrace = time.Nanosecond
		numWorkers            = 1
		requestFrequency      = 300 * time.Millisecond

		testDuration = time.Minute
	)

	scheduler, _, requests := setup(t, maxConnectionAge, maxConnectionAgeGrace, numWorkers)
	enqueueResults := make(chan enqueueResult, 3)
	queryResults := make(chan *frontendv2pb.QueryResultRequest, 3)

	go func() {
		r := &frontendRequest{
			queryID:  1,
			userID:   "tenant",
			enqueue:  enqueueResults,
			response: queryResults,
		}
		for {
			requests <- r
			time.Sleep(requestFrequency)
		}
	}()

	go func() {
		for range enqueueResults {
		}
	}()
	go func() {
		for range queryResults {
		}
	}()

	require.NoError(t, scheduler.AwaitRunning(context.Background()))

	time.Sleep(testDuration)
	t.Log(runtime.NumGoroutine())
}

func setup(t *testing.T, maxConnectionAge, maxConnectionAgeGrace time.Duration, numWorkers int) (*scheduler.Scheduler, *frontendSchedulerWorker, chan *frontendRequest) {
	cfg := scheduler.Config{}
	flagext.DefaultValues(&cfg)
	cfg.MaxOutstandingPerTenant = 100000

	format := logging.Format{}
	require.NoError(t, format.Set("logfmt"))
	level := logging.Level{}
	require.NoError(t, level.Set("debug"))
	logger := util_log.NewDefaultLogger(level, format)
	s, err := scheduler.NewScheduler(cfg, &limits{queriers: 2}, logger, prometheus.DefaultRegisterer)
	require.NoError(t, err)

	server := grpc.NewServer(grpc.KeepaliveParams(keepalive.ServerParameters{
		MaxConnectionIdle:     time.Hour,
		MaxConnectionAge:      maxConnectionAge,
		MaxConnectionAgeGrace: maxConnectionAgeGrace,
	}),
	)
	schedulerpb.RegisterSchedulerForFrontendServer(server, s)
	schedulerpb.RegisterSchedulerForQuerierServer(server, s)
	frontendv2pb.RegisterFrontendForQuerierServer(server, &frontendv2pb.UnimplementedFrontendForQuerierServer{})

	require.NoError(t, s.StartAsync(context.Background()))
	t.Cleanup(func() {
		_ = services.StopAndAwaitTerminated(context.Background(), s)
	})

	l, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)

	go func() {
		_ = server.Serve(l)
	}()

	t.Cleanup(func() {
		_ = l.Close()
	})

	c, err := grpc.Dial(l.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithKeepaliveParams(keepalive.ClientParameters{
		Time:                time.Second * 10,
		Timeout:             time.Second,
		PermitWithoutStream: true,
	}))
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = c.Close()
	})

	requests := make(chan *frontendRequest)
	worker := newFrontendSchedulerWorker(c, l.Addr().String(), l.Addr().String(), requests, numWorkers, promauto.NewCounter(prometheus.CounterOpts{Name: "name"}), logger)
	worker.start()
	t.Cleanup(worker.stop)

	return s, worker, requests
}

type limits struct {
	queriers int
}

func (l limits) MaxQueriersPerUser(string) int {
	return l.queriers
}
