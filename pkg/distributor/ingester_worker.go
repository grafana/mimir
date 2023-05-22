package distributor

import (
	"context"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/common/user"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
)

type ingesterRequest struct {
	user string
	req  *mimirpb.WriteRequest
	ch   chan ingesterResponse
}

type ingesterResponse struct {
	resp *httpgrpc.HTTPResponse
	err  error
}

type ingesterWorker struct {
	client  client.IngesterClient
	ch      chan ingesterRequest
	logger  log.Logger
	metrics *ingesterWorkerMetrics
}

type ingesterWorkerMetrics struct {
	runningWorkers prometheus.Gauge
	activeWorkers  prometheus.Gauge
}

func newIngesterWorkerMetrics(r prometheus.Registerer) *ingesterWorkerMetrics {
	return &ingesterWorkerMetrics{
		runningWorkers: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "cortex_distributor_running_ingester_workers",
			Help: "Number of running ingester workers (for all ingesters)",
		}),
		activeWorkers: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "cortex_distributor_active_ingester_workers",
			Help: "Number of active (handling request) ingester workers",
		}),
	}
}

func newIngesterWorker(client client.IngesterClient, logger log.Logger, metrics *ingesterWorkerMetrics) *ingesterWorker {
	return &ingesterWorker{
		client:  client,
		ch:      make(chan ingesterRequest),
		logger:  logger,
		metrics: metrics,
	}
}

func (w *ingesterWorker) push(ctx context.Context, user string, m *mimirpb.WriteRequest) error {
	r := ingesterRequest{
		req:  m,
		user: user,
		ch:   make(chan ingesterResponse, 1),
	}

	sent := false

	select {
	case w.ch <- r:
		sent = true
	default:
	}

	if !sent {
		// spawn new worker and send request again, this time block until some worker accepts the request.
		go w.processJobs()

		select {
		case w.ch <- r:
			// ok
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// now wait for reply
	select {
	case out := <-r.ch:
		if out.resp != nil {
			if out.resp.Code/100 == 2 {
				return nil
			}

			return httpgrpc.ErrorFromHTTPResponse(out.resp)
		}
		return out.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (w *ingesterWorker) processJobs() {
	w.metrics.runningWorkers.Inc()
	defer w.metrics.runningWorkers.Dec()

	s, err := w.client.PushStream(user.InjectOrgID(context.Background(), "dummy"))
	if err != nil {
		level.Warn(w.logger).Log("msg", "failed to initiate PushStream", "err", err)
		// log error
		return
	}

	for {
		// FIXME: don't create new time for each iteration
		waitTimeout := time.NewTimer(5 * time.Second)

		select {
		case r, ok := <-w.ch:
			waitTimeout.Stop()

			if !ok {
				// channel closed, stop
				return
			}

			if w.processRequest(s, r) {
				return
			}

		case <-waitTimeout.C:
			// no new request in some time, exit
			_ = s.CloseSend()
			return
		}
	}
}

// returns whether worker should keep running and process more requests
func (w *ingesterWorker) processRequest(s client.Ingester_PushStreamClient, r ingesterRequest) bool {
	w.metrics.activeWorkers.Inc()
	defer w.metrics.activeWorkers.Dec()

	err := s.Send(&client.WriteRequestWithUser{
		Request: r.req,
		User:    r.user,
	})
	if err != nil {
		r.ch <- ingesterResponse{
			err: err,
		}
		return false
	}

	recv, err := s.Recv()
	r.ch <- ingesterResponse{
		resp: recv,
		err:  err,
	}

	if err != nil {
		return false
	}
	return true
}
