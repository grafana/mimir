package distributor

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	ring_client "github.com/grafana/dskit/ring/client"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
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
	client.HealthAndIngesterClient

	closed  atomic.Bool
	ch      chan ingesterRequest
	logger  log.Logger
	metrics *ingesterWorkerMetrics
}

type ingesterWorkerMetrics struct {
	runningWorkers   prometheus.Gauge
	activeWorkers    prometheus.Gauge
	requestsSent     prometheus.Counter
	responseReceived prometheus.Counter
}

func newIngesterWorkerMetrics(r prometheus.Registerer) *ingesterWorkerMetrics {
	return &ingesterWorkerMetrics{
		runningWorkers: promauto.With(r).NewGauge(prometheus.GaugeOpts{
			Name: "cortex_distributor_running_ingester_workers",
			Help: "Number of running ingester workers (for all ingesters)",
		}),
		activeWorkers: promauto.With(r).NewGauge(prometheus.GaugeOpts{
			Name: "cortex_distributor_active_ingester_workers",
			Help: "Number of active (handling request) ingester workers",
		}),
		requestsSent: promauto.With(r).NewCounter(prometheus.CounterOpts{
			Name: "cortex_distributor_ingester_workers_requests_sent_total",
			Help: "AAA",
		}),
		responseReceived: promauto.With(r).NewCounter(prometheus.CounterOpts{
			Name: "cortex_distributor_ingester_workers_responses_received_total",
			Help: "BBB",
		}),
	}
}

var _ ring_client.PoolClient = &ingesterWorker{}

func newIngesterWorker(client client.HealthAndIngesterClient, logger log.Logger, metrics *ingesterWorkerMetrics) *ingesterWorker {
	return &ingesterWorker{
		HealthAndIngesterClient: client,

		ch:      make(chan ingesterRequest),
		logger:  logger,
		metrics: metrics,
	}
}

func (w *ingesterWorker) Close() error {
	w.closed.Store(true)
	return w.HealthAndIngesterClient.Close()
}

var closedErr = errors.New("client closed")

func (w *ingesterWorker) push(ctx context.Context, user string, m *mimirpb.WriteRequest) error {
	if w.closed.Load() {
		return closedErr
	}

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

	s, err := w.HealthAndIngesterClient.PushStream(user.InjectOrgID(context.Background(), "dummy"))
	if err != nil {
		level.Warn(w.logger).Log("msg", "failed to initiate PushStream, closing client", "err", err)
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

			if !w.processRequest(s, r) {
				return
			}

		case <-waitTimeout.C:
			// no new request in some time, exit
			err = s.CloseSend()
			if err != nil {
				level.Warn(w.logger).Log("msg", "failed to close PushStream stream", "err", err)
			}
			_, err := s.Recv()
			if err == nil || !errors.Is(err, io.EOF) {
				level.Warn(w.logger).Log("msg", "unexpected error when closing stream, expected EOF", "err", err)
			}
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
		level.Warn(w.logger).Log("msg", "error while sending request to ingester", "err", err)

		r.ch <- ingesterResponse{err: err}
		return false
	}

	w.metrics.requestsSent.Inc()

	recv, err := s.Recv()
	if recv != nil {
		w.metrics.requestsSent.Inc()
	}

	r.ch <- ingesterResponse{resp: recv, err: err}

	if err != nil {
		level.Warn(w.logger).Log("msg", "error while receiving response from ingester", "err", err)
		return false
	}
	return true
}
