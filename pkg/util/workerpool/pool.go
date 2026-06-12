// SPDX-License-Identifier: AGPL-3.0-only

// Package workerpool runs func() tasks on a fixed set of worker goroutines.
// Tasks are pulled from a per-tenant queue in round-robin order,
// so one tenant flooding the pool with work cannot starve another tenant's tasks.
package workerpool

import (
	"context"
	"errors"
	"fmt"
	"math"
	"runtime"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/grafana/mimir/pkg/queue"
)

// ErrPoolStopped is returned when the pool is stopped while a submission was waiting.
var ErrPoolStopped = errors.New("worker pool is stopped")

// Config holds the pool's settings.
// The pool deliberately does not register its own flags;
// each caller defines its flags and maps them into a Config, keeping flag names in the caller's namespace.
type Config struct {
	// Size is the number of worker goroutines.
	// 0 selects runtime.GOMAXPROCS(0).
	Size int `yaml:"size" category:"experimental"`
}

// Validate returns an error if the config is invalid.
func (cfg *Config) Validate() error {
	if cfg.Size < 0 {
		return errors.New("worker pool size must be >= 0")
	}
	return nil
}

// Pool is a running worker pool; construct one with New.
//
// A panicking task is not recovered: the panic unwinds the worker goroutine and crashes the process,
// just as it would have before this pool existed.
// Callers that want panics turned into errors must recover inside the func they submit.
type Pool struct {
	services.Service

	queue   *queue.Queue
	workers int
	name    string
	logger  log.Logger

	taskDuration *prometheus.HistogramVec

	workersWg sync.WaitGroup
}

// task pairs the submitted func with its dimension so the worker can label
// the run-duration metric without the queue having to return the dimension.
type task struct {
	dimension string
	fn        func()
}

// queueForgetDelay is passed to the underlying queue.
// Pool workers never disconnect while running,
// so this only controls how long a disconnected worker is remembered before the queue forgets it.
const queueForgetDelay = 0 * time.Second

// New constructs a Pool.
// name becomes a const label on the pool's metrics, so several pools can share one registerer.
func New(cfg Config, name string, reg prometheus.Registerer, logger log.Logger) (*Pool, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	size := cfg.Size
	if size == 0 {
		size = runtime.GOMAXPROCS(0)
	}

	constLabels := prometheus.Labels{"pool": name}
	queueLength := promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
		Name:        "mimir_workerpool_queue_length",
		Help:        "Number of work items queued in the worker pool, per tenant.",
		ConstLabels: constLabels,
	}, []string{"user"})
	discarded := promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name:        "mimir_workerpool_discarded_requests_total",
		Help:        "Total number of work items discarded by the worker pool because the tenant's queue was full.",
		ConstLabels: constLabels,
	}, []string{"user"})
	enqueueDuration := promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
		Name:        "mimir_workerpool_enqueue_duration_seconds",
		Help:        "Time spent enqueueing work items into the worker pool.",
		ConstLabels: constLabels,
	})
	taskDuration := promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
		Name:        "mimir_workerpool_task_duration_seconds",
		Help:        "Time taken to execute work items in the worker pool, per task dimension.",
		ConstLabels: constLabels,
		// Task execution ranges from sub-millisecond index lookups to multi-second
		// high-cardinality counts; these buckets match cortex_ingester_tsdb_appender_add_duration_seconds.
		Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
	}, []string{"dimension"})

	// The queue requires a per-tenant cap, but we don't want one, so pass an effectively unlimited value.
	// Use MaxInt32 rather than MaxInt64 so internal "+1" bookkeeping in the queue can't overflow.
	rq, err := queue.New(
		logger,
		math.MaxInt32,
		queueForgetDelay,
		queueLength,
		discarded,
		enqueueDuration,
	)
	if err != nil {
		return nil, fmt.Errorf("creating worker pool queue: %w", err)
	}

	p := &Pool{
		queue:        rq,
		workers:      size,
		name:         name,
		logger:       logger,
		taskDuration: taskDuration,
	}
	p.Service = services.NewBasicService(p.starting, p.running, p.stopping)
	return p, nil
}

func (p *Pool) starting(ctx context.Context) (err error) {
	if startErr := services.StartAndAwaitRunning(ctx, p.queue); startErr != nil {
		return fmt.Errorf("starting worker pool queue: %w", startErr)
	}
	// The queue is now running and we are about to launch worker goroutines.
	// dskit's BasicService does NOT call stopping() when starting() returns an error,
	// so if anything below fails we must tear down the queue and workers ourselves
	// to avoid leaking them.
	defer func() {
		if err != nil {
			if stopErr := p.stopQueueAndWaitForWorkers(); stopErr != nil {
				level.Error(p.logger).Log("msg", "cleaning up worker pool after failed start", "pool", p.name, "err", stopErr)
			}
		}
	}()

	// Wait for every worker to register with the queue before reporting the pool as running.
	// Otherwise work submitted right after StartAndAwaitRunning could sit in the queue with no worker connected to pick it up.
	// If a worker fails to register, fail startup:
	// a half-staffed pool would quietly do less work than configured, with no signal other than a growing queue.
	ready := make(chan error, p.workers)
	for i := 0; i < p.workers; i++ {
		p.workersWg.Add(1)
		go p.workerLoop(ready)
	}
	for i := 0; i < p.workers; i++ {
		select {
		case regErr := <-ready:
			if regErr != nil {
				err = fmt.Errorf("registering worker pool worker: %w", regErr)
				return err
			}
		case <-ctx.Done():
			err = ctx.Err()
			return err
		}
	}
	return nil
}

func (p *Pool) running(ctx context.Context) error {
	<-ctx.Done()
	return nil
}

func (p *Pool) stopping(_ error) error {
	return p.stopQueueAndWaitForWorkers()
}

// stopQueueAndWaitForWorkers stops the inner queue and waits for all worker goroutines to exit.
// Stopping the queue first makes AwaitItemForConsumer return ErrStopped to idle workers
// (and unblocks any still registering), which lets the workers exit cleanly and workersWg drain.
// It uses context.Background() deliberately: cleanup must not depend on a possibly
// already-cancelled caller context.
func (p *Pool) stopQueueAndWaitForWorkers() error {
	err := services.StopAndAwaitTerminated(context.Background(), p.queue)
	p.workersWg.Wait()
	return err
}

func (p *Pool) workerLoop(ready chan<- error) {
	defer p.workersWg.Done()

	conn := queue.NewUnregisteredConsumerWorkerConn(context.Background(), p.name)
	err := p.queue.AwaitRegisterConsumerWorkerConn(conn)
	// Always signal, even on failure, so starting() does not block on a worker that has already given up.
	// Forward the registration error so a failed registration fails startup.
	ready <- err
	if err != nil {
		return
	}
	defer p.queue.SubmitUnregisterConsumerWorkerConn(conn)

	lastTenantIdx := queue.FirstTenant()
	for {
		dequeueReq := queue.NewConsumerWorkerDequeueRequest(conn, lastTenantIdx)
		item, idx, err := p.queue.AwaitItemForConsumer(dequeueReq)
		lastTenantIdx = idx
		if err != nil {
			// The queue only returns an error when it is shutting down (ErrStopped) or when the worker's connection context is cancelled.
			// We give workers a background context, so in practice this is always a shutdown:
			// the worker should exit, and stopping() waits for it.
			if errors.Is(err, queue.ErrStopped) || errors.Is(err, context.Canceled) {
				return
			}
			// Anything else is unexpected, but do not exit on it:
			// a worker that quit on a transient error would silently shrink the pool,
			// and if they all did the pool would still report Running while doing no work at all.
			// Log it and keep serving.
			level.Warn(p.logger).Log("msg", "worker pool worker received unexpected error from queue; continuing", "pool", p.name, "worker", conn.WorkerID, "err", err)
			continue
		}
		// Submit only ever enqueues a task, so this assertion cannot fail in practice.
		// If it does, an invariant is broken; panic loudly rather than silently drop the work.
		t := item.(task)
		start := time.Now()
		t.fn()
		p.taskDuration.WithLabelValues(t.dimension).Observe(time.Since(start).Seconds())
	}
}

// Submit enqueues fn to run on a worker on behalf of tenantID.
// It returns ErrPoolStopped if the pool is shutting down.
//
// dimension groups tasks by type. The queue spreads workers across dimensions,
// so different task types make progress in parallel instead of one type monopolising every worker.
//
// Two arguments to the underlying queue are worth explaining:
//   - maxConsumers=0 disables shuffle sharding, so every worker is eligible to run any tenant's tasks.
//     All the pool's workers register under one consumer, so per-tenant consumer sharding does not apply.
//   - successFn=nil is fine: the queue only calls successFn when it is non-nil,
//     and the pool has nothing to do between enqueue and dispatch.
//
// If Queue.SubmitItemToEnqueue ever changes these behaviours, this call must be updated to pass real values.
func (p *Pool) Submit(dimension string, tenantID string, fn func()) error {
	t := task{dimension: dimension, fn: fn}
	if err := p.queue.SubmitItemToEnqueue(tenantID, t, dimension, 0, nil); err != nil {
		if errors.Is(err, queue.ErrStopped) {
			return ErrPoolStopped
		}
		return err
	}
	return nil
}
