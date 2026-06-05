// SPDX-License-Identifier: AGPL-3.0-only

// Package workerpool provides a tenant-fair worker pool that executes opaque
// units of work submitted as func() values. Work is dispatched across workers
// using a per-tenant fair queue, so a tenant submitting many large tasks
// cannot starve another tenant's small task.
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

// Config configures a worker Pool. Each caller wires its own user-facing flags
// and translates them into a Config; the pool intentionally does not register
// flags itself so that flag names live in the caller's namespace.
type Config struct {
	// Size is the number of worker goroutines. 0 selects runtime.GOMAXPROCS(0).
	Size int `yaml:"size" category:"experimental"`
}

// Validate returns an error if the config is invalid.
func (cfg *Config) Validate() error {
	if cfg.Size < 0 {
		return errors.New("worker pool size must be >= 0")
	}
	return nil
}

// Pool is a tenant-fair worker pool. Use New to construct one.
//
// A panicking task is not recovered: the worker goroutine unwinds and the
// panic crashes the process, matching the behaviour callers had before this
// pool existed. Callers that need to convert panics into errors must wrap
// their submitted function themselves.
type Pool struct {
	services.Service

	queue   *queue.Queue
	workers int
	name    string
	logger  log.Logger

	workersWg sync.WaitGroup
}

// queueForgetDelay is passed to the underlying Queue. Pool workers
// never disconnect during normal operation; the value only affects how long
// disconnected workers are remembered before being forgotten.
const queueForgetDelay = 0 * time.Second

// New constructs a Pool. The name is used as a const label on the pool's
// metrics so multiple pools can share a registerer.
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
		Name:        "cortex_workerpool_queue_length",
		Help:        "Number of work items queued in the worker pool, per tenant.",
		ConstLabels: constLabels,
	}, []string{"user"})
	discarded := promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name:        "cortex_workerpool_discarded_requests_total",
		Help:        "Total number of work items discarded by the worker pool because the tenant's queue was full.",
		ConstLabels: constLabels,
	}, []string{"user"})
	enqueueDuration := promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
		Name:        "cortex_workerpool_enqueue_duration_seconds",
		Help:        "Time spent enqueueing work items into the worker pool.",
		ConstLabels: constLabels,
	})

	// The underlying queue requires a per-tenant cap; passing math.MaxInt32
	// effectively disables it. MaxInt32 (not MaxInt64) avoids any overflow
	// risk in the broker's `tenantQueueSize + 1 > maxTenantQueueSize` check.
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
		queue:   rq,
		workers: size,
		name:    name,
		logger:  logger,
	}
	p.Service = services.NewBasicService(p.starting, p.running, p.stopping)
	return p, nil
}

func (p *Pool) starting(ctx context.Context) error {
	if err := services.StartAndAwaitRunning(ctx, p.queue); err != nil {
		return fmt.Errorf("starting worker pool queue: %w", err)
	}
	// Wait for each worker to finish registering with the queue before
	// reporting the pool as running. Otherwise a Submit immediately after
	// StartAndAwaitRunning could be enqueued before any worker is connected
	// to the queue dispatcher, leaving the chunk idle until registration races
	// through. A registration failure fails pool startup: a partially-staffed
	// pool would silently process less work than configured, with no visible
	// signal beyond a growing queue.
	ready := make(chan error, p.workers)
	for i := 0; i < p.workers; i++ {
		p.workersWg.Add(1)
		go p.workerLoop(fmt.Sprintf("%s-worker-%d", p.name, i), ready)
	}
	for i := 0; i < p.workers; i++ {
		select {
		case err := <-ready:
			if err != nil {
				return fmt.Errorf("registering worker pool worker: %w", err)
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (p *Pool) running(ctx context.Context) error {
	<-ctx.Done()
	return nil
}

func (p *Pool) stopping(_ error) error {
	// Stop the queue first. The queue's dispatcher will drain any pending items
	// (workers continue processing) and then return ErrStopped from any pending
	// AwaitItemForQuerier calls, allowing workers to exit cleanly.
	err := services.StopAndAwaitTerminated(context.Background(), p.queue)
	p.workersWg.Wait()
	return err
}

func (p *Pool) workerLoop(workerID string, ready chan<- error) {
	defer p.workersWg.Done()

	conn := queue.NewUnregisteredQuerierWorkerConn(context.Background(), workerID)
	err := p.queue.AwaitRegisterQuerierWorkerConn(conn)
	// Always signal so starting() does not deadlock waiting for a worker that
	// has already exited. Pass the registration error through so a failure
	// fails pool startup.
	ready <- err
	if err != nil {
		return
	}
	defer p.queue.SubmitUnregisterQuerierWorkerConn(conn)

	lastTenantIdx := queue.FirstTenant()
	for {
		dequeueReq := queue.NewQuerierWorkerDequeueRequest(conn, lastTenantIdx)
		item, idx, err := p.queue.AwaitItemForQuerier(dequeueReq)
		lastTenantIdx = idx
		if err != nil {
			// The queue only errors when it is shutting down (ErrStopped) or when
			// the worker connection's context is cancelled. The pool gives workers
			// a background context, so in practice this is always a shutdown, and
			// the worker should exit; stopping() waits for all of them.
			if errors.Is(err, queue.ErrStopped) || errors.Is(err, context.Canceled) {
				return
			}
			// Any other error is unexpected. Do not exit: a worker that retired on
			// a transient error would silently shrink the pool, and if every worker
			// did so the pool would report Running with zero workers and quietly
			// stop making progress. Log and keep serving instead.
			level.Warn(p.logger).Log("msg", "worker pool worker received unexpected error from queue; continuing", "pool", p.name, "worker", workerID, "err", err)
			continue
		}
		// Submit's signature guarantees func(), so the assertion is total. A
		// failure here means an internal invariant has been broken; panic so
		// it surfaces immediately rather than silently dropping work.
		fn := item.(func())
		fn()
	}
}

// Submit enqueues fn for execution by a worker on behalf of tenantID. Returns
// ErrPoolStopped if the pool is shutting down.
//
// The underlying queue call passes:
//   - maxQueriers=0: treated by tenantQuerierShards as "no shuffle sharding"
//     (the sentinel for "every querier-worker is eligible"). The pool only
//     registers a single querierID, so sharding is meaningless here.
//   - successFn=nil: guarded by the queue (it only invokes successFn when
//     non-nil); the pool has no work to do between enqueue and dispatch.
//
// Future changes to Queue.SubmitItemToEnqueue must preserve these
// behaviors, or the pool must be updated to supply real values.
func (p *Pool) Submit(dimension string, tenantID string, fn func()) error {
	if err := p.queue.SubmitItemToEnqueue(tenantID, fn, dimension, 0, nil); err != nil {
		if errors.Is(err, queue.ErrStopped) {
			return ErrPoolStopped
		}
		return err
	}
	return nil
}
