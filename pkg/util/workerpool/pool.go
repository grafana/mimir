// SPDX-License-Identifier: AGPL-3.0-only

// Package workerpool provides a tenant-fair worker pool that executes opaque
// units of work submitted as func() values. Work is dispatched across workers
// using a per-tenant fair queue, so a tenant submitting many large tasks
// cannot starve another tenant's small task.
package workerpool

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math"
	"runtime"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/grafana/mimir/pkg/queue"
)

// ErrPoolStopped is returned when the pool is stopped while a submission was waiting.
var ErrPoolStopped = errors.New("worker pool is stopped")

// Config configures a worker Pool. Use RegisterFlagsWithPrefix to wire it to CLI flags.
type Config struct {
	// Size is the number of worker goroutines. 0 selects runtime.GOMAXPROCS(0).
	Size int `yaml:"size" category:"experimental"`
}

// RegisterFlagsWithPrefix registers the config flags under the given prefix.
func (cfg *Config) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.IntVar(&cfg.Size, prefix+"size", 0, "Number of worker goroutines in the pool. 0 uses GOMAXPROCS.")
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

	workersWg sync.WaitGroup
}

const (
	// poolWorkerID is the synthetic querierID used to register all of the pool's
	// workers with the underlying queue. The queue's tenant-fair dispatch does
	// not distinguish between workers belonging to the same querierID, which is
	// what we want.
	poolWorkerID = "worker-pool"

	// queueForgetDelay is passed to the underlying Queue. Pool workers
	// never disconnect during normal operation; the value only affects how
	// long disconnected workers are remembered before being forgotten.
	queueForgetDelay = 0 * time.Second
)

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
	// through.
	ready := make(chan struct{}, p.workers)
	for i := 0; i < p.workers; i++ {
		p.workersWg.Add(1)
		go p.workerLoop(ready)
	}
	for i := 0; i < p.workers; i++ {
		select {
		case <-ready:
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

func (p *Pool) workerLoop(ready chan<- struct{}) {
	defer p.workersWg.Done()

	conn := queue.NewUnregisteredQuerierWorkerConn(context.Background(), poolWorkerID)
	err := p.queue.AwaitRegisterQuerierWorkerConn(conn)
	// Signal readiness even on registration failure so starting() does not
	// deadlock waiting for a worker that has already exited.
	ready <- struct{}{}
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
			return
		}
		fn, ok := item.(func())
		if !ok {
			continue
		}
		fn()
	}
}

// Submit enqueues fn for execution by a worker on behalf of tenantID. Returns
// ErrPoolStopped if the pool is shutting down.
//
// The underlying queue call passes:
//   - queueDimension="": treated by the broker as "unknown"; the pool has no
//     per-component dimension, so all tasks share a single first-layer queue.
//   - maxQueriers=0: treated by tenantQuerierShards as "no shuffle sharding"
//     (the sentinel for "every querier-worker is eligible"). The pool only
//     registers a single querierID, so sharding is meaningless here.
//   - successFn=nil: guarded by the queue (it only invokes successFn when
//     non-nil); the pool has no work to do between enqueue and dispatch.
//
// Future changes to Queue.SubmitItemToEnqueue must preserve these
// behaviors, or the pool must be updated to supply real values.
func (p *Pool) Submit(tenantID string, fn func()) error {
	if err := p.queue.SubmitItemToEnqueue(tenantID, fn, "", 0, nil); err != nil {
		if errors.Is(err, queue.ErrStopped) {
			return ErrPoolStopped
		}
		return err
	}
	return nil
}
