// SPDX-License-Identifier: AGPL-3.0-only

// Package workerpool provides a tenant-fair worker pool that executes opaque
// units of work submitted as func() values. Work is dispatched across workers
// using a per-tenant fair queue, so a tenant submitting many large tasks
// cannot starve another tenant's small task.
//
// Note: this package currently depends on pkg/scheduler/queue for its
// underlying queue implementation. A follow-up will extract the generic
// tenant-fair queue into pkg/util so this package no longer imports
// pkg/scheduler.
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

	"github.com/grafana/mimir/pkg/scheduler/queue"
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
type Pool struct {
	services.Service

	name   string
	logger log.Logger

	queue   *queue.RequestQueue
	workers int

	workersWg sync.WaitGroup
}

// poolItem is the opaque value enqueued into the underlying queue. The pool's
// worker goroutines type-assert dequeued items to *poolItem and invoke fn.
type poolItem struct {
	fn   func()
	done chan struct{}
}

const (
	// poolWorkerID is the synthetic querierID used to register all of the pool's
	// workers with the underlying queue. The queue's tenant-fair dispatch does
	// not distinguish between workers belonging to the same querierID, which is
	// what we want.
	poolWorkerID = "worker-pool"

	// queueForgetDelay is passed to the underlying RequestQueue. Pool workers
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
	// inflightRequests is required by the underlying queue but is observed in
	// terms of query components, which do not apply here. The pool registers
	// a no-op summary that the queue will populate with zeroed observations.
	inflightRequests := promauto.With(reg).NewSummaryVec(prometheus.SummaryOpts{
		Name:        "cortex_workerpool_inflight_requests",
		Help:        "Inflight work items in the worker pool. Always zero for pool usage; retained for queue compatibility.",
		ConstLabels: constLabels,
	}, []string{"query_component"})

	// The underlying queue requires a per-tenant cap; passing math.MaxInt32
	// effectively disables it. MaxInt32 (not MaxInt64) avoids any overflow
	// risk in the broker's `tenantQueueSize + 1 > maxTenantQueueSize` check.
	rq, err := queue.NewRequestQueue(
		logger,
		math.MaxInt32,
		queueForgetDelay,
		queueLength,
		discarded,
		enqueueDuration,
		inflightRequests,
	)
	if err != nil {
		return nil, fmt.Errorf("creating worker pool queue: %w", err)
	}

	p := &Pool{
		name:    name,
		logger:  logger,
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
	for i := 0; i < p.workers; i++ {
		p.workersWg.Add(1)
		go p.workerLoop()
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
	// AwaitRequestForQuerier calls, allowing workers to exit cleanly.
	err := services.StopAndAwaitTerminated(context.Background(), p.queue)
	p.workersWg.Wait()
	return err
}

func (p *Pool) workerLoop() {
	defer p.workersWg.Done()

	conn := queue.NewUnregisteredQuerierWorkerConn(context.Background(), poolWorkerID)
	if err := p.queue.AwaitRegisterQuerierWorkerConn(conn); err != nil {
		return
	}
	defer p.queue.SubmitUnregisterQuerierWorkerConn(conn)

	lastTenantIdx := queue.FirstTenant()
	for {
		dequeueReq := queue.NewQuerierWorkerDequeueRequest(conn, lastTenantIdx)
		item, idx, err := p.queue.AwaitRequestForQuerier(dequeueReq)
		lastTenantIdx = idx
		if err != nil {
			return
		}
		pi, ok := item.(*poolItem)
		if !ok || pi == nil {
			continue
		}
		pi.fn()
		close(pi.done)
	}
}

// Submit enqueues fn for execution by a worker on behalf of tenantID. It
// returns a channel that is closed when fn completes. Returns ErrPoolStopped
// if the pool is shutting down.
func (p *Pool) Submit(tenantID string, fn func()) (<-chan struct{}, error) {
	item := &poolItem{fn: fn, done: make(chan struct{})}
	if err := p.queue.SubmitRequestToEnqueue(tenantID, item, "", 0, nil); err != nil {
		if errors.Is(err, queue.ErrStopped) {
			return nil, ErrPoolStopped
		}
		return nil, err
	}
	return item.done, nil
}

// SubmitAndWait submits all fns for tenantID and blocks until every submitted
// fn completes or ctx is cancelled.
//
// If a submission fails it returns the error immediately, but still waits for
// any already-submitted fns to finish before returning, so callers can rely
// on no submitted fn outliving the call.
//
// If ctx is cancelled while waiting, SubmitAndWait returns ctx.Err() without
// interrupting in-flight fns - the caller's fn must observe its own context.
func (p *Pool) SubmitAndWait(ctx context.Context, tenantID string, fns []func()) error {
	if len(fns) == 0 {
		return nil
	}

	dones := make([]<-chan struct{}, 0, len(fns))
	var submitErr error
	for _, fn := range fns {
		done, err := p.Submit(tenantID, fn)
		if err != nil {
			submitErr = err
			break
		}
		dones = append(dones, done)
	}

	for _, d := range dones {
		select {
		case <-d:
		case <-ctx.Done():
			// Don't return immediately - drain remaining done channels to keep
			// the call's invariant (fns either ran to completion or are still
			// running but no longer block this caller).
			//
			// Actually, the caller may want to abandon waiting. Return ctx.Err()
			// but the fns still run; callers should propagate ctx into their fns.
			return ctx.Err()
		}
	}
	return submitErr
}
