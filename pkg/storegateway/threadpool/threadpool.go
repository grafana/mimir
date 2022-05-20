// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cpg1111/threadpool-go/blob/master/threadpool.go
// Provenance-includes-license: MIT
// Provenance-includes-copyright: Christian Grabowski

package threadpool

import (
	"context"
	"errors"
	"time"

	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	labelWaiting = "waiting"
	labelRunning = "running"
)

var ErrPoolStopped = errors.New("thread pool has been stopped")

type Threadpool struct {
	services.Service

	// pool is used for callers to acquire and return threads, blocking when they are all in use.
	pool chan *osThread
	// threads is used to perform operations on all threads at once (such as stopping and shutting down).
	threads []*osThread
	// stopping is closed when calling code wants the threadpool to shut down.
	stopping chan struct{}

	timing *prometheus.HistogramVec
	tasks  prometheus.Gauge
}

// NewThreadpool creates a new instance of Threadpool that runs tasks in a pool
// of dedicated OS threads, meaning no other goroutines are scheduled on the threads.
// If num is zero, no threads will be started and all calls to Execute will run in
// the goroutine of the caller.
func NewThreadpool(num uint, reg prometheus.Registerer) *Threadpool {
	tp := &Threadpool{
		pool:     make(chan *osThread, num),
		threads:  make([]*osThread, num),
		stopping: make(chan struct{}),

		timing: promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
			Name:    "cortex_storegateway_thread_pool_seconds",
			Help:    "Amount of time spent performing operations on a dedicated thread",
			Buckets: []float64{0.001, 0.01, 0.1, 1, 10},
		}, []string{"stage"}),
		tasks: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "cortex_storegateway_thread_pool_tasks",
			Help: "Number of thread pool operations currently executing",
		}),
	}

	for i := uint(0); i < num; i++ {
		// Note that we are creating OS threads here but not starting their main loop
		// until the start method of this service is called. This means that the thread
		// pool is not usable until it has been started.
		t := newOSThread(tp.stopping)

		// Use a slice so that we keep a reference to all threads that are running
		// and we can easily stop all of them. However, use a channel as the pool
		// so that we can limit the number of threads in use and block when there
		// are none available.
		tp.threads[i] = t
		tp.pool <- t
	}

	tp.Service = services.NewBasicService(tp.start, tp.run, tp.stop)
	return tp
}

func (t *Threadpool) start(context.Context) error {
	// Start the main loop of each worker OS thread which causes it to start
	// running closures and returning their results.
	for _, thread := range t.threads {
		thread.start()
	}

	return nil
}

func (t *Threadpool) run(ctx context.Context) error {
	// Our run method doesn't actually do any work, that's left to each of the
	// workers that are accessed via the Execute method. Just block until this
	// service is stopped.
	<-ctx.Done()
	return nil
}

func (t *Threadpool) stop(error) error {
	// Close that channel that we've given to our threads to indicate that they
	// should stop accepting new work and end.
	close(t.stopping)

	// Wait for all threads, regardless if they are "in" the pool or being used to
	// run caller code. The avoids race conditions where threads are removed and
	// added back to the pool while we are trying to stop all of them.
	for _, thread := range t.threads {
		thread.join()
	}

	return nil
}

func (t *Threadpool) Execute(fn func() (interface{}, error)) (interface{}, error) {
	// No threads to run the closure in. This is how operators disable use of the threadpool
	// so just run the closure in the caller's goroutine in that case.
	if len(t.threads) == 0 {
		return fn()
	}

	start := time.Now()

	select {
	case <-t.stopping:
		return nil, ErrPoolStopped
	case thread := <-t.pool:
		acquired := time.Now()

		defer func() {
			complete := time.Now()

			t.pool <- thread
			t.tasks.Dec()
			t.timing.WithLabelValues(labelWaiting).Observe(acquired.Sub(start).Seconds())
			t.timing.WithLabelValues(labelRunning).Observe(complete.Sub(acquired).Seconds())
		}()

		t.tasks.Inc()
		return thread.execute(fn)
	}
}
