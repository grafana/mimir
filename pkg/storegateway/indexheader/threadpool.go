// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cpg1111/threadpool-go/blob/master/threadpool.go
// Provenance-includes-license: MIT
// Provenance-includes-copyright: Christian Grabowski

package indexheader

import (
	"errors"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	labelWaiting = "waiting"
	labelRunning = "running"
)

var ErrPoolStopped = errors.New("thread pool has been stopped")

type Threadpool struct {
	// pool is used for callers to acquire and return threads, blocking when they are all in use.
	pool chan *osThread
	// threads is used to perform operations on all threads at once (such as stopping and shutting down).
	threads []*osThread
	// stopping is closed when calling code wants the threadpool to shut down.
	stopping chan struct{}
	// stopped is closed once all threads have stopped running.
	stopped chan struct{}

	timing *prometheus.HistogramVec
	tasks  prometheus.Gauge
}

// NewThreadPool creates a new instance of ThreadPool that runs tasks in a pool
// of dedicated OS threads, meaning no other goroutines are scheduled on the threads.
// If num is less than or equal to zero, nil is returned.
func NewThreadPool(num int, reg prometheus.Registerer) *Threadpool {
	if num <= 0 {
		return nil
	}

	tp := &Threadpool{
		pool:     make(chan *osThread, num),
		threads:  make([]*osThread, num),
		stopping: make(chan struct{}),
		stopped:  make(chan struct{}),

		timing: promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
			Name:    "cortex_bucket_store_indexheader_thread_pool_seconds",
			Help:    "Amount of time spent performing index header operations on a dedicated thread",
			Buckets: []float64{0.001, 0.01, 0.1, 1, 10},
		}, []string{"stage"}),
		tasks: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "cortex_bucket_store_indexheader_thread_pool_tasks",
			Help: "Number of index header operations currently executing",
		}),
	}

	for i := 0; i < num; i++ {
		t := newOSThread(tp.stopping)
		t.start()

		// Use a slice so that we keep a reference to all threads that are running
		// and we can easily stop all of them. However, use a channel as the pool
		// so that we can limit the number of threads in use and block when there
		// are none available.
		tp.threads[i] = t
		tp.pool <- t
	}

	return tp
}

func (t *Threadpool) run() {
	defer close(t.stopped)

	// The t.stopping channel is never written so this blocks until the channel is
	// closed at which point the threadpool is shutting down, so we want to stop
	// each of the expected threads in it.
	<-t.stopping

	// Wait for all threads, regardless if they are "in" the pool or being used to
	// run caller code. The avoids race conditions where threads are removed and
	// added back to the pool while we are trying to stop all of them.
	for _, thread := range t.threads {
		thread.join()
	}
}

func (t *Threadpool) Start() {
	go t.run()
}

func (t *Threadpool) StopAndWait() {
	// Indicate to all threads that they should stop, then wait for them to do so
	// by trying to read from a channel that will be closed when all threads have
	// finally stopped.
	close(t.stopping)
	<-t.stopped
}

func (t *Threadpool) Execute(fn func() (interface{}, error)) (interface{}, error) {
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
