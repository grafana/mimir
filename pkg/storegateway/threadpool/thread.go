// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cpg1111/threadpool-go/blob/master/thread.go
// Provenance-includes-license: MIT
// Provenance-includes-copyright: Christian Grabowski

package threadpool

import (
	"runtime"
)

type execResult struct {
	value interface{}
	err   error
}

type osThread struct {
	// stopping is a request for this thread to stop running.
	stopping chan struct{}

	// stopped is confirmation that the thread has stopped running.
	stopped chan struct{}

	// call is used to submit closures for this thread to execute.
	call chan func() (interface{}, error)

	// res is used to send the results of running closures back to callers.
	res chan execResult
}

func newOSThread(stopping chan struct{}) *osThread {
	return &osThread{
		stopping: stopping,
		stopped:  make(chan struct{}),

		call: make(chan func() (interface{}, error)),
		res:  make(chan execResult),
	}
}

func (o *osThread) run() {
	runtime.LockOSThread()

	// NOTE: That we are purposefully not unlocking the OS thread here. When a
	// goroutine that was locked to an OS thread ends without unlocking it, the
	// underlying OS thread is terminated because it is assumed to be tainted.
	// We want that here to ensure that the code using mmap hasn't forgotten to
	// clean up any state.
	defer close(o.stopped)

	for {
		select {
		case <-o.stopping:
			return
		case fn := <-o.call:
			val, err := fn()
			o.res <- execResult{value: val, err: err}
		}
	}
}

func (o *osThread) start() {
	go o.run()
}

// join waits for the thread to complete any tasks it was executing after the
// stopping channel is closed by the parent thread pool. This allows callers to
// ensure the thread has completed executing which is useful in tests to verify
// no goroutines leak.
func (o *osThread) join() {
	<-o.stopped
}

func (o *osThread) execute(fn func() (interface{}, error)) (interface{}, error) {
	select {
	case <-o.stopping:
		// Make sure the pool (and hence this thread) hasn't been stopped before
		// submitting our function to run.
		return nil, ErrPoolStopped
	case o.call <- fn:
		res := <-o.res
		return res.value, res.err
	}
}
