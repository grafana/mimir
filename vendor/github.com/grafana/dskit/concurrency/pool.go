package concurrency

import (
	"context"
	"sync"

	"github.com/grafana/dskit/multierror"
)

// WorkerPool ensures that for any number of concurrent ForEachNotInFlight calls each with any number of tokens only up to numWorkers f
// invocations are executing concurrently. See the docs of ForEachNotInFlight for the uniqueness semantics of tokens.
type WorkerPool struct {
	inflightTokensMx sync.Mutex
	inflightTokens   map[string]struct{}
	inflightCalls    sync.WaitGroup
	semaphore        chan struct{}
}

func NewWorkerPool(numWorkers int) *WorkerPool {
	return &WorkerPool{
		inflightTokensMx: sync.Mutex{},
		inflightTokens:   make(map[string]struct{}),
		inflightCalls:    sync.WaitGroup{},
		semaphore:        make(chan struct{}, numWorkers),
	}
}

// Wait returns when there are no in-flight calls to ForEachNotInFlight.
func (w *WorkerPool) Wait() {
	w.inflightCalls.Wait()
}

// ForEachNotInFlight invokes f for every token in tokens that is not in-flight (not still being executed) in a different
// concurrent call to ForEachNotInFlight. ForEachNotInFlight returns when invocations to f for all such tokens have
// returned. Upon context cancellation ForEachNotInFlight stops making new invocations of f for tokens and waits for all
// already started invocations of f to return. ForEachNotInFlight returns the combined errors from all f invocations.
func (w *WorkerPool) ForEachNotInFlight(ctx context.Context, tokens []string, f func(context.Context, string) error) error {
	w.inflightCalls.Add(1)
	defer w.inflightCalls.Done()

	notInflightTokens := make([]string, 0, len(tokens))
	w.inflightTokensMx.Lock()
	for _, token := range tokens {
		if _, ok := w.inflightTokens[token]; ok {
			continue
		}
		notInflightTokens = append(notInflightTokens, token)
		w.inflightTokens[token] = struct{}{}
	}
	w.inflightTokensMx.Unlock()

	var (
		errs    multierror.MultiError
		errsMx  sync.Mutex
		workers sync.WaitGroup
	)

	for _, token := range notInflightTokens {
		select {
		case <-ctx.Done():
			workers.Wait()
			return errs.Err()
		case w.semaphore <- struct{}{}:
		}

		workers.Add(1)
		go func(token string) {
			if err := f(ctx, token); err != nil {
				errsMx.Lock()
				errs.Add(err)
				errsMx.Unlock()
			}

			w.inflightTokensMx.Lock()
			delete(w.inflightTokens, token)
			w.inflightTokensMx.Unlock()

			<-w.semaphore
			workers.Done()
		}(token)
	}

	workers.Wait()
	return errs.Err()
}
