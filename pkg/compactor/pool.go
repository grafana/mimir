package compactor

import (
	"context"
	"sync"

	"github.com/grafana/dskit/multierror"
)

type workerPool struct {
	inflightUsersMx sync.Mutex
	inflightUsers   map[string]struct{}
	inflightCalls   sync.WaitGroup
	semaphore       chan struct{}
}

func newWorkerPool(numWorkers int) *workerPool {
	return &workerPool{
		inflightUsersMx: sync.Mutex{},
		inflightUsers:   make(map[string]struct{}),
		inflightCalls:   sync.WaitGroup{},
		semaphore:       make(chan struct{}, numWorkers),
	}
}

func (w *workerPool) wait() {
	w.inflightCalls.Wait()
}

// doForUsers returns false if this user is already being executed; if doForUsers returns false, f was not invoked.
// doForUsers returns true if f was invoked along with the error that f returned.
func (w *workerPool) doForUsers(ctx context.Context, userIDs []string, f func(context.Context, string) error) error {
	w.inflightCalls.Add(1)
	defer w.inflightCalls.Done()

	notInflightUsers := make([]string, 0, len(userIDs))
	w.inflightUsersMx.Lock()
	for _, userID := range userIDs {
		if _, ok := w.inflightUsers[userID]; ok {
			continue
		}
		notInflightUsers = append(notInflightUsers, userID)
		w.inflightUsers[userID] = struct{}{}
	}
	w.inflightUsersMx.Unlock()

	errs := multierror.New()
	var errsMx sync.Mutex
	var workers sync.WaitGroup

	for _, userID := range notInflightUsers {
		select {
		case <-ctx.Done():
			workers.Wait()
			return ctx.Err()
		case w.semaphore <- struct{}{}:
		}

		workers.Add(1)
		go func(userID string) {
			if err := f(ctx, userID); err != nil {
				errsMx.Lock()
				errs.Add(err)
				errsMx.Unlock()
			}

			w.inflightUsersMx.Lock()
			delete(w.inflightUsers, userID)
			w.inflightUsersMx.Unlock()

			<-w.semaphore
			workers.Done()
		}(userID)
	}

	workers.Wait()
	return errs.Err()
}
