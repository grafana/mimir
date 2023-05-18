// SPDX-License-Identifier: AGPL-3.0-only

package ruler

import (
	"context"
	"sync"
	"time"

	"github.com/grafana/dskit/services"
)

const (
	defaultRulerSyncPollFrequency = 10 * time.Second
)

type rulerSyncQueue struct {
	services.Service

	queueMx sync.Mutex
	queue   map[string]struct{}

	pollChan      chan []string
	pollFrequency time.Duration
}

func newRulerSyncQueue(pollFrequency time.Duration) *rulerSyncQueue {
	q := &rulerSyncQueue{
		pollChan:      make(chan []string),
		pollFrequency: pollFrequency,
		queue:         map[string]struct{}{},
	}

	q.Service = services.NewBasicService(nil, q.running, nil)
	return q
}

func (q *rulerSyncQueue) running(ctx context.Context) error {
	ticker := time.NewTicker(q.pollFrequency)
	defer ticker.Stop()

	for {
		q.queueMx.Lock()
		userIDs := make([]string, 0, len(q.queue))
		for userID := range q.queue {
			userIDs = append(userIDs, userID)
			delete(q.queue, userID)
		}
		q.queueMx.Unlock()

		if len(userIDs) > 0 {
			select {
			case q.pollChan <- userIDs:
			case <-ctx.Done():
				// We're done.
				return nil
			}
		}

		// Wait.
		select {
		case <-ticker.C:
		case <-ctx.Done():
			// We're done.
			return nil
		}
	}
}

// enqueue adds to the queue the request to sync rules for the input userID.
func (q *rulerSyncQueue) enqueue(userIDs ...string) {
	q.queueMx.Lock()
	defer q.queueMx.Unlock()

	for _, userID := range userIDs {
		q.queue[userID] = struct{}{}
	}
}

// poll returns a channel from which you can get the list of user IDs to sync.
func (q *rulerSyncQueue) poll() chan []string {
	return q.pollChan
}

// rulerSyncQueueProcessor is a service which polls from a queue and invoke
// a callback function to process the polled tenants.
type rulerSyncQueueProcessor struct {
	services.Service

	queue   *rulerSyncQueue
	process func(ctx context.Context, userIDs []string)
}

func newRulerSyncQueueProcessor(queue *rulerSyncQueue, process func(ctx context.Context, userIDs []string)) *rulerSyncQueueProcessor {
	q := &rulerSyncQueueProcessor{
		queue:   queue,
		process: process,
	}

	q.Service = services.NewBasicService(nil, q.running, nil)
	return q
}

func (p *rulerSyncQueueProcessor) running(ctx context.Context) error {
	for ctx.Err() == nil {
		select {
		case userIDs := <-p.queue.poll():
			p.process(ctx, userIDs)
		case <-ctx.Done():
			return nil
		}
	}

	return nil
}
