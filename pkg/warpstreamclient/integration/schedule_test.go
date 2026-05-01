// SPDX-License-Identifier: AGPL-3.0-only

package integration

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

// produceClient is the minimum surface runSchedule exercises against both
// *WarpstreamClient and the kgo baseline wrapper.
type produceClient interface {
	Produce(ctx context.Context, r *kgo.Record, promise func(*kgo.Record, error))
	Close()
}

// scheduleEvent fires its produce action at scheduleEvent.at relative to the
// start time passed to runSchedule. records is the per-partition fan-out for
// one application request.
type scheduleEvent struct {
	at      time.Duration
	records []*kgo.Record
}

// buildSchedule produces one scheduleEvent every spacing for the given
// duration. Each event carries one record per partition so the application
// request fans out to every partition simultaneously.
func buildSchedule(topic string, numPartitions int32, duration, spacing time.Duration, value string) []scheduleEvent {
	var events []scheduleEvent
	for at := time.Duration(0); at < duration; at += spacing {
		recs := make([]*kgo.Record, 0, numPartitions)
		for p := int32(0); p < numPartitions; p++ {
			recs = append(recs, &kgo.Record{Topic: topic, Partition: p, Value: []byte(value)})
		}
		events = append(events, scheduleEvent{at: at, records: recs})
	}
	return events
}

// runSchedule runs a schedule against one client, recording every produce's
// latency and outcome. Each scheduleEvent is modeled as one application
// request that writes to every partition concurrently: appObs records the
// per-event latency (from issue to the slowest partition's promise) and the
// first non-nil per-record error. partObs records the per-record latency
// for side-channel diagnostics. Returns once every promise has fired or
// every per-event ctx has expired.
func runSchedule(ctx context.Context, client produceClient, events []scheduleEvent) (appObs, partObs *observations) {
	appObs = &observations{}
	partObs = &observations{}
	start := time.Now()
	var outerWG sync.WaitGroup
	for _, ev := range events {
		ev := ev
		outerWG.Add(1)
		go func() {
			defer outerWG.Done()
			d := time.Until(start.Add(ev.at))
			if d > 0 {
				select {
				case <-time.After(d):
				case <-ctx.Done():
					return
				}
			}
			issued := time.Now()
			// Per-event ctx caps the application request: if the slowest
			// partition hasn't acked within scenarioAppRequestTimeout the
			// in-flight Produce calls observe ctx cancellation and fire
			// with ctx.Err.
			eventCtx, eventCancel := context.WithTimeout(ctx, scenarioAppRequestTimeout)
			defer eventCancel()

			var (
				innerWG  sync.WaitGroup
				mu       sync.Mutex
				firstErr error
			)
			for _, rec := range ev.records {
				innerWG.Add(1)
				recIssued := time.Now()
				client.Produce(eventCtx, rec, func(_ *kgo.Record, err error) {
					partObs.record(recIssued, time.Since(recIssued), err)
					if err != nil {
						mu.Lock()
						if firstErr == nil {
							firstErr = err
						}
						mu.Unlock()
					}
					innerWG.Done()
				})
			}
			innerWG.Wait()
			appObs.record(issued, time.Since(issued), firstErr)
		}()
	}
	outerWG.Wait()
	return appObs, partObs
}

func TestBuildSchedule_EventCountAndShape(t *testing.T) {
	t.Parallel()
	const topic = "t"
	const partitions = int32(3)
	events := buildSchedule(topic, partitions, 5*time.Second, time.Second, "v")
	assert.Len(t, events, 5)
	for i, ev := range events {
		assert.Equal(t, time.Duration(i)*time.Second, ev.at, "event %d", i)
		assert.Len(t, ev.records, int(partitions), "event %d records", i)
		for p := int32(0); p < partitions; p++ {
			assert.Equal(t, topic, ev.records[p].Topic)
			assert.Equal(t, p, ev.records[p].Partition)
			assert.Equal(t, []byte("v"), ev.records[p].Value)
		}
	}
}

func TestBuildSchedule_EmptyWhenDurationLEZero(t *testing.T) {
	t.Parallel()
	events := buildSchedule("t", 3, 0, time.Second, "v")
	assert.Empty(t, events)
}

// fakeProduceClient is a tiny in-memory client used for runSchedule tests.
// It fires the promise synchronously after an optional delay so the
// scheduler can be exercised without spinning up real Kafka machinery.
type fakeProduceClient struct {
	delay time.Duration
	err   error
}

func (f *fakeProduceClient) Produce(ctx context.Context, r *kgo.Record, promise func(*kgo.Record, error)) {
	go func() {
		if f.delay > 0 {
			select {
			case <-time.After(f.delay):
			case <-ctx.Done():
				promise(r, ctx.Err())
				return
			}
		}
		promise(r, f.err)
	}()
}

func (f *fakeProduceClient) Close() {}

func TestRunSchedule_RecordsAppAndPartitionObservations(t *testing.T) {
	t.Parallel()
	events := buildSchedule("t", 2, 200*time.Millisecond, 100*time.Millisecond, "v")

	app, part := runSchedule(context.Background(), &fakeProduceClient{delay: 5 * time.Millisecond}, events)

	// 2 events × 2 partitions = 4 partition observations.
	require.Len(t, part.snapshot(), 4)
	// 2 events at the app level.
	require.Len(t, app.snapshot(), 2)
	for _, o := range app.snapshot() {
		assert.NoError(t, o.err)
	}
}

func TestRunSchedule_EventCtxTimeoutFailsEvent(t *testing.T) {
	t.Parallel()
	// One event at t=0 with one partition, with simulated latency that
	// exceeds the per-event app timeout.
	events := []scheduleEvent{{at: 0, records: []*kgo.Record{{Topic: "t", Partition: 0}}}}
	client := &fakeProduceClient{delay: scenarioAppRequestTimeout + 200*time.Millisecond}

	app, _ := runSchedule(context.Background(), client, events)

	snap := app.snapshot()
	require.Len(t, snap, 1)
	assert.Error(t, snap[0].err)
	// App latency capped near the timeout.
	assert.Less(t, snap[0].latency, scenarioAppRequestTimeout+time.Second)
}
