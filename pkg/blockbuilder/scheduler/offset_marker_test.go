// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/goleak"

	"github.com/grafana/mimir/pkg/storage/bucket"
)

const testMarkerKey = bucket.MimirInternalsPrefix + "/block-builder-scheduler/ingest/test-builder.json"

func TestOffsetMarker_EncodeDecode(t *testing.T) {
	now := time.Date(2026, 11, 18, 10, 0, 0, 0, time.UTC)
	offsets := markerOffsets{{0: 100, 3: 7}, {0: 250}}
	ids := []clusterIdentity{{kafkaClusterID: "kc-0", kafkaTopicID: "t-0"}, {}}
	m := newOffsetMarker("ingest", "grp", offsets, ids, now)

	data, err := m.encode()
	require.NoError(t, err)

	// Output is deterministic and human readable. IDs the broker doesn't report are omitted.
	again, err := m.encode()
	require.NoError(t, err)
	assert.Equal(t, string(data), string(again))
	assert.Contains(t, string(data), `"version":1`)
	assert.Contains(t, string(data), `"consumer_group":"grp"`)
	assert.Contains(t, string(data), `{"cluster_id":0,"kafka_cluster_id":"kc-0","kafka_topic_id":"t-0","offsets":{"0":100,"3":7}}`)
	assert.Contains(t, string(data), `{"cluster_id":1,"offsets":{"0":250}}`)

	got, err := decodeOffsetMarker(data)
	require.NoError(t, err)
	assert.Equal(t, m.Topic, got.Topic)
	assert.Equal(t, m.ConsumerGroup, got.ConsumerGroup)
	assert.True(t, now.Equal(got.Timestamp))
	assert.Equal(t, map[int32]int64{0: 100, 3: 7}, got.offsetsForCluster(0))
	assert.Equal(t, map[int32]int64{0: 250}, got.offsetsForCluster(1))
	assert.Nil(t, got.offsetsForCluster(2))
	assert.Equal(t, "kc-0", got.cluster(0).KafkaClusterID)
	assert.Equal(t, "t-0", got.cluster(0).KafkaTopicID)
}

func TestOffsetMarker_DecodeErrors(t *testing.T) {
	_, err := decodeOffsetMarker([]byte(`not json`))
	require.Error(t, err)

	_, err = decodeOffsetMarker([]byte(`{"version":2,"topic":"ingest"}`))
	require.ErrorContains(t, err, "unexpected offset marker version 2, expected 1")

	_, err = decodeOffsetMarker([]byte(`{}`))
	require.ErrorContains(t, err, "unexpected offset marker version 0, expected 1")
}

func TestClusterIdentity_Conflict(t *testing.T) {
	a := clusterIdentity{kafkaClusterID: "c1", kafkaTopicID: "t1"}
	assert.Equal(t, "", a.conflict(a))
	assert.Equal(t, "kafka_cluster_id", a.conflict(clusterIdentity{kafkaClusterID: "c2", kafkaTopicID: "t1"}))
	assert.Equal(t, "kafka_topic_id", a.conflict(clusterIdentity{kafkaClusterID: "c1", kafkaTopicID: "t2"}))

	// If either side doesn't know an ID, there is nothing to compare.
	assert.Equal(t, "", a.conflict(clusterIdentity{}))
	assert.Equal(t, "", clusterIdentity{}.conflict(a))
	assert.Equal(t, "", a.conflict(clusterIdentity{kafkaClusterID: "c1"}))
	assert.Equal(t, "kafka_topic_id", a.conflict(clusterIdentity{kafkaTopicID: "t2"}))
}

func TestOffsetMarkerStore_ReadWrite(t *testing.T) {
	ctx := context.Background()
	bkt := objstore.NewInMemBucket()
	st := newOffsetMarkerStore(bkt, "ingest", "test-builder")

	m, err := st.read(ctx)
	require.NoError(t, err)
	require.Nil(t, m, "no marker yet is not an error")

	require.NoError(t, st.write(ctx, newOffsetMarker("ingest", "test-builder", markerOffsets{{1: 5}}, nil, time.Now())))

	ok, err := bkt.Exists(ctx, testMarkerKey)
	require.NoError(t, err)
	require.True(t, ok, "marker must live under the reserved Mimir prefix, in a directory per topic")

	m, err = st.read(ctx)
	require.NoError(t, err)
	require.NotNil(t, m)
	assert.Equal(t, map[int32]int64{1: 5}, m.offsetsForCluster(0))

	// The key has the topic and the group, escaped, so neither can escape its directory.
	assert.Equal(t, "block-builder-scheduler/a%2Fb/c%2Fd.json", newOffsetMarkerStore(bkt, "a/b", "c/d").key)
	// Another topic, same group: another object.
	other, err := newOffsetMarkerStore(bkt, "ingest-2", "test-builder").read(ctx)
	require.NoError(t, err)
	require.Nil(t, other)
}

func TestOffsetMarkerStore_ReadCorrupt(t *testing.T) {
	ctx := context.Background()
	bkt := objstore.NewInMemBucket()
	require.NoError(t, bkt.Upload(ctx, testMarkerKey, strings.NewReader("garbage")))
	_, err := newOffsetMarkerStore(bkt, "ingest", "test-builder").read(ctx)
	require.Error(t, err)
}

func TestMarkerWriteGate(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	g := markerWriteGate{refreshInterval: time.Hour}
	a := markerOffsets{{0: 10}}

	require.True(t, g.shouldWrite(a, t0, false), "first write always happens (startup)")
	g.recordWrite(a, t0)

	require.False(t, g.shouldWrite(markerOffsets{{0: 10}}, t0.Add(time.Minute), false), "unchanged: skip")
	require.True(t, g.shouldWrite(markerOffsets{{0: 11}}, t0.Add(time.Minute), false), "offset advanced")
	require.True(t, g.shouldWrite(markerOffsets{{0: 10, 1: 1}}, t0.Add(time.Minute), false), "partition added")
	require.True(t, g.shouldWrite(markerOffsets{{0: 10}, {}}, t0.Add(time.Minute), false), "cluster added")
	require.False(t, g.shouldWrite(a, t0.Add(59*time.Minute), false), "still within the refresh interval")
	require.True(t, g.shouldWrite(a, t0.Add(time.Hour), false), "forced refresh after the interval")
	require.True(t, g.shouldWrite(a, t0.Add(time.Minute), true), "force flag (shutdown)")

	// A failed write never calls recordWrite, so the gate keeps asking.
	require.True(t, g.shouldWrite(markerOffsets{{0: 11}}, t0.Add(2*time.Minute), false))
	require.True(t, g.shouldWrite(markerOffsets{{0: 11}}, t0.Add(3*time.Minute), false))

	// The gate must not alias the caller's maps.
	b := markerOffsets{{0: 20}}
	g.recordWrite(b, t0)
	b[0][0] = 21
	require.True(t, g.shouldWrite(b, t0, false))
}

func TestMergeMarkerOffsets(t *testing.T) {
	base := newOffsetMarker("ingest", "g", markerOffsets{{0: 100, 1: 200}, {0: 5}}, []clusterIdentity{{kafkaTopicID: "t0"}, {kafkaTopicID: "t1"}}, time.Now())
	current := markerOffsets{{0: 150}, {}}

	got := mergeMarkerOffsets(current, base, []clusterIdentity{{kafkaTopicID: "t0"}, {kafkaTopicID: "t1"}})
	assert.Equal(t, markerOffsets{{0: 150, 1: 200}, {0: 5}}, got, "present partitions win, absent ones are carried forward")
	assert.Equal(t, markerOffsets{{0: 150}, {}}, current, "inputs are not modified")

	got = mergeMarkerOffsets(current, base, nil)
	assert.Equal(t, markerOffsets{{0: 150, 1: 200}, {0: 5}}, got, "unknown IDs don't prevent carrying forward")

	// Cluster 1's topic was recreated: its old offsets are for a topic that no longer exists.
	got = mergeMarkerOffsets(current, base, []clusterIdentity{{kafkaTopicID: "t0"}, {kafkaTopicID: "NEW"}})
	assert.Equal(t, markerOffsets{{0: 150, 1: 200}, {}}, got)

	assert.Equal(t, current, mergeMarkerOffsets(current, nil, nil))
}

func TestMergeMarkerOffsets_NeverMovesBackwards(t *testing.T) {
	ids := []clusterIdentity{{kafkaTopicID: "t0"}}
	// The marker recorded partition 0 at 1000. Kafka then lost the commit and the scheduler resumed from
	// lookback-on-no-commit.
	base := newOffsetMarker("ingest", "g", markerOffsets{{0: 1000, 1: 50}}, ids, time.Now())

	t.Run("lookback behind the marker: the marker doesn't rewind while the range is rebuilt", func(t *testing.T) {
		got := mergeMarkerOffsets(markerOffsets{{0: 400}}, base, ids)
		assert.Equal(t, int64(1000), got[0][0])
	})

	t.Run("once the scheduler passes the marker, the live offset wins", func(t *testing.T) {
		got := mergeMarkerOffsets(markerOffsets{{0: 1200, 1: 60}}, base, ids)
		assert.Equal(t, markerOffsets{{0: 1200, 1: 60}}, got)
	})

	t.Run("a recreated topic's higher offsets are not kept", func(t *testing.T) {
		got := mergeMarkerOffsets(markerOffsets{{0: 10}}, base, []clusterIdentity{{kafkaTopicID: "NEW"}})
		assert.Equal(t, markerOffsets{{0: 10}}, got)
	})
}

func TestCompareMarkerToKafka(t *testing.T) {
	const topic = "ingest"
	kafka := func(offs map[int32]int64) kadm.Offsets {
		o := make(kadm.Offsets)
		for p, at := range offs {
			o.AddOffset(topic, p, at, 0)
		}
		return o
	}

	t.Run("each disagreement is counted by reason", func(t *testing.T) {
		marker := map[int32]int64{0: 500, 1: 600, 2: 300, 3: 10}
		k := kafka(map[int32]int64{0: 700, 1: 300, 2: 300, 4: 5})
		assert.Equal(t, map[string]int{
			mismatchMarkerBehind:    1, // 0
			mismatchMarkerAhead:     1, // 1
			mismatchMissingInKafka:  1, // 3
			mismatchMissingInMarker: 1, // 4
		}, compareMarkerToKafka(topic, marker, k))
	})

	t.Run("agreeing offsets", func(t *testing.T) {
		assert.Empty(t, compareMarkerToKafka(topic, map[int32]int64{0: 500}, kafka(map[int32]int64{0: 500})))
	})

	t.Run("no marker yet is not a mismatch", func(t *testing.T) {
		assert.Empty(t, compareMarkerToKafka(topic, nil, kafka(map[int32]int64{0: 400})))
	})
}

func TestMarkerOffsetsMissingInKafka(t *testing.T) {
	const topic = "ingest"
	k := make(kadm.Offsets)
	k.AddOffset(topic, 0, 700, 0)
	k.AddOffset("other-topic", 1, 900, 0)

	// Partition 0 has a Kafka commit. Partition 1 only has one for another topic, and partition 2 has none.
	got := markerOffsetsMissingInKafka(topic, map[int32]int64{0: 500, 1: 600, 2: 300}, k)
	assert.Equal(t, map[int32]int64{1: 600, 2: 300}, got)

	assert.Empty(t, markerOffsetsMissingInKafka(topic, nil, k))
}

// --- Writer.

type testWriter struct {
	w       *offsetMarkerWriter
	metrics schedulerMetrics
}

func newTestWriter(t *testing.T, bkt objstore.Bucket) *testWriter {
	reg := prometheus.NewPedanticRegistry()
	tw := &testWriter{metrics: newSchedulerMetrics(reg, false, 1)}
	tw.w = newOffsetMarkerWriter(newOffsetMarkerStore(bkt, "ingest", "test-builder"), "ingest", "test-builder", time.Hour, &tw.metrics, log.NewNopLogger())
	tw.w.minBackoff, tw.w.maxBackoff = 5*time.Millisecond, 20*time.Millisecond
	return tw
}

func (tw *testWriter) startAndRelease(t *testing.T) {
	tw.w.start()
	tw.w.release()
	t.Cleanup(func() { tw.w.stop(time.Second) })
}

func snap(offs ...map[int32]int64) markerSnapshot {
	return markerSnapshot{offsets: markerOffsets(offs)}
}

func (tw *testWriter) waitIdle(t *testing.T) {
	t.Helper()
	require.Eventually(t, tw.w.idle, 5*time.Second, time.Millisecond)
}

// gatedBucket lets tests hold, count and fail uploads.
type gatedBucket struct {
	objstore.Bucket

	mu       sync.Mutex
	block    chan struct{} // If not nil, uploads wait for it to be closed, or their context to be done.
	failWith error         // Fails uploads.
	getFail  error         // Fails gets.
	uploads  int
	started  chan struct{} // Receives a value when an upload starts.
	onUpload func(ctx context.Context, name string, data []byte)
}

func (b *gatedBucket) Upload(ctx context.Context, name string, r io.Reader, opts ...objstore.ObjectUploadOption) error {
	b.mu.Lock()
	b.uploads++
	block, failWith, started, onUpload := b.block, b.failWith, b.started, b.onUpload
	b.mu.Unlock()

	if started != nil {
		select {
		case started <- struct{}{}:
		default:
		}
	}
	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if failWith != nil {
		return failWith
	}
	if onUpload != nil {
		data, _ := io.ReadAll(r)
		onUpload(ctx, name, data)
		r = strings.NewReader(string(data))
	}
	return b.Bucket.Upload(ctx, name, r, opts...)
}

func (b *gatedBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	b.mu.Lock()
	getFail := b.getFail
	b.mu.Unlock()
	if getFail != nil {
		return nil, getFail
	}
	return b.Bucket.Get(ctx, name)
}

func (b *gatedBucket) uploadCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.uploads
}

func (b *gatedBucket) set(f func(b *gatedBucket)) {
	b.mu.Lock()
	defer b.mu.Unlock()
	f(b)
}

func TestWriter_WritesAndSkipsUnchanged(t *testing.T) {
	bkt := &gatedBucket{Bucket: objstore.NewInMemBucket()}
	tw := newTestWriter(t, bkt)
	tw.startAndRelease(t)

	tw.w.submit(snap(map[int32]int64{0: 100}), false)
	tw.waitIdle(t)
	require.Equal(t, 1, bkt.uploadCount(), "the first write after startup always happens")
	assert.Equal(t, 1.0, testutil.ToFloat64(tw.metrics.offsetMarkerWrites))

	tw.w.submit(snap(map[int32]int64{0: 100}), false)
	tw.waitIdle(t)
	require.Equal(t, 1, bkt.uploadCount(), "unchanged offsets: no write")

	tw.w.submit(snap(map[int32]int64{0: 120}), false)
	tw.waitIdle(t)
	require.Equal(t, 2, bkt.uploadCount())

	tw.w.submit(snap(map[int32]int64{0: 120}), true)
	tw.waitIdle(t)
	require.Equal(t, 3, bkt.uploadCount(), "force rewrites")

	m, err := newOffsetMarkerStore(bkt, "ingest", "test-builder").read(t.Context())
	require.NoError(t, err)
	assert.Equal(t, map[int32]int64{0: 120}, m.offsetsForCluster(0))
}

func TestWriter_RefreshesAfterInterval(t *testing.T) {
	bkt := &gatedBucket{Bucket: objstore.NewInMemBucket()}
	tw := newTestWriter(t, bkt)
	tw.startAndRelease(t)

	tw.w.submit(snap(map[int32]int64{0: 100}), false)
	tw.waitIdle(t)
	tw.w.gate.lastWrite = time.Now().Add(-2 * time.Hour) // Safe: the writer is idle.
	tw.w.submit(snap(map[int32]int64{0: 100}), false)
	tw.waitIdle(t)
	require.Equal(t, 2, bkt.uploadCount())
}

func TestWriter_SubmitDoesNotBlockAndSnapshotsCoalesce(t *testing.T) {
	bkt := &gatedBucket{Bucket: objstore.NewInMemBucket(), block: make(chan struct{}), started: make(chan struct{}, 1)}
	tw := newTestWriter(t, bkt)
	tw.startAndRelease(t)

	tw.w.submit(snap(map[int32]int64{0: 1}), false)
	<-bkt.started // The first write is now stuck in the object store.

	// Submitting never waits for it.
	done := make(chan struct{})
	go func() {
		for i := int64(2); i <= 50; i++ {
			tw.w.submit(snap(map[int32]int64{0: i}), false)
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("submit blocked on a marker write")
	}

	// Only the latest of the snapshots queued behind the stuck write is written.
	bkt.set(func(b *gatedBucket) { close(b.block); b.block = nil })
	tw.waitIdle(t)
	assert.Equal(t, 2, bkt.uploadCount(), "the first, and the latest; the 48 in between were replaced")
	m, err := newOffsetMarkerStore(bkt, "ingest", "test-builder").read(t.Context())
	require.NoError(t, err)
	assert.Equal(t, map[int32]int64{0: 50}, m.offsetsForCluster(0))
}

func TestWriter_BacksOffAfterFailuresThenRecovers(t *testing.T) {
	bkt := &gatedBucket{Bucket: objstore.NewInMemBucket(), failWith: errors.New("object store is down")}
	tw := newTestWriter(t, bkt)
	tw.w.minBackoff, tw.w.maxBackoff = 50*time.Millisecond, 200*time.Millisecond
	tw.startAndRelease(t)

	tw.w.submit(snap(map[int32]int64{0: 100}), false)
	time.Sleep(600 * time.Millisecond)
	attempts := bkt.uploadCount()
	assert.GreaterOrEqual(t, attempts, 3, "it retries without waiting for another submit")
	assert.LessOrEqual(t, attempts, 9, "but not in a hot loop")
	assert.Equal(t, float64(attempts), testutil.ToFloat64(tw.metrics.offsetMarkerWriteFailures))
	assert.Zero(t, testutil.ToFloat64(tw.metrics.offsetMarkerWrites))

	bkt.set(func(b *gatedBucket) { b.failWith = nil })
	require.Eventually(t, func() bool { return testutil.ToFloat64(tw.metrics.offsetMarkerWrites) == 1 }, 5*time.Second, 5*time.Millisecond)
	m, err := newOffsetMarkerStore(bkt, "ingest", "test-builder").read(t.Context())
	require.NoError(t, err)
	assert.Equal(t, map[int32]int64{0: 100}, m.offsetsForCluster(0))
}

func TestWriter_NeverWritesEmptyMarker(t *testing.T) {
	ctx := t.Context()
	bkt := objstore.NewInMemBucket()
	require.NoError(t, newOffsetMarkerStore(bkt, "ingest", "test-builder").write(ctx, newOffsetMarker("ingest", "test-builder", markerOffsets{{0: 42}}, nil, time.Now())))
	tw := newTestWriter(t, bkt)
	tw.startAndRelease(t)

	tw.w.submit(snap(map[int32]int64{}), true)
	tw.waitIdle(t)
	m, err := newOffsetMarkerStore(bkt, "ingest", "test-builder").read(ctx)
	require.NoError(t, err)
	assert.Equal(t, map[int32]int64{0: 42}, m.offsetsForCluster(0))
	assert.Zero(t, testutil.ToFloat64(tw.metrics.offsetMarkerWrites))
}

func TestWriter_CarriesForwardPartitionsMissingFromSnapshot(t *testing.T) {
	ctx := t.Context()
	bkt := objstore.NewInMemBucket()
	require.NoError(t, newOffsetMarkerStore(bkt, "ingest", "test-builder").write(ctx, newOffsetMarker("ingest", "test-builder", markerOffsets{{0: 100, 1: 80}}, nil, time.Now())))
	tw := newTestWriter(t, bkt)
	tw.startAndRelease(t) // Reads the marker in the background.

	// The scheduler only knows partition 0, as Kafka lost partition 1's commit.
	tw.w.submit(snap(map[int32]int64{0: 110}), false)
	tw.waitIdle(t)
	m, err := newOffsetMarkerStore(bkt, "ingest", "test-builder").read(ctx)
	require.NoError(t, err)
	assert.Equal(t, map[int32]int64{0: 110, 1: 80}, m.offsetsForCluster(0), "partition 1's offset must not be dropped")
}

func TestWriter_UnreadableMarkerPausesWritesUntilReadable(t *testing.T) {
	ctx := t.Context()
	mem := objstore.NewInMemBucket()
	require.NoError(t, newOffsetMarkerStore(mem, "ingest", "test-builder").write(ctx, newOffsetMarker("ingest", "test-builder", markerOffsets{{0: 100, 1: 80}}, nil, time.Now())))
	bkt := &gatedBucket{Bucket: mem, getFail: errors.New("boom")}
	tw := newTestWriter(t, bkt)
	tw.startAndRelease(t)

	// While the marker can't be read we don't overwrite it, as we might drop partitions it holds.
	tw.w.submit(snap(map[int32]int64{0: 110}), false)
	require.Eventually(t, func() bool { return testutil.ToFloat64(tw.metrics.offsetMarkerReadFailures) > 1 }, 5*time.Second, 5*time.Millisecond)
	m, err := newOffsetMarkerStore(mem, "ingest", "test-builder").read(ctx)
	require.NoError(t, err)
	assert.Equal(t, map[int32]int64{0: 100, 1: 80}, m.offsetsForCluster(0))

	bkt.set(func(b *gatedBucket) { b.getFail = nil })
	require.Eventually(t, func() bool {
		m, err := newOffsetMarkerStore(mem, "ingest", "test-builder").read(ctx)
		return err == nil && m.offsetsForCluster(0)[0] == 110
	}, 5*time.Second, 5*time.Millisecond)
	m, err = newOffsetMarkerStore(mem, "ingest", "test-builder").read(ctx)
	require.NoError(t, err)
	assert.Equal(t, map[int32]int64{0: 110, 1: 80}, m.offsetsForCluster(0))
}

func TestWriter_StopWritesLatestWithinDeadline(t *testing.T) {
	bkt := &gatedBucket{Bucket: objstore.NewInMemBucket()}
	tw := newTestWriter(t, bkt)
	tw.w.start()
	tw.w.release()

	tw.w.submit(snap(map[int32]int64{0: 1}), false)
	tw.waitIdle(t)

	// A final snapshot, forced, even though nothing changed from the written one.
	tw.w.submit(snap(map[int32]int64{0: 1}), true)
	tw.w.stop(time.Second)
	assert.Equal(t, 2, bkt.uploadCount())
	tw.w.stop(time.Second) // Idempotent.

	// If the object store hangs, stop gives up at its deadline, abandoning the write in flight.
	hung := &gatedBucket{Bucket: objstore.NewInMemBucket(), block: make(chan struct{}), started: make(chan struct{}, 1)}
	tw2 := newTestWriter(t, hung)
	tw2.w.start()
	tw2.w.release()
	tw2.w.submit(snap(map[int32]int64{0: 1}), false)
	<-hung.started
	tw2.w.submit(snap(map[int32]int64{0: 2}), true)
	start := time.Now()
	tw2.w.stop(200 * time.Millisecond)
	assert.Less(t, time.Since(start), 5*time.Second)
	assert.Equal(t, 2, hung.uploadCount(), "the stuck write was abandoned, then the final one attempted")
}

func TestWriter_NoGoroutineLeak(t *testing.T) {
	opts := goleak.IgnoreCurrent()

	bkt := &gatedBucket{Bucket: objstore.NewInMemBucket()}
	// Stopped after use.
	tw := newTestWriter(t, bkt)
	tw.w.start()
	tw.w.release()
	tw.w.submit(snap(map[int32]int64{0: 1}), false)
	tw.waitIdle(t)
	tw.w.stop(time.Second)

	// Stopped while waiting to be released.
	tw2 := newTestWriter(t, bkt)
	tw2.w.start()
	tw2.w.stop(time.Second)

	// Stopped during a backoff after a failure.
	failing := &gatedBucket{Bucket: objstore.NewInMemBucket(), failWith: errors.New("down")}
	tw3 := newTestWriter(t, failing)
	tw3.w.minBackoff, tw3.w.maxBackoff = time.Hour, time.Hour
	tw3.w.start()
	tw3.w.release()
	tw3.w.submit(snap(map[int32]int64{0: 1}), false)
	require.Eventually(t, func() bool { return failing.uploadCount() > 0 }, 5*time.Second, time.Millisecond)
	tw3.w.stop(time.Second)

	// Never started.
	tw4 := newTestWriter(t, bkt)
	tw4.w.stop(time.Second)

	goleak.VerifyNone(t, opts)
}

// --- Scheduler.

// attachMarker enables the offset marker on a scheduler, writing to bkt.
func attachMarker(t *testing.T, sched *BlockBuilderScheduler, bkt objstore.Bucket) {
	t.Helper()
	sched.cfg.OffsetMarkerRefreshInterval = time.Hour
	sched.markerStore = newOffsetMarkerStore(bkt, "ingest", "test-builder")
	w := newOffsetMarkerWriter(sched.markerStore, "ingest", "test-builder", time.Hour, &sched.metrics, sched.logger)
	w.minBackoff, w.maxBackoff = 5*time.Millisecond, 20*time.Millisecond
	w.onBase = sched.onMarkerBaseLoaded
	sched.markerWriter = w
	sched.markerFinalTimeout = time.Second
	w.start()
	t.Cleanup(func() { w.stop(time.Second) })
}

// markerTestScheduler returns a scheduler on a fresh fake Kafka whose partition 0 holds `records` records
// and which writes its marker to bkt.
func markerTestScheduler(t *testing.T, bkt objstore.Bucket, records int) (*BlockBuilderScheduler, *kgo.Client) {
	sched, cli := mustScheduler(t, 2)
	produceRecords(t.Context(), t, cli, 0, records)
	attachMarker(t, sched, bkt)
	return sched, cli
}

func readTestMarker(t *testing.T, bkt objstore.Bucket) *offsetMarker {
	t.Helper()
	m, err := newOffsetMarkerStore(bkt, "ingest", "test-builder").read(t.Context())
	require.NoError(t, err)
	return m
}

func writeTestMarker(t *testing.T, bkt objstore.Bucket, offsets markerOffsets, ids []clusterIdentity) {
	t.Helper()
	require.NoError(t, newOffsetMarkerStore(bkt, "ingest", "test-builder").write(t.Context(), newOffsetMarker("ingest", "test-builder", offsets, ids, time.Now())))
}

func commitInKafka(t *testing.T, sched *BlockBuilderScheduler, offsets map[int32]int64) {
	t.Helper()
	o := kadm.Offsets{"ingest": {}}
	for p, at := range offsets {
		o["ingest"][p] = kadm.Offset{Topic: "ingest", Partition: p, At: at}
	}
	_, err := sched.adminClients[0].CommitOffsets(t.Context(), "test-builder", o)
	require.NoError(t, err)
}

func waitMarkerIdle(t *testing.T, sched *BlockBuilderScheduler) {
	t.Helper()
	require.Eventually(t, sched.markerWriter.idle, 5*time.Second, time.Millisecond)
}

func TestFlush_WritesMarkerInBackgroundAndCommitsKafka(t *testing.T) {
	ctx := t.Context()
	bkt := objstore.NewInMemBucket()
	sched, _ := markerTestScheduler(t, bkt, 150)
	require.NoError(t, sched.loadInitialCommittedOffsets(ctx))
	initCommits(sched, "ingest", 0, map[int]int64{0: 100})

	require.NoError(t, sched.flushOffsetsToKafka(ctx))
	requireCommittedInKafka(ctx, t, sched, 0, map[int32]int64{0: 100})
	waitMarkerIdle(t, sched)

	m := readTestMarker(t, bkt)
	require.NotNil(t, m)
	assert.Equal(t, "ingest", m.Topic)
	assert.Equal(t, "test-builder", m.ConsumerGroup)
	assert.Equal(t, map[int32]int64{0: 100}, m.offsetsForCluster(0))
	assert.NotEmpty(t, m.cluster(0).KafkaTopicID, "the fake Kafka reports topic IDs, which are recorded")
	assert.Equal(t, 1.0, testutil.ToFloat64(sched.metrics.offsetMarkerWrites))
}

func TestFlush_DoesNotWaitForTheMarker(t *testing.T) {
	ctx := t.Context()
	bkt := &gatedBucket{Bucket: objstore.NewInMemBucket(), block: make(chan struct{}), started: make(chan struct{}, 1)}
	sched, _ := markerTestScheduler(t, bkt, 150)
	require.NoError(t, sched.loadInitialCommittedOffsets(ctx))
	initCommits(sched, "ingest", 0, map[int]int64{0: 100})

	require.NoError(t, sched.flushOffsetsToKafka(ctx))
	<-bkt.started // The marker write is stuck in the object store.

	// Later flushes still commit to Kafka at once.
	for i := int64(101); i < 106; i++ {
		sched.getPartitionState("ingest", 0).initCommit(0, i)
		start := time.Now()
		require.NoError(t, sched.flushOffsetsToKafka(ctx))
		assert.Less(t, time.Since(start), 5*time.Second)
		requireCommittedInKafka(ctx, t, sched, 0, map[int32]int64{0: i})
	}
	assert.Zero(t, testutil.ToFloat64(sched.metrics.offsetMarkerWrites))
}

func TestFlush_DualWriteToleratesMarkerFailure(t *testing.T) {
	ctx := t.Context()
	failing := &bucket.ErrorInjectedBucketClient{
		Bucket:   objstore.NewInMemBucket(),
		Injector: bucket.InjectErrorOn(bucket.OpUpload, testMarkerKey, errors.New("object store is down")),
	}
	sched, _ := markerTestScheduler(t, failing, 150)
	require.NoError(t, sched.loadInitialCommittedOffsets(ctx))
	initCommits(sched, "ingest", 0, map[int]int64{0: 100})

	require.NoError(t, sched.flushOffsetsToKafka(ctx))
	requireCommittedInKafka(ctx, t, sched, 0, map[int32]int64{0: 100})
	require.Eventually(t, func() bool { return testutil.ToFloat64(sched.metrics.offsetMarkerWriteFailures) >= 1 }, 5*time.Second, time.Millisecond)
	assert.Zero(t, testutil.ToFloat64(sched.metrics.offsetMarkerWrites))
}

func TestScheduling_MakesNoObjectStorageCalls(t *testing.T) {
	ctx := t.Context()
	// Every object storage call hangs until its context ends. No writer goroutine is running, so any call
	// would be made by the scheduler's own goroutine.
	hung := &hungBucket{Bucket: objstore.NewInMemBucket()}
	sched, _ := mustScheduler(t, 2)
	produceRecords(ctx, t, mustProducer(t, sched), 0, 10)
	sched.cfg.OffsetMarkerRefreshInterval = time.Hour
	sched.markerStore = newOffsetMarkerStore(hung, "ingest", "test-builder")
	sched.markerWriter = newOffsetMarkerWriter(sched.markerStore, "ingest", "test-builder", time.Hour, &sched.metrics, sched.logger)
	// The writer is never started.

	done := make(chan struct{})
	go func() {
		defer close(done)
		assert.NoError(t, sched.loadInitialCommittedOffsets(ctx))
		initCommits(sched, "ingest", 0, map[int]int64{0: 5})
		assert.NoError(t, sched.flushOffsetsToKafka(ctx))
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("startup or flush waited for object storage")
	}
	assert.Zero(t, hung.calls.Load())
}

type hungBucket struct {
	objstore.Bucket
	calls atomic.Int64
}

func (b *hungBucket) Get(ctx context.Context, _ string) (io.ReadCloser, error) {
	b.calls.Add(1)
	<-ctx.Done()
	return nil, ctx.Err()
}

func (b *hungBucket) Upload(ctx context.Context, _ string, _ io.Reader, _ ...objstore.ObjectUploadOption) error {
	b.calls.Add(1)
	<-ctx.Done()
	return ctx.Err()
}

func mustProducer(t *testing.T, sched *BlockBuilderScheduler) *kgo.Client {
	t.Helper()
	cli, err := kgo.NewClient(kgo.SeedBrokers(sched.cfg.Kafka.Address...), kgo.Dialer(sched.cfg.Kafka.Dialer), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	require.NoError(t, err)
	t.Cleanup(cli.Close)
	return cli
}

func TestStopping_CommitsKafkaBeforeTheFinalMarkerWrite(t *testing.T) {
	ctx := t.Context()
	bkt := &gatedBucket{Bucket: objstore.NewInMemBucket()}
	sched, _ := markerTestScheduler(t, bkt, 150)
	require.NoError(t, sched.loadInitialCommittedOffsets(ctx))
	initCommits(sched, "ingest", 0, map[int]int64{0: 100})
	require.NoError(t, sched.flushOffsetsToKafka(ctx))
	waitMarkerIdle(t, sched)
	requireCommittedInKafka(ctx, t, sched, 0, map[int32]int64{0: 100})

	// Offsets advance, and then the scheduler stops. When the final marker write happens, Kafka must already hold them.
	sched.getPartitionState("ingest", 0).initCommit(0, 120)
	var kafkaAtMarkerWrite atomic.Int64
	kafkaAtMarkerWrite.Store(-1)
	bkt.set(func(b *gatedBucket) {
		b.onUpload = func(_ context.Context, _ string, _ []byte) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			offs, err := fetchCommittedOffsets(ctx, sched.adminClients[0], "test-builder", "ingest")
			if assert.NoError(t, err) {
				o, _ := offs.Lookup("ingest", 0)
				kafkaAtMarkerWrite.Store(o.At)
			}
		}
	})

	require.NoError(t, sched.stopping(nil))
	assert.Equal(t, int64(120), kafkaAtMarkerWrite.Load(), "Kafka was committed before the marker")
	assert.Equal(t, map[int32]int64{0: 120}, readTestMarker(t, bkt).offsetsForCluster(0))
}

func TestStopping_DoesNotWaitLongForAStuckObjectStore(t *testing.T) {
	ctx := t.Context()
	bkt := &gatedBucket{Bucket: objstore.NewInMemBucket(), block: make(chan struct{})}
	sched, _ := markerTestScheduler(t, bkt, 150)
	require.NoError(t, sched.loadInitialCommittedOffsets(ctx))
	initCommits(sched, "ingest", 0, map[int]int64{0: 100})

	sched.markerFinalTimeout = 100 * time.Millisecond // Production uses offsetMarkerFinalWriteTimeout.
	require.NoError(t, sched.flushOffsetsToKafka(ctx))

	start := time.Now()
	require.NoError(t, sched.stopping(nil))
	assert.Less(t, time.Since(start), offsetMarkerFinalWriteTimeout)

	// The scheduler's clients are closed by now: check with another.
	offs, err := fetchCommittedOffsets(ctx, kadm.NewClient(mustProducer(t, sched)), "test-builder", "ingest")
	require.NoError(t, err)
	o, _ := offs.Lookup("ingest", 0)
	assert.Equal(t, int64(100), o.At)
}

// --- Startup.

// The marker never changes where the scheduler resumes from: it only reports how it compares to Kafka.
func TestStartup_MarkerDoesNotChangeStartupOffsets(t *testing.T) {
	ctx := t.Context()

	for name, tc := range map[string]struct {
		marker markerOffsets
		kafka  map[int32]int64
		want   map[int]int64
	}{
		"marker ahead of Kafka":                          {markerOffsets{{0: 100}}, map[int32]int64{0: 50}, map[int]int64{0: 50}},
		"marker behind Kafka":                            {markerOffsets{{0: 10}}, map[int32]int64{0: 50}, map[int]int64{0: 50}},
		"marker beyond the partition end":                {markerOffsets{{0: 9999}}, map[int32]int64{0: 50}, map[int]int64{0: 50}},
		"marker, Kafka lost its commit":                  {markerOffsets{{0: 100}}, nil, map[int]int64{}},
		"Kafka commit, no marker for it":                 {markerOffsets{{1: 20}}, map[int32]int64{0: 50}, map[int]int64{0: 50}},
		"marker entry for a cluster that does not exist": {markerOffsets{{}, {0: 100}}, map[int32]int64{0: 50}, map[int]int64{0: 50}},
	} {
		t.Run(name, func(t *testing.T) {
			bkt := objstore.NewInMemBucket()
			writeTestMarker(t, bkt, tc.marker, nil)
			sched, _ := markerTestScheduler(t, bkt, 150)
			if tc.kafka != nil {
				commitInKafka(t, sched, tc.kafka)
			}

			require.NoError(t, sched.loadInitialCommittedOffsets(ctx))
			requireOffsets(t, sched, "ingest", 0, tc.want)
			requireOffsets(t, sched, "ingest", 1, map[int]int64{})
		})
	}
}

func TestStartup_MarkerMismatchIsReportedInTheBackground(t *testing.T) {
	ctx := t.Context()
	bkt := objstore.NewInMemBucket()
	writeTestMarker(t, bkt, markerOffsets{{0: 100}}, nil)
	sched, _ := markerTestScheduler(t, bkt, 150) // Kafka lost its commit.

	require.NoError(t, sched.loadInitialCommittedOffsets(ctx))
	requireOffsets(t, sched, "ingest", 0, map[int]int64{})
	require.Eventually(t, func() bool {
		return testutil.ToFloat64(sched.metrics.offsetMarkerMismatches.WithLabelValues(mismatchMissingInKafka)) == 1
	}, 5*time.Second, time.Millisecond)
}

func TestStartup_UnreadableMarkerDoesNotBlockStartup(t *testing.T) {
	ctx := t.Context()
	mem := objstore.NewInMemBucket()
	writeTestMarker(t, mem, markerOffsets{{0: 100}}, nil)

	for name, bkt := range map[string]objstore.Bucket{
		"the object store fails": &gatedBucket{Bucket: mem, getFail: errors.New("object store is down")},
		"the marker is corrupt": func() objstore.Bucket {
			corrupt := objstore.NewInMemBucket()
			require.NoError(t, corrupt.Upload(ctx, testMarkerKey, strings.NewReader("garbage")))
			return corrupt
		}(),
		"the marker is for another topic": func() objstore.Bucket {
			other := objstore.NewInMemBucket()
			require.NoError(t, newOffsetMarkerStore(other, "ingest", "test-builder").write(ctx, newOffsetMarker("other", "test-builder", markerOffsets{{0: 1}}, nil, time.Now())))
			return other
		}(),
	} {
		t.Run(name, func(t *testing.T) {
			sched, _ := markerTestScheduler(t, bkt, 150)
			commitInKafka(t, sched, map[int32]int64{0: 50})

			require.NoError(t, sched.loadInitialCommittedOffsets(ctx))
			requireOffsets(t, sched, "ingest", 0, map[int]int64{0: 50})
			require.Eventually(t, func() bool { return testutil.ToFloat64(sched.metrics.offsetMarkerReadFailures) >= 1 }, 5*time.Second, time.Millisecond)
		})
	}
}

// An entry of a deleted topic must not be carried into the next marker, though startup doesn't use it.
func TestStartup_MarkerEntryOfARecreatedTopicIsNotCarriedForward(t *testing.T) {
	ctx := t.Context()
	bkt := objstore.NewInMemBucket()
	writeTestMarker(t, bkt, markerOffsets{{0: 100, 1: 7}}, []clusterIdentity{{kafkaTopicID: "the-id-of-a-deleted-topic"}})
	sched, _ := markerTestScheduler(t, bkt, 150)
	commitInKafka(t, sched, map[int32]int64{0: 50})

	require.NoError(t, sched.loadInitialCommittedOffsets(ctx))
	require.NotEmpty(t, sched.clusterInfos[0].identity.kafkaTopicID, "this test needs the fake Kafka to report topic IDs")
	requireOffsets(t, sched, "ingest", 0, map[int]int64{0: 50})

	require.NoError(t, sched.flushOffsetsToKafka(ctx))
	waitMarkerIdle(t, sched)
	m := readTestMarker(t, bkt)
	assert.Equal(t, map[int32]int64{0: 50}, m.offsetsForCluster(0), "partition 1 of the old topic is dropped")
	assert.Equal(t, sched.clusterInfos[0].identity.kafkaTopicID, m.cluster(0).KafkaTopicID)
}

func TestStartup_LookbackFallbackCountsOnlySkippedRecords(t *testing.T) {
	ctx := t.Context()
	sched, cli := markerTestScheduler(t, objstore.NewInMemBucket(), 0)
	sched.cfg.LookbackOnNoCommit = time.Hour

	// Partition 0: old records, so resuming from the lookback skips them.
	for i := 0; i < 20; i++ {
		require.NoError(t, cli.ProduceSync(ctx, &kgo.Record{Topic: "ingest", Partition: 0, Value: []byte("v"), Timestamp: time.Now().Add(-5 * time.Hour)}).FirstErr())
	}
	// Partition 1: new records, all within the lookback: nothing is skipped.
	for i := 0; i < 20; i++ {
		require.NoError(t, cli.ProduceSync(ctx, &kgo.Record{Topic: "ingest", Partition: 1, Value: []byte("v"), Timestamp: time.Now()}).FirstErr())
	}

	require.NoError(t, sched.loadInitialCommittedOffsets(ctx))
	require.NoError(t, sched.completeObservationMode(ctx))
	assert.Equal(t, 1.0, testutil.ToFloat64(sched.metrics.lookbackFallbacks), "only partition 0 skipped records")
}

// --- Snapshot.

func TestSnapshotAllOffsets_SingleLock(t *testing.T) {
	sched, _ := mustMultiClusterScheduler(t, 2, 2)

	sched.mu.Lock()
	type result struct{ committed, planned []kadm.Offsets }
	got := make(chan result, 1)
	go func() {
		c, p := sched.snapshotAllOffsets()
		got <- result{c, p}
	}()

	// A job completing on both clusters, applied under one lock hold, can't be observed half done.
	select {
	case <-got:
		t.Fatal("the snapshot didn't wait for the lock")
	case <-time.After(50 * time.Millisecond):
	}
	ps := sched.getPartitionState("ingest", 0)
	ps.initCommit(0, 200)
	ps.initCommit(1, 200)
	sched.mu.Unlock()

	r := <-got
	for clusterID := 0; clusterID < 2; clusterID++ {
		o, ok := r.committed[clusterID].Lookup("ingest", 0)
		require.True(t, ok)
		assert.Equal(t, int64(200), o.At, "cluster %d", clusterID)
		p, ok := r.planned[clusterID].Lookup("ingest", 0)
		require.True(t, ok)
		assert.Equal(t, int64(200), p.At, "cluster %d", clusterID)
	}
}
