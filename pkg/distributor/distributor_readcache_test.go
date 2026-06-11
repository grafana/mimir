// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/grafana/mimir/pkg/nautilus/readcacheassignment"
	"github.com/grafana/mimir/pkg/nautilus/rebalancer"
)

// fakeReadcacheWatchStream feeds canned WatchReadcacheAssignments
// responses to consumeReadcacheStream, then io.EOF. Only Recv is
// implemented; the embedded grpc.ClientStream is never touched.
type fakeReadcacheWatchStream struct {
	grpc.ClientStream
	responses []*rebalancer.WatchReadcacheAssignmentsResponse
}

func (s *fakeReadcacheWatchStream) Recv() (*rebalancer.WatchReadcacheAssignmentsResponse, error) {
	if len(s.responses) == 0 {
		return nil, io.EOF
	}
	resp := s.responses[0]
	s.responses = s.responses[1:]
	return resp, nil
}

// TestConsumeReadcacheStream_DeltaProtocol exercises the client side
// of the delta wire protocol: a reset snapshot primes the local log,
// deltas are upserted into it (including a To rewrite from a
// preemption), and the server's prune horizon drops aged-out
// entries.
func TestConsumeReadcacheStream_DeltaProtocol(t *testing.T) {
	t.Parallel()

	t0 := time.Date(2026, 5, 13, 12, 0, 0, 0, time.UTC)
	d := &Distributor{}

	mkProto := func(entries ...readcacheassignment.LogEntry) []rebalancer.ReadcacheLogEntry {
		return rebalancer.ReadcacheEntriesToProto(entries)
	}

	old := readcacheassignment.LogEntry{PartitionID: 0, InstanceID: "rc-a", From: t0, To: t0.Add(5 * time.Minute)}
	aged := readcacheassignment.LogEntry{PartitionID: 1, InstanceID: "rc-c", From: t0.Add(-2 * time.Hour), To: t0.Add(-time.Hour)}

	stream := &fakeReadcacheWatchStream{responses: []*rebalancer.WatchReadcacheAssignmentsResponse{
		// Priming snapshot.
		{Entries: mkProto(old, aged), Reset_: true},
		// Delta: rc-a's lease is clamped by a move (To rewrite) and
		// rc-b gets a fresh lease. The prune horizon drops `aged`.
		{
			Entries: mkProto(
				readcacheassignment.LogEntry{PartitionID: 0, InstanceID: "rc-a", From: t0, To: t0.Add(3 * time.Minute)},
				readcacheassignment.LogEntry{PartitionID: 0, InstanceID: "rc-b", From: t0.Add(time.Minute), To: t0.Add(6 * time.Minute)},
			),
			PruneBeforeUnixMs: t0.Add(-30 * time.Minute).UnixMilli(),
		},
	}}

	require.ErrorIs(t, d.consumeReadcacheStream(stream), io.EOF)

	log := d.GetReadcacheLog()
	require.NotNil(t, log)

	// The aged entry must be pruned, the other two merged.
	require.Equal(t, 2, log.Len())

	// rc-a's To must reflect the delta's rewrite: with the original
	// To it would still be an owner at t0+4m.
	owners := log.Lookup(t0.Add(4*time.Minute), 0)
	assert.Equal(t, []string{"rc-b"}, owners,
		"after the preemption delta, only rc-b may own partition 0 at t0+4m")

	// Both owners are visible across the move window.
	assert.Equal(t, []string{"rc-a", "rc-b"}, log.OwnersDuring(0, t0, t0.Add(6*time.Minute)))
}

// TestPreviousReadcacheOwnerForPartition exercises the
// previous-owner lookup the distributor uses when readcache returns
// a "still warming" error. The plan calls for the previous owner of
// a partition to be queryable for a short window after a move so
// reads see at least one warm head.
func TestPreviousReadcacheOwnerForPartition(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 13, 12, 0, 0, 0, time.UTC)

	d := &Distributor{
		now: func() time.Time { return now },
	}

	t.Run("no log returns false", func(t *testing.T) {
		_, ok := d.previousReadcacheOwnerForPartition(1)
		assert.False(t, ok)
	})

	t.Run("returns the just-truncated owner", func(t *testing.T) {
		// Build a log with two leases for the same partition:
		//   - previous owner whose lease just ended at `now`
		//   - new owner whose lease starts at `now`
		entries := []readcacheassignment.LogEntry{
			{
				PartitionID: 1,
				InstanceID:  "rc-old",
				From:        now.Add(-10 * time.Minute),
				To:          now,
			},
			{
				PartitionID: 1,
				InstanceID:  "rc-new",
				From:        now,
				To:          now.Add(5 * time.Minute),
			},
		}
		d.readcacheLog.Store(readcacheassignment.NewLogFromEntries(entries))

		prev, ok := d.previousReadcacheOwnerForPartition(1)
		assert.True(t, ok)
		assert.Equal(t, "rc-old", prev)
	})

	t.Run("ignores current owner and unrelated partitions", func(t *testing.T) {
		entries := []readcacheassignment.LogEntry{
			{PartitionID: 1, InstanceID: "rc-new", From: now.Add(-1 * time.Minute), To: now.Add(5 * time.Minute)},
			{PartitionID: 2, InstanceID: "rc-other", From: now.Add(-1 * time.Minute), To: now.Add(5 * time.Minute)},
		}
		d.readcacheLog.Store(readcacheassignment.NewLogFromEntries(entries))

		_, ok := d.previousReadcacheOwnerForPartition(1)
		assert.False(t, ok, "should not return the current owner")
	})
}

// TestReadcacheHitTracker exercises the per-query bookkeeping that
// feeds the cortex_distributor_query_readcache_instances_hit_per_query
// histogram. The tracker has three properties the histogram relies on:
//
//   - Distinct instance IDs are counted; duplicates collapse. A query
//     that touches the same readcache pod across multiple partitions
//     must register as a single hit, otherwise the histogram would
//     over-report instance fan-out.
//   - Safe under concurrent calls. queryIngesterStream's per-replica
//     callbacks run on independent goroutines and may all decide to
//     record at once.
//   - nil-safe so callers can pass nil in code paths where the
//     histogram isn't wanted (e.g. future call sites that don't yet
//     thread a tracker through).
func TestReadcacheHitTracker(t *testing.T) {
	t.Parallel()

	t.Run("nil tracker is safe to call and counts zero", func(t *testing.T) {
		var tr *readcacheHitTracker
		tr.record("rc-1")
		assert.Equal(t, 0, tr.count())
	})

	t.Run("empty string is ignored", func(t *testing.T) {
		tr := newReadcacheHitTracker()
		tr.record("")
		assert.Equal(t, 0, tr.count())
	})

	t.Run("duplicate IDs collapse to a single hit", func(t *testing.T) {
		tr := newReadcacheHitTracker()
		tr.record("rc-1")
		tr.record("rc-1")
		tr.record("rc-2")
		tr.record("rc-1")
		assert.Equal(t, 2, tr.count())
	})

	t.Run("counts each distinct instance once under concurrent writers", func(t *testing.T) {
		tr := newReadcacheHitTracker()
		// 8 goroutines record an interleaved mix of 4 IDs. The
		// final count must be 4 regardless of scheduling: the
		// mutex serializes inserts and the map keys dedupe.
		var wg sync.WaitGroup
		const writers = 8
		ids := []string{"rc-a", "rc-b", "rc-c", "rc-d"}
		for i := 0; i < writers; i++ {
			wg.Add(1)
			go func(start int) {
				defer wg.Done()
				for j := 0; j < 64; j++ {
					tr.record(ids[(start+j)%len(ids)])
				}
			}(i)
		}
		wg.Wait()
		assert.Equal(t, len(ids), tr.count())
	})
}
