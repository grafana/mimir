// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/grafana/mimir/pkg/nautilus/readcacheassignment"
)

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
