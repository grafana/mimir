// SPDX-License-Identifier: AGPL-3.0-only

package usagetracker

import (
	"context"
	"time"

	"github.com/go-kit/log"
)

// ToolLimiterMock is a limiter implementation for tools.
type ToolLimiterMock map[string]uint64

func (l ToolLimiterMock) localSeriesLimit(userID string) uint64 { return l[userID] }

func (l ToolLimiterMock) zonesCount() uint64 { return 2 }

// ToolNoopEvents is an events implementation for tools.
type ToolNoopEvents struct{}

func (n ToolNoopEvents) publishCreatedSeries(_ context.Context, _ string, _ []uint64, _ time.Time) error {
	return nil
}

// LoadSnapshotsForTool loads snapshots from a SnapshotFile into a new trackerStore.
// It creates a new trackerStore, loads all shard snapshots, and returns any error encountered.
func LoadSnapshotsForTool(snapshotData [][]byte, idleTimeout time.Duration, userCloseToLimitPercentageThreshold int, logger log.Logger, now time.Time) error {
	lim := ToolLimiterMock{}
	ev := ToolNoopEvents{}
	t := newTrackerStore(idleTimeout, userCloseToLimitPercentageThreshold, logger, lim, ev)

	for _, d := range snapshotData {
		if err := t.loadSnapshot(d, now, true); err != nil {
			return err
		}
	}
	return nil
}
