// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/nautilus/readcacheassignment"
)

// TestAdminState_ReadcacheHistory_FiltersNoOpRounds confirms that
// the round history only retains rounds with moves. A quiescent
// system can run for hours without producing any moves; if we kept
// every round, the 20-slot ring buffer would be flushed of useful
// signal during normal operation.
func TestAdminState_ReadcacheHistory_FiltersNoOpRounds(t *testing.T) {
	s := &adminState{}
	now := time.Unix(1000, 0)

	// A no-move round should NOT appear in history.
	s.setLastReadcachePlan(now, readcachePlan{
		Assignment:     &readcacheassignment.Assignment{Entries: []readcacheassignment.AssignmentEntry{{PartitionID: 0, InstanceID: "rc-a"}}},
		LoadByInstance: map[string]float64{"rc-a": 100},
		Moves:          nil,
	}, map[int32]string{0: "rc-a"})

	hist := s.snapshotReadcacheHistory()
	assert.Empty(t, hist, "no-move round must not appear in history")

	// A round with moves should appear.
	s.setLastReadcachePlan(now.Add(time.Second), readcachePlan{
		Assignment:     &readcacheassignment.Assignment{Entries: []readcacheassignment.AssignmentEntry{{PartitionID: 0, InstanceID: "rc-b"}}},
		LoadByInstance: map[string]float64{"rc-a": 50, "rc-b": 50},
		Moves: []readcacheMove{
			{PartitionID: 0, From: "rc-a", To: "rc-b", Load: 50, Reason: "src rc-a over target"},
		},
	}, map[int32]string{0: "rc-a"})

	hist = s.snapshotReadcacheHistory()
	require.Len(t, hist, 1)
	assert.Equal(t, "src rc-a over target", hist[0].Moves[0].Reason)
	assert.Equal(t, "rc-a", hist[0].PerPartition[0].CurrentOwner)
	assert.Equal(t, "rc-b", hist[0].PerPartition[0].PlannedOwner)
}

// TestAdminState_ReadcacheHistory_NewestFirst confirms snapshot
// returns rounds in display order (newest first), matching how the
// HTML template iterates them.
func TestAdminState_ReadcacheHistory_NewestFirst(t *testing.T) {
	s := &adminState{}
	for i := 0; i < 5; i++ {
		s.setLastReadcachePlan(
			time.Unix(int64(1000+i), 0),
			readcachePlan{
				Assignment:     &readcacheassignment.Assignment{},
				LoadByInstance: map[string]float64{},
				Moves: []readcacheMove{
					{PartitionID: int32(i), From: "rc-a", To: "rc-b", Reason: "test"},
				},
			},
			map[int32]string{},
		)
	}
	hist := s.snapshotReadcacheHistory()
	require.Len(t, hist, 5)
	for i, r := range hist {
		// Newest first: i=0 is the most recently appended round,
		// which we wrote with PartitionID=4.
		expected := int32(4 - i)
		assert.Equal(t, expected, r.Moves[0].PartitionID, "history index %d should hold round for partition %d", i, expected)
	}
}

// TestAdminState_ReadcacheHistory_RingBufferCap exercises the
// maxRoundLogs bound: the buffer must drop the oldest entries, not
// reject new ones, so an operator always sees the most recent
// move activity.
func TestAdminState_ReadcacheHistory_RingBufferCap(t *testing.T) {
	s := &adminState{}
	for i := 0; i < maxRoundLogs+5; i++ {
		s.setLastReadcachePlan(
			time.Unix(int64(1000+i), 0),
			readcachePlan{
				Assignment:     &readcacheassignment.Assignment{},
				LoadByInstance: map[string]float64{},
				Moves: []readcacheMove{
					{PartitionID: int32(i), From: "rc-a", To: "rc-b", Reason: "test"},
				},
			},
			map[int32]string{},
		)
	}
	hist := s.snapshotReadcacheHistory()
	require.Len(t, hist, maxRoundLogs)
	// Newest entry should be the last we wrote.
	assert.Equal(t, int32(maxRoundLogs+4), hist[0].Moves[0].PartitionID)
	// Oldest retained entry should be 5 rounds after the very first
	// (we wrote 25 total, kept the last 20 → kept rounds 5..24).
	assert.Equal(t, int32(5), hist[len(hist)-1].Moves[0].PartitionID)
}

// TestAdminState_LastStats_TracksPartitionRate verifies that the
// per-partition samples-per-second EWMA is captured into adminState
// alongside the legacy series count, which is what powers the
// "Imbalance (rate)" stat card on the dashboard.
func TestAdminState_LastStats_TracksPartitionRate(t *testing.T) {
	s := &adminState{}
	lm := &loadMap{
		series:     map[partitionRangeKey]int64{},
		sampleRate: map[partitionRangeKey]float64{},
	}
	partitionL := map[int32]int64{0: 100, 1: 200}
	partitionRate := map[int32]float64{0: 50.5, 1: 150.5}

	s.setLastStats(lm, partitionL, partitionRate, nil, []int32{0, 1})
	_, _, _, rates, _ := s.snapshot()
	assert.Equal(t, 50.5, rates[0])
	assert.Equal(t, 150.5, rates[1])
}

// TestAdminHTML_RendersRecentReadcacheRoundsHistory drives the
// admin page through a few synthetic readcache slicer rounds and
// asserts the resulting HTML contains the new history section,
// each move's reason, and the per-instance load table.
func TestAdminHTML_RendersRecentReadcacheRoundsHistory(t *testing.T) {
	r := newTestRebalancerForReset(t, 4, []string{"rc-a", "rc-b"})

	// Stage a couple of move-bearing rounds into the admin state.
	r.admin.setLastReadcachePlan(time.Unix(1000, 0), readcachePlan{
		Assignment: &readcacheassignment.Assignment{
			Entries: []readcacheassignment.AssignmentEntry{
				{PartitionID: 0, InstanceID: "rc-a"},
				{PartitionID: 1, InstanceID: "rc-b"},
			},
		},
		LoadByInstance: map[string]float64{"rc-a": 100, "rc-b": 100},
		Moves: []readcacheMove{
			{PartitionID: 2, From: "rc-a", To: "rc-b", Load: 50, Reason: "src rc-a over target by 100 (load=300, target=200); dst rc-b at 50; partition load=50"},
		},
	}, map[int32]string{2: "rc-a"})
	r.admin.setLastReadcachePlan(time.Unix(2000, 0), readcachePlan{
		Assignment: &readcacheassignment.Assignment{
			Entries: []readcacheassignment.AssignmentEntry{
				{PartitionID: 0, InstanceID: "rc-a"},
				{PartitionID: 1, InstanceID: "rc-b"},
			},
		},
		LoadByInstance: map[string]float64{"rc-a": 75, "rc-b": 125},
		Moves: []readcacheMove{
			{PartitionID: 3, From: "rc-b", To: "rc-a", Load: 25, Reason: "src rc-b over target"},
		},
	}, map[int32]string{3: "rc-b"})

	rec := httptest.NewRecorder()
	r.serveAdminHTML(rec)
	body := rec.Body.String()

	assert.Contains(t, body, "Recent Readcache Slicer Rounds", "history section header must render")
	assert.Contains(t, body, "P2 rc-a\u2192rc-b", "move arrow for round 1 must render (→ between src and dst)")
	assert.Contains(t, body, "P3 rc-b\u2192rc-a", "move arrow for round 2 must render")
	assert.Contains(t, body, "src rc-a over target by 100", "move reason text must render")
	assert.Contains(t, body, "Per-instance load", "per-instance load table label must render")
	assert.Contains(t, body, "Pod-to-partition assignments", "pod-to-partition table label must render")
	// Newest first: the second round (rc-b→rc-a) should appear in
	// the body BEFORE the first round (rc-a→rc-b).
	idx1 := strings.Index(body, "P3 rc-b\u2192rc-a")
	idx2 := strings.Index(body, "P2 rc-a\u2192rc-b")
	require.True(t, idx1 >= 0 && idx2 >= 0 && idx1 < idx2, "newest round must render first (idx1=%d, idx2=%d)", idx1, idx2)
}

// TestAdminHTML_RendersBothImbalanceStats confirms the two
// imbalance cards (series and rate) both render on the dashboard,
// including the supporting Mean rate / Max rate cards. The
// rebalancer's partition-source path needs a live ring to populate
// the partition list, so we render the template directly with a
// hand-built page-data fixture instead of going through buildAdminPageData.
// This pins the public-facing labels so future template edits can't
// silently drop one of the two figures the user asked for.
func TestAdminHTML_RendersBothImbalanceStats(t *testing.T) {
	var buf bytesBuffer
	data := adminPageData{
		GeneratedAt:        "2026-05-20T00:00:00Z",
		TotalMemorySeries:  100,
		AboveAverage:       20,
		MeanL:              50,
		MaxL:               80,
		MinL:               20,
		ImbalanceRatio:     1.6,
		MeanRate:           1200,
		MaxRate:            2400,
		MinRate:            600,
		RateImbalance:      2.0,
		NumPartitions:      2,
		NumEntries:         4,
		MovedFraction:      0.05,
		CompactionInterval: 2 * time.Hour,
		Partitions: []partitionView{
			{PartitionID: 0, MemorySeries: 80, SampleRate: 2400},
			{PartitionID: 1, MemorySeries: 20, SampleRate: 600},
		},
		HeatmapData: "[]",
	}
	err := adminTemplate.Execute(&buf, data)
	require.NoError(t, err)
	body := buf.String()

	// Two distinct imbalance labels: cardinality (series) and ingest
	// throughput (rate). Both must be visible at all times so an
	// operator can tell at a glance which axis is hot.
	assert.Contains(t, body, "Imbalance (series)", "must label the cardinality imbalance card")
	assert.Contains(t, body, "Imbalance (rate)", "must label the rate imbalance card")
	// Supporting absolute values for the rate axis.
	assert.Contains(t, body, "Mean rate", "must show absolute mean samples/s")
	assert.Contains(t, body, "Max rate", "must show absolute max samples/s")
	// Per-partition rate must surface on the partition row too,
	// so /s appears at least once via the partition stats line
	// alongside the summary cards. We use a substring rather than
	// a fixed format so future formatter tweaks don't break this.
	assert.Contains(t, body, "/s")
}

// bytesBuffer is a tiny adapter so tests don't need to import
// bytes for the only use here. It keeps the test self-contained
// with the rest of the assertions.
type bytesBuffer struct {
	b []byte
}

func (b *bytesBuffer) Write(p []byte) (int, error) {
	b.b = append(b.b, p...)
	return len(p), nil
}

func (b *bytesBuffer) String() string { return string(b.b) }
