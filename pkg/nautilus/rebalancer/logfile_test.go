// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/nautilus/assignment"
	"github.com/grafana/mimir/pkg/nautilus/readcacheassignment"
)

func TestLogFile_AssignmentRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, assignmentLogFilename)
	f := newLogFile(path, log.NewNopLogger())

	// Cold start: read before any write.
	entries, ok := f.readAssignmentLog()
	assert.False(t, ok)
	assert.Empty(t, entries)

	now := time.Unix(1000, 0)
	want := []assignment.LogEntry{
		{
			Range:       assignment.HashRange{Lo: 0, Hi: 999},
			PartitionID: 0,
			From:        now,
			To:          now.Add(time.Minute),
		},
		{
			Range:       assignment.HashRange{Lo: 1000, Hi: 1999},
			PartitionID: 1,
			From:        now,
			To:          now.Add(time.Minute),
		},
	}
	require.NoError(t, f.writeAssignmentLog(want))

	got, ok := f.readAssignmentLog()
	require.True(t, ok)
	require.Len(t, got, 2)
	for i := range want {
		assert.Equal(t, want[i].Range, got[i].Range)
		assert.Equal(t, want[i].PartitionID, got[i].PartitionID)
		assert.True(t, want[i].From.Equal(got[i].From), "From mismatch at %d", i)
		assert.True(t, want[i].To.Equal(got[i].To), "To mismatch at %d", i)
	}
}

func TestLogFile_ReadcacheRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, readcacheLogFilename)
	f := newLogFile(path, log.NewNopLogger())

	entries, ok := f.readReadcacheLog()
	assert.False(t, ok)
	assert.Empty(t, entries)

	now := time.Unix(1000, 0)
	want := []readcacheassignment.LogEntry{
		{PartitionID: 0, InstanceID: "rc-a", From: now, To: now.Add(time.Minute)},
		{PartitionID: 1, InstanceID: "rc-b", From: now, To: now.Add(time.Minute)},
	}
	require.NoError(t, f.writeReadcacheLog(want))

	got, ok := f.readReadcacheLog()
	require.True(t, ok)
	require.Len(t, got, 2)
	for i := range want {
		assert.Equal(t, want[i].PartitionID, got[i].PartitionID)
		assert.Equal(t, want[i].InstanceID, got[i].InstanceID)
		assert.True(t, want[i].From.Equal(got[i].From))
		assert.True(t, want[i].To.Equal(got[i].To))
	}
}

func TestLogFile_MoveCooldownsRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, moveCooldownsFilename)
	f := newLogFile(path, log.NewNopLogger())

	// Cold start: read before any write.
	cooldowns, ok := f.readMoveCooldowns()
	assert.False(t, ok)
	assert.Empty(t, cooldowns)

	// Round trip through the same wire encoding the rebalancer uses.
	deadline := time.Unix(2000, 0).UTC()
	want := map[assignment.HashRange]time.Time{
		{Lo: 0, Hi: 999}:     deadline,
		{Lo: 1000, Hi: 1999}: deadline.Add(time.Minute),
	}
	require.NoError(t, f.writeMoveCooldowns(cooldownsToWire(want)))

	wire, ok := f.readMoveCooldowns()
	require.True(t, ok)
	got := cooldownsFromWire(wire)
	require.Len(t, got, 2)
	for hr, wantDeadline := range want {
		assert.True(t, wantDeadline.Equal(got[hr]), "deadline mismatch for %v", hr)
	}
}

func TestLogFile_StabilizationStateRoundTrip(t *testing.T) {
	f := newLogFile(filepath.Join(t.TempDir(), moveCooldownsFilename), log.NewNopLogger())
	deadline := time.Unix(2000, 0).UTC()
	move := map[assignment.HashRange]time.Time{{Lo: 0, Hi: 99}: deadline}
	structural := map[assignment.HashRange]time.Time{{Lo: 100, Hi: 199}: deadline.Add(time.Minute)}
	roles := partitionRoleCooldowns{
		recentSources:      map[int32]time.Time{1: deadline.Add(2 * time.Minute)},
		recentDestinations: map[int32]time.Time{2: deadline.Add(3 * time.Minute)},
	}

	require.NoError(t, f.writeCooldownState(cooldownsToWire(move), cooldownsToWire(structural), roles))
	state, ok := f.readCooldownState()
	require.True(t, ok)

	assert.Equal(t, move, cooldownsFromWire(state.MoveCooldowns))
	assert.Equal(t, structural, cooldownsFromWire(state.StructuralCooldowns))
	assert.Equal(t, roles.recentSources, state.RecentSourcePartitions)
	assert.Equal(t, roles.recentDestinations, state.RecentDestinationPartitions)
}

func TestLogFile_CorruptionFallsBackToColdStart(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, assignmentLogFilename)
	f := newLogFile(path, log.NewNopLogger())

	require.NoError(t, os.WriteFile(path, []byte("not json"), 0o644))

	entries, ok := f.readAssignmentLog()
	assert.False(t, ok)
	assert.Empty(t, entries)
}

func TestLogStore_PersistOnApply(t *testing.T) {
	store := newLogStore()
	var persisted [][]assignment.LogEntry
	store.setPersistFn(func(entries []assignment.LogEntry) error {
		persisted = append(persisted, append([]assignment.LogEntry(nil), entries...))
		return nil
	}, log.NewNopLogger())

	at := time.Unix(1000, 0)
	store.apply(at, assignment.EvenSplit([]int32{0, 1}), time.Minute, 10*time.Second, time.Hour)

	require.Len(t, persisted, 1, "first apply should trigger one persist")
	assert.NotEmpty(t, persisted[0])

	// Steady-state apply (same target) should not persist again.
	store.apply(at, assignment.EvenSplit([]int32{0, 1}), time.Minute, 10*time.Second, time.Hour)
	assert.Len(t, persisted, 1, "no-op apply should not persist")
}

func TestReadcacheLogStore_PersistOnApply(t *testing.T) {
	store := newReadcacheLogStore()
	var persisted [][]readcacheassignment.LogEntry
	store.setPersistFn(func(entries []readcacheassignment.LogEntry) error {
		persisted = append(persisted, append([]readcacheassignment.LogEntry(nil), entries...))
		return nil
	}, log.NewNopLogger())

	at := time.Unix(1000, 0)
	store.apply(at, &readcacheassignment.Assignment{
		Entries: []readcacheassignment.AssignmentEntry{
			{PartitionID: 0, InstanceID: "rc-a"},
			{PartitionID: 1, InstanceID: "rc-b"},
		},
	}, time.Minute, 10*time.Second, time.Hour, 0)

	require.Len(t, persisted, 1)
	assert.NotEmpty(t, persisted[0])

	// Steady state.
	store.apply(at, &readcacheassignment.Assignment{
		Entries: []readcacheassignment.AssignmentEntry{
			{PartitionID: 0, InstanceID: "rc-a"},
			{PartitionID: 1, InstanceID: "rc-b"},
		},
	}, time.Minute, 10*time.Second, time.Hour, 0)
	assert.Len(t, persisted, 1)
}
