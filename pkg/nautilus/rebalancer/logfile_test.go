// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"encoding/json"
	"math"
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
	state, ok := f.readAssignmentLog()
	assert.False(t, ok)
	assert.Empty(t, state.Entries)

	now := time.Unix(1000, 0)
	want := []assignment.LogEntry{
		{
			TenantID:    "tenant-a",
			Range:       assignment.HashRange{Lo: 0, Hi: math.MaxUint32},
			PartitionID: 1,
			From:        now,
			To:          now.Add(time.Minute),
		},
		{
			TenantID:    "tenant-b",
			Range:       assignment.HashRange{Lo: 0, Hi: math.MaxUint32},
			PartitionID: 2,
			From:        now,
			To:          now.Add(time.Minute),
		},
	}
	validUntil := now.Add(2 * time.Minute)
	require.NoError(t, f.writeAssignmentLog(assignment.LogState{
		Entries:    want,
		Generation: 7,
		ValidUntil: validUntil,
	}))

	got, ok := f.readAssignmentLog()
	require.True(t, ok)
	require.Len(t, got.Entries, 2)
	assert.Equal(t, uint64(7), got.Generation)
	assert.Equal(t, validUntil, got.ValidUntil)
	for i := range want {
		assert.Equal(t, want[i].TenantID, got.Entries[i].TenantID)
		assert.Equal(t, want[i].Range, got.Entries[i].Range)
		assert.Equal(t, want[i].PartitionID, got.Entries[i].PartitionID)
		assert.True(t, want[i].From.Equal(got.Entries[i].From), "From mismatch at %d", i)
		assert.True(t, want[i].To.Equal(got.Entries[i].To), "To mismatch at %d", i)
	}
}

func TestLogFile_AssignmentVersionOneMigration(t *testing.T) {
	path := filepath.Join(t.TempDir(), assignmentLogFilename)
	now := time.Unix(1000, 0).UTC()
	legacy := logFileData{
		Version: logFileVersion,
		Entries: []assignment.LogEntry{{
			Range:       assignment.HashRange{Lo: 0, Hi: math.MaxUint32},
			PartitionID: 1,
			From:        now,
			To:          now.Add(time.Minute),
		}},
	}
	data, err := json.Marshal(legacy)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, data, 0o600))

	state, ok := newLogFile(path, log.NewNopLogger()).readAssignmentLog()
	require.True(t, ok)
	require.Len(t, state.Entries, 1)
	assert.Empty(t, state.Entries[0].TenantID)
	assert.Equal(t, uint64(1), state.Generation)
	assert.True(t, state.ValidUntil.IsZero())
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
	want := map[tenantRangeKey]time.Time{
		{hr: assignment.HashRange{Lo: 0, Hi: 999}}:                           deadline,
		{tenantID: "tenant-a", hr: assignment.HashRange{Lo: 1000, Hi: 1999}}: deadline.Add(time.Minute),
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
	move := map[tenantRangeKey]time.Time{{tenantID: "tenant-a", hr: assignment.HashRange{Lo: 0, Hi: 99}}: deadline}
	structural := map[tenantRangeKey]time.Time{{tenantID: "tenant-b", hr: assignment.HashRange{Lo: 100, Hi: 199}}: deadline.Add(time.Minute)}
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

	state, ok := f.readAssignmentLog()
	assert.False(t, ok)
	assert.Empty(t, state.Entries)
}

func TestLogStore_PersistOnApply(t *testing.T) {
	store := newLogStore()
	var persisted []assignment.LogState
	store.setPersistFn(func(state assignment.LogState) error {
		state.Entries = append([]assignment.LogEntry(nil), state.Entries...)
		persisted = append(persisted, state)
		return nil
	}, log.NewNopLogger())

	at := time.Unix(1000, 0)
	store.apply(at, assignment.EvenSplit([]int32{0, 1}), time.Minute, 10*time.Second, time.Hour)

	require.Len(t, persisted, 1, "first apply should trigger one persist")
	assert.NotEmpty(t, persisted[0].Entries)
	assert.Equal(t, uint64(1), persisted[0].Generation)

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
