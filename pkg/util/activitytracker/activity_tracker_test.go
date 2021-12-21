// SPDX-License-Identifier: AGPL-3.0-only

package activitytracker

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestActivityTracker(t *testing.T) {
	file := filepath.Join(t.TempDir(), "activity")

	const maxEntries = 5
	tr, err := NewActivityTracker(Config{Filepath: file, MaxEntries: maxEntries}, nil)
	require.NoError(t, err)

	defer func() {
		require.NoError(t, tr.Close())
	}()

	now := time.Now()

	var insertedActivities []string
	for i := 0; i < maxEntries; i++ {
		a := fmt.Sprintf("activity-%d", i)
		aix := tr.InsertStatic(a)
		require.True(t, aix >= 0 && aix < maxEntries)
		insertedActivities = append(insertedActivities, a)
		require.Equal(t, float64(maxEntries-i)-1, testutil.ToFloat64(tr.freeActivityEntries))
	}

	checkActivitiesInFile(t, file, insertedActivities, now, 5*time.Second)

	// Test inserting another activity, that doesn't fit anymore.
	{
		require.Equal(t, 0.0, testutil.ToFloat64(tr.failedInserts.WithLabelValues(reasonFull)))
		aix := tr.InsertStatic("extra")
		require.True(t, aix < 0)
		require.Equal(t, 1.0, testutil.ToFloat64(tr.failedInserts.WithLabelValues(reasonFull)))
	}

	// Test delete and verify that other entries are still there.
	for i := 0; i < maxEntries; i++ {
		tr.Delete(i)

		insertedActivities = insertedActivities[1:]
		checkActivitiesInFile(t, file, insertedActivities, now, 5*time.Second)
		require.Equal(t, float64(i+1), testutil.ToFloat64(tr.freeActivityEntries))
	}

	// Test inserting empty activity
	{
		require.Equal(t, 0.0, testutil.ToFloat64(tr.failedInserts.WithLabelValues(reasonEmptyActivity)))
		aix := tr.InsertStatic("")
		require.True(t, aix < 0)
		require.Equal(t, 1.0, testutil.ToFloat64(tr.failedInserts.WithLabelValues(reasonEmptyActivity)))
	}
}

func checkActivitiesInFile(t *testing.T, file string, expectedActivities []string, referenceTime time.Time, timeDelta time.Duration) {
	entries, err := LoadUnfinishedEntries(file)
	require.NoError(t, err)

	var activities []string
	for ix := range entries {
		assert.WithinDuration(t, referenceTime, entries[ix].Timestamp, timeDelta)
		activities = append(activities, entries[ix].Activity)
	}

	assert.ElementsMatch(t, expectedActivities, activities)
}

func TestNilActivityTracker(t *testing.T) {
	// Test that nil activity tracker doesn't cause panics.
	var tr *ActivityTracker = nil

	ix := tr.InsertStatic("test")
	tr.Delete(ix)

	require.NoError(t, tr.Close())
}

func BenchmarkActivityTracker(b *testing.B) {
	file := filepath.Join(b.TempDir(), "activity")

	const maxEntries = 100
	tr, err := NewActivityTracker(Config{Filepath: file, MaxEntries: maxEntries}, nil)
	require.NoError(b, err)

	defer func() {
		require.NoError(b, tr.Close())
	}()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		ix := tr.InsertStatic("test")
		tr.Delete(ix)
	}
}

func TestTrimEntrySize(t *testing.T) {
	for _, tc := range []struct {
		input    string
		size     int
		expected string
	}{
		{"hello world", 100, "hello world"},
		{"hello world", 10, "hello worl"},
		{"hello world", 5, "hello"},
		{"\U0001f389\U0001f384\U0001f385", 5, "\U0001f389"},
		{"\U0001f389\U0001f384\U0001f385", 8, "\U0001f389\U0001f384"},
		{"\U0001f389\U0001f384\U0001f385", 9, "\U0001f389\U0001f384"},
		{"\U0001f389\U0001f384\U0001f385", 10, "\U0001f389\U0001f384"},
		{"\U0001f389\U0001f384\U0001f385", 11, "\U0001f389\U0001f384"},
		{"\U0001f389\U0001f384\U0001f385", 12, "\U0001f389\U0001f384\U0001f385"},
	} {
		t.Run(fmt.Sprintf("%s, %d", tc.input, tc.size), func(t *testing.T) {
			assert.Equal(t, tc.expected, trimEntryToSize(tc.input, tc.size))
		})
	}
}
