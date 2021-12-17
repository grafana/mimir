package util

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestActivityTracker(t *testing.T) {
	file := filepath.Join(t.TempDir(), "activity")

	const maxEntries = 5
	tr, err := NewActivityTracker(file, 5, nil)
	require.NoError(t, err)

	defer func() {
		require.NoError(t, tr.Close())
	}()

	var insertedActivities []string
	for i := 0; i < maxEntries; i++ {
		a := fmt.Sprintf("activity-%d", i)
		aix := tr.InsertStatic(a)
		require.True(t, aix >= 0)
		insertedActivities = append(insertedActivities, a)
	}

	require.Equal(t, 0.0, testutil.ToFloat64(tr.failedInserts))

	aix := tr.InsertStatic("extra")
	require.True(t, aix < 0)

	require.Equal(t, 1.0, testutil.ToFloat64(tr.failedInserts))

	require.NoError(t, tr.flush())

	activities := LoadUnfinishedEntries(file)
	require.ElementsMatch(t, activities, insertedActivities)

	for i := 0; i < maxEntries; i++ {
		tr.Delete(i)
		require.NoError(t, tr.flush())

		insertedActivities = insertedActivities[1:]

		require.ElementsMatch(t, LoadUnfinishedEntries(file), insertedActivities)
	}
}

func TestNilActivityTracker(t *testing.T) {
	// Test that nil activity tracker doesn't cause panics.
	var tr *ActivityTracker = nil

	ix := tr.InsertStatic("test")
	tr.Delete(ix)

	require.NoError(t, tr.Close())
}

func BenchmarkName(b *testing.B) {
	file := filepath.Join(b.TempDir(), "activity")

	const maxEntries = 100
	tr, err := NewActivityTracker(file, maxEntries, nil)
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
