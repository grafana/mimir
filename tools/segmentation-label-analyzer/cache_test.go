// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFileCache_GetSet(t *testing.T) {
	cache, err := NewFileCache(true, t.TempDir())
	require.NoError(t, err)

	type testData struct {
		Name  string
		Value int
	}

	key := buildKey("test", "namespace", "tenant")

	// Cache miss.
	var result testData
	found, err := cache.Get(key, &result)
	require.NoError(t, err)
	require.False(t, found)

	// Set.
	input := testData{Name: "foo", Value: 42}
	require.NoError(t, cache.Set(key, input))

	// Cache hit.
	found, err = cache.Get(key, &result)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, input, result)
}

func TestFileCache_StreamReadWrite(t *testing.T) {
	cache, err := NewFileCache(true, t.TempDir())
	require.NoError(t, err)

	type testEntry struct {
		ID    int
		Query string
	}

	key := buildKey("test", "namespace", "tenant")

	// Cache miss.
	var results []testEntry
	hit := cache.StreamRead(key, func(line []byte) error {
		t.Fatal("should not be called on cache miss")
		return nil
	})
	require.False(t, hit)

	// Write entries.
	entries := []testEntry{
		{ID: 1, Query: "up"},
		{ID: 2, Query: "node_cpu_seconds_total"},
		{ID: 3, Query: "rate(http_requests_total[5m])"},
	}

	writer, err := cache.StreamWrite(key)
	require.NoError(t, err)
	require.NotNil(t, writer)

	for _, entry := range entries {
		require.NoError(t, writer.Write(entry))
	}
	require.NoError(t, writer.Close())

	// Cache hit - read entries back.
	hit = cache.StreamRead(key, func(line []byte) error {
		var entry testEntry
		require.NoError(t, json.Unmarshal(line, &entry))
		results = append(results, entry)
		return nil
	})
	require.True(t, hit)
	require.Equal(t, entries, results)
}
