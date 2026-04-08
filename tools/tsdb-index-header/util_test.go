// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBytesToMB(t *testing.T) {
	tests := []struct {
		name     string
		bytes    int64
		expected float64
	}{
		{"zero", 0, 0},
		{"one MB", 1024 * 1024, 1.0},
		{"half MB", 512 * 1024, 0.5},
		{"100 MB", 100 * 1024 * 1024, 100.0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := bytesToMB(tc.bytes)
			assert.InDelta(t, tc.expected, result, 0.001)
		})
	}
}

func TestFormatBytes(t *testing.T) {
	tests := []struct {
		name     string
		bytes    int64
		expected string
	}{
		{"zero", 0, "0 (0.00 MB)"},
		{"one MB", 1024 * 1024, "1048576 (1.00 MB)"},
		{"small", 1000, "1000 (0.00 MB)"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := formatBytes(tc.bytes)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestTruncateString(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		maxLen   int
		expected string
	}{
		{"shorter than max", "hello", 10, "hello"},
		{"equal to max", "hello", 5, "hello"},
		{"longer than max", "hello world", 5, "hello..."},
		{"empty string", "", 10, ""},
		{"max zero", "hello", 0, "..."},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := truncateString(tc.input, tc.maxLen)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestGetLengthBucketName(t *testing.T) {
	tests := []struct {
		name     string
		length   int
		expected string
	}{
		{"zero", 0, "0-16"},
		{"small", 10, "0-16"},
		{"boundary 16", 16, "0-16"},
		{"boundary 17", 17, "17-32"},
		{"medium", 100, "65-128"},
		{"large", 1000, "513-1K"},
		{"very large", 50000, "32K+"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := getLengthBucketName(tc.length)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestLengthBucketNames(t *testing.T) {
	names := lengthBucketNames()

	assert.Len(t, names, len(lengthBuckets))
	assert.Equal(t, "0-16", names[0])
	assert.Equal(t, "32K+", names[len(names)-1])
}

func TestGetCardinalityBucketName(t *testing.T) {
	tests := []struct {
		name        string
		cardinality int
		expected    string
	}{
		{"one", 1, "1"},
		{"small", 5, "2-10"},
		{"boundary 10", 10, "2-10"},
		{"boundary 11", 11, "11-100"},
		{"medium", 500, "101-1K"},
		{"large", 50000, "10K-100K"},
		{"very large", 2000000, "1M+"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := getCardinalityBucketName(tc.cardinality)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestCardinalityBucketNames(t *testing.T) {
	names := cardinalityBucketNames()

	assert.Len(t, names, len(cardinalityBuckets))
	assert.Equal(t, "1", names[0])
	assert.Equal(t, "1M+", names[len(names)-1])
}

func TestGetChunkCountBucketName(t *testing.T) {
	tests := []struct {
		name     string
		count    int
		expected string
	}{
		{"one", 1, "1"},
		{"boundary 2", 2, "2-10"},
		{"boundary 10", 10, "2-10"},
		{"boundary 11", 11, "11-50"},
		{"medium", 100, "51-100"},
		{"large", 500, "101-500"},
		{"boundary 1000", 1000, "501-1K"},
		{"boundary 5000", 5000, "1K-5K"},
		{"boundary 10000", 10000, "5K-10K"},
		{"very large", 50000, "10K+"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := getChunkCountBucketName(tc.count)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestChunkCountBucketNames(t *testing.T) {
	names := chunkCountBucketNames()

	assert.Len(t, names, len(chunkCountBuckets))
	assert.Equal(t, "1", names[0])
	assert.Equal(t, "10K+", names[len(names)-1])
}
