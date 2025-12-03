// SPDX-License-Identifier: AGPL-3.0-only

package main

import "fmt"

// lengthBucket defines a histogram bucket for string lengths.
type lengthBucket struct {
	name string
	max  int // -1 means unbounded
}

// lengthBuckets defines the histogram buckets for string length distribution.
// Buckets are power-of-2 based.
var lengthBuckets = []lengthBucket{
	{"0-16", 16},
	{"17-32", 32},
	{"33-64", 64},
	{"65-128", 128},
	{"129-256", 256},
	{"257-512", 512},
	{"513-1K", 1024},
	{"1K-2K", 2048},
	{"2K-4K", 4096},
	{"4K-8K", 8192},
	{"8K-16K", 16384},
	{"16K-32K", 32768},
	{"32K+", -1},
}

// cardinalityBucket defines a histogram bucket for label cardinality.
type cardinalityBucket struct {
	name string
	max  int // -1 means unbounded
}

// cardinalityBuckets defines the histogram buckets for cardinality distribution.
var cardinalityBuckets = []cardinalityBucket{
	{"1", 1},
	{"2-10", 10},
	{"11-100", 100},
	{"101-1K", 1000},
	{"1K-10K", 10000},
	{"10K-100K", 100000},
	{"100K-1M", 1000000},
	{"1M+", -1},
}

// bytesToMB converts bytes to megabytes.
func bytesToMB(b int64) float64 {
	return float64(b) / (1024 * 1024)
}

// formatBytes formats a byte count as "N (X.XX MB)".
func formatBytes(b int64) string {
	return fmt.Sprintf("%d (%.2f MB)", b, bytesToMB(b))
}

// truncateString truncates a string to maxLen characters, adding "..." if truncated.
func truncateString(s string, maxLen int) string {
	if len(s) > maxLen {
		return s[:maxLen] + "..."
	}
	return s
}

// getLengthBucketName returns the bucket name for a given string length.
func getLengthBucketName(length int) string {
	for _, b := range lengthBuckets {
		if b.max == -1 || length <= b.max {
			return b.name
		}
	}
	return lengthBuckets[len(lengthBuckets)-1].name
}

// lengthBucketNames returns a slice of all length bucket names in order.
func lengthBucketNames() []string {
	names := make([]string, len(lengthBuckets))
	for i, b := range lengthBuckets {
		names[i] = b.name
	}
	return names
}

// getCardinalityBucketName returns the bucket name for a given cardinality.
func getCardinalityBucketName(cardinality int) string {
	for _, b := range cardinalityBuckets {
		if b.max == -1 || cardinality <= b.max {
			return b.name
		}
	}
	return cardinalityBuckets[len(cardinalityBuckets)-1].name
}

// cardinalityBucketNames returns a slice of all cardinality bucket names in order.
func cardinalityBucketNames() []string {
	names := make([]string, len(cardinalityBuckets))
	for i, b := range cardinalityBuckets {
		names[i] = b.name
	}
	return names
}
