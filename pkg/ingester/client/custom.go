// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ingester/client/custom.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package client

func ChunksCount(series []TimeSeriesChunk) int {
	if len(series) == 0 {
		return 0
	}

	count := 0
	for _, entry := range series {
		count += len(entry.Chunks)
	}
	return count
}

func ChunksSize(series []TimeSeriesChunk) int {
	if len(series) == 0 {
		return 0
	}

	size := 0
	for _, entry := range series {
		for _, chunk := range entry.Chunks {
			size += chunk.Size()
		}
	}
	return size
}
