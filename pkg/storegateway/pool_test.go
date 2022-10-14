// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/storegateway/chunk_bytes_pool_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package storegateway

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
)

func TestChunkBytesPool_Get(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	p, err := newChunkBytesPool(mimir_tsdb.ChunkPoolDefaultMinBucketSize, mimir_tsdb.ChunkPoolDefaultMaxBucketSize, 0, reg)
	require.NoError(t, err)

	_, err = p.Get(mimir_tsdb.EstimatedMaxChunkSize - 1)
	require.NoError(t, err)

	_, err = p.Get(mimir_tsdb.EstimatedMaxChunkSize + 1)
	require.NoError(t, err)

	assert.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(fmt.Sprintf(`
		# HELP cortex_bucket_store_chunk_pool_requested_bytes_total Total bytes requested to chunk bytes pool.
		# TYPE cortex_bucket_store_chunk_pool_requested_bytes_total counter
		cortex_bucket_store_chunk_pool_requested_bytes_total %d

		# HELP cortex_bucket_store_chunk_pool_returned_bytes_total Total bytes returned by the chunk bytes pool.
		# TYPE cortex_bucket_store_chunk_pool_returned_bytes_total counter
		cortex_bucket_store_chunk_pool_returned_bytes_total %d
	`, mimir_tsdb.EstimatedMaxChunkSize*2, mimir_tsdb.EstimatedMaxChunkSize*3))))
}
