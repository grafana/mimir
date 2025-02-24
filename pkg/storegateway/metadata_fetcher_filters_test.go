// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/storegateway/metadata_fetcher_filters_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package storegateway

import (
	"bytes"
	"context"
	"encoding/json"
	"path"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	promtest "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
	mimir_testutil "github.com/grafana/mimir/pkg/storage/tsdb/testutil"
	"github.com/grafana/mimir/pkg/util/extprom"
)

func TestIgnoreDeletionMarkFilter_Filter(t *testing.T) {
	testIgnoreDeletionMarkFilter(t, false)
}

func TestIgnoreDeletionMarkFilter_FilterWithBucketIndex(t *testing.T) {
	testIgnoreDeletionMarkFilter(t, true)
}

func testIgnoreDeletionMarkFilter(t *testing.T, bucketIndexEnabled bool) {
	const userID = "user-1"

	now := time.Now()
	ctx := context.Background()
	logger := log.NewNopLogger()

	// Create a bucket backed by filesystem.
	bkt, _ := mimir_testutil.PrepareFilesystemBucket(t)
	bkt = block.BucketWithGlobalMarkers(bkt)
	userBkt := bucket.NewUserBucketClient(userID, bkt, nil)

	shouldFetch := &block.DeletionMark{
		ID:           ulid.MustNew(1, nil),
		DeletionTime: now.Add(-15 * time.Hour).Unix(),
		Version:      1,
	}

	shouldIgnore := &block.DeletionMark{
		ID:           ulid.MustNew(2, nil),
		DeletionTime: now.Add(-60 * time.Hour).Unix(),
		Version:      1,
	}

	var buf bytes.Buffer
	require.NoError(t, json.NewEncoder(&buf).Encode(&shouldFetch))
	require.NoError(t, userBkt.Upload(ctx, path.Join(shouldFetch.ID.String(), block.DeletionMarkFilename), &buf))
	require.NoError(t, json.NewEncoder(&buf).Encode(&shouldIgnore))
	require.NoError(t, userBkt.Upload(ctx, path.Join(shouldIgnore.ID.String(), block.DeletionMarkFilename), &buf))
	require.NoError(t, userBkt.Upload(ctx, path.Join(ulid.MustNew(3, nil).String(), block.DeletionMarkFilename), bytes.NewBufferString("not a valid deletion-mark.json")))

	// Create the bucket index if required.
	var idx *bucketindex.Index
	if bucketIndexEnabled {
		var err error

		u := bucketindex.NewUpdater(bkt, userID, nil, 16, logger)
		idx, _, err = u.UpdateIndex(ctx, nil)
		require.NoError(t, err)
		require.NoError(t, bucketindex.WriteIndex(ctx, bkt, userID, nil, idx))
	}

	inputMetas := map[ulid.ULID]*block.Meta{
		ulid.MustNew(1, nil): {},
		ulid.MustNew(2, nil): {},
		ulid.MustNew(3, nil): {},
		ulid.MustNew(4, nil): {},
	}

	expectedMetas := map[ulid.ULID]*block.Meta{
		ulid.MustNew(1, nil): {},
		ulid.MustNew(3, nil): {},
		ulid.MustNew(4, nil): {},
	}

	expectedDeletionMarks := map[ulid.ULID]*block.DeletionMark{
		ulid.MustNew(1, nil): shouldFetch,
		ulid.MustNew(2, nil): shouldIgnore,
	}

	synced := extprom.NewTxGaugeVec(nil, prometheus.GaugeOpts{Name: "synced"}, []string{"state"})
	f := NewIgnoreDeletionMarkFilter(logger, objstore.WithNoopInstr(userBkt), 48*time.Hour, 32)

	if bucketIndexEnabled {
		require.NoError(t, f.FilterWithBucketIndex(ctx, inputMetas, idx, synced))
	} else {
		require.NoError(t, f.Filter(ctx, inputMetas, synced))
	}

	assert.Equal(t, 1.0, promtest.ToFloat64(synced.WithLabelValues(block.MarkedForDeletionMeta)))
	assert.Equal(t, expectedMetas, inputMetas)
	assert.Equal(t, expectedDeletionMarks, f.DeletionMarkBlocks())
}

func TestTimeMetaFilter(t *testing.T) {
	now := time.Now()
	limit := 10 * time.Minute
	limitTime := now.Add(-limit)

	ulid1 := ulid.MustNew(1, nil)
	ulid2 := ulid.MustNew(2, nil)
	ulid3 := ulid.MustNew(3, nil)
	ulid4 := ulid.MustNew(4, nil)

	inputMetas := map[ulid.ULID]*block.Meta{
		ulid1: {BlockMeta: tsdb.BlockMeta{MinTime: 100}},                                             // Very old, keep it
		ulid2: {BlockMeta: tsdb.BlockMeta{MinTime: timestamp.FromTime(now)}},                         // Fresh block, remove.
		ulid3: {BlockMeta: tsdb.BlockMeta{MinTime: timestamp.FromTime(limitTime.Add(time.Minute))}},  // Inside limit time, remove.
		ulid4: {BlockMeta: tsdb.BlockMeta{MinTime: timestamp.FromTime(limitTime.Add(-time.Minute))}}, // Before limit time, keep.
	}

	expectedMetas := map[ulid.ULID]*block.Meta{}
	expectedMetas[ulid1] = inputMetas[ulid1]
	expectedMetas[ulid4] = inputMetas[ulid4]

	synced := extprom.NewTxGaugeVec(nil, prometheus.GaugeOpts{Name: "synced"}, []string{"state"})

	// Test negative limit.
	f := newMinTimeMetaFilter(-10 * time.Minute)
	require.NoError(t, f.Filter(context.Background(), inputMetas, synced))
	assert.Equal(t, inputMetas, inputMetas)
	assert.Equal(t, 0.0, promtest.ToFloat64(synced.WithLabelValues(minTimeExcludedMeta)))

	f = newMinTimeMetaFilter(limit)
	require.NoError(t, f.Filter(context.Background(), inputMetas, synced))

	assert.Equal(t, expectedMetas, inputMetas)
	assert.Equal(t, 2.0, promtest.ToFloat64(synced.WithLabelValues(minTimeExcludedMeta)))
}
