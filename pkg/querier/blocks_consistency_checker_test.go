// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/blocks_consistency_checker_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querier

import (
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"

	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
	"github.com/grafana/mimir/pkg/util"
)

func TestBlocksConsistencyTracker_Check(t *testing.T) {
	now := time.Now()
	uploadGracePeriod := 10 * time.Minute

	block1 := ulid.MustNew(uint64(util.TimeToMillis(now.Add(-uploadGracePeriod*2))), nil)
	block2 := ulid.MustNew(uint64(util.TimeToMillis(now.Add(-uploadGracePeriod*3))), nil)
	block3 := ulid.MustNew(uint64(util.TimeToMillis(now.Add(-uploadGracePeriod*4))), nil)

	tests := map[string]struct {
		knownBlocks           bucketindex.Blocks
		queriedBlocks         [][]ulid.ULID
		expectedMissingBlocks []ulid.ULID
	}{
		"no known blocks": {
			knownBlocks:   bucketindex.Blocks{},
			queriedBlocks: [][]ulid.ULID{{}},
		},
		"all known blocks have been queried from a single store-gateway": {
			knownBlocks: bucketindex.Blocks{
				{ID: block1, UploadedAt: now.Add(-time.Hour).Unix()},
				{ID: block2, UploadedAt: now.Add(-time.Hour).Unix()},
			},
			queriedBlocks: [][]ulid.ULID{{block1, block2}},
		},
		"all known blocks have been queried from multiple store-gateway": {
			knownBlocks: bucketindex.Blocks{
				{ID: block1, UploadedAt: now.Add(-time.Hour).Unix()},
				{ID: block2, UploadedAt: now.Add(-time.Hour).Unix()},
			},
			queriedBlocks: [][]ulid.ULID{{block1, block2}},
		},
		"store-gateway has queried more blocks than expected": {
			knownBlocks: bucketindex.Blocks{
				{ID: block1, UploadedAt: now.Add(-time.Hour).Unix()},
				{ID: block2, UploadedAt: now.Add(-time.Hour).Unix()},
			},
			queriedBlocks: [][]ulid.ULID{{block1, block2, block3}},
		},
		"store-gateway has queried less blocks than expected": {
			knownBlocks: bucketindex.Blocks{
				{ID: block1, UploadedAt: now.Add(-time.Hour).Unix()},
				{ID: block2, UploadedAt: now.Add(-time.Hour).Unix()},
				{ID: block3, UploadedAt: now.Add(-time.Hour).Unix()},
			},
			queriedBlocks:         [][]ulid.ULID{{block1, block3}},
			expectedMissingBlocks: []ulid.ULID{block2},
		},
		"store-gateway has queried less blocks than expected, but the missing block has been recently uploaded": {
			knownBlocks: bucketindex.Blocks{
				{ID: block1, UploadedAt: now.Add(-time.Hour).Unix()},
				{ID: block2, UploadedAt: now.Add(-time.Hour).Unix()},
				{ID: block3, UploadedAt: now.Add(-uploadGracePeriod).Add(time.Minute).Unix()},
			},
			queriedBlocks: [][]ulid.ULID{{block1, block2}},
		},
		"blocks are queried in multiple attempts": {
			knownBlocks: bucketindex.Blocks{
				{ID: block1, UploadedAt: now.Add(-time.Hour).Unix()},
				{ID: block2, UploadedAt: now.Add(-time.Hour).Unix()},
			},
			queriedBlocks: [][]ulid.ULID{{block1}, {block2}},
		},
		"querying the same block again doesn't fail the consistency check": {
			knownBlocks: bucketindex.Blocks{
				{ID: block1, UploadedAt: now.Add(-time.Hour).Unix()},
				{ID: block2, UploadedAt: now.Add(-time.Hour).Unix()},
			},
			queriedBlocks: [][]ulid.ULID{{block1}, {block1, block2}},
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			reg := prometheus.NewPedanticRegistry()
			c := NewBlocksConsistency(uploadGracePeriod, reg)
			tracker := c.NewTracker(testData.knownBlocks, log.NewNopLogger())
			var missingBlocks []ulid.ULID
			for _, queriedBlocksAttempt := range testData.queriedBlocks {
				missingBlocks = tracker.Check(queriedBlocksAttempt)
			}
			tracker.Complete()

			assert.Equal(t, testData.expectedMissingBlocks, missingBlocks)
			assert.Equal(t, float64(1), testutil.ToFloat64(c.checksTotal))

			if len(testData.expectedMissingBlocks) > 0 {
				assert.Equal(t, float64(1), testutil.ToFloat64(c.checksFailed))
			} else {
				assert.Equal(t, float64(0), testutil.ToFloat64(c.checksFailed))
			}
		})
	}
}
