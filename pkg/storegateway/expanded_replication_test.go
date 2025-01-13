// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
)

func TestMaxTimeExpandedReplication(t *testing.T) {
	// Round "now" to the nearest millisecond since we are using millisecond precision
	// for min/max times for the blocks.
	now := time.Now().Round(time.Millisecond)
	replication := NewMaxTimeExpandedReplication(25*time.Hour, 45*time.Minute)
	replication.now = func() time.Time { return now }

	type testCase struct {
		block         bucketindex.Block
		expectedSync  bool
		expectedQuery bool
	}

	testCases := map[string]testCase{
		"max time eligible": {
			block: bucketindex.Block{
				MinTime: now.Add(-24 * time.Hour).UnixMilli(),
				MaxTime: now.Add(-12 * time.Hour).UnixMilli(),
			},
			expectedSync:  true,
			expectedQuery: true,
		},
		"max time on boundary": {
			block: bucketindex.Block{
				MinTime: now.Add(-49 * time.Hour).UnixMilli(),
				MaxTime: now.Add(-25 * time.Hour).UnixMilli(),
			},
			expectedSync:  true,
			expectedQuery: false,
		},
		"max time on boundary including grace period": {
			block: bucketindex.Block{
				MinTime: now.Add(-49 * time.Hour).UnixMilli(),
				MaxTime: now.Add(-(24*time.Hour + 15*time.Minute)).UnixMilli(),
			},
			expectedSync:  true,
			expectedQuery: true,
		},
		"max time too old": {
			block: bucketindex.Block{
				MinTime: now.Add(-72 * time.Hour).UnixMilli(),
				MaxTime: now.Add(-48 * time.Hour).UnixMilli(),
			},
			expectedSync:  false,
			expectedQuery: false,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			canSync := replication.EligibleForSync(&tc.block)
			canQuery := replication.EligibleForQuerying(&tc.block)

			require.Equal(t, tc.expectedSync, canSync, "expected to be able/not-able to sync block %+v using %+v", tc.block, replication)
			require.Equal(t, tc.expectedQuery, canQuery, "expected to be able/not-able to query block %+v using %+v", tc.block, replication)
		})
	}
}
