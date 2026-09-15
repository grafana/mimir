// SPDX-License-Identifier: AGPL-3.0-only

package verify

import (
	"context"
	"testing"

	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
)

func TestSingleUTCDayVerifier_Header(t *testing.T) {
	const day = msPerDay
	const hour = int64(3_600_000)

	tests := []struct {
		name            string
		minTime         int64
		maxTime         int64
		wantErrContains string
	}{
		{"aligned_24h_day_0", 0, day, ""},
		{"aligned_24h_day_10", 10 * day, 11 * day, ""},
		{"aligned_24h_day_1", day, 2 * day, ""},
		{"sparse_same_day", 30 * 60 * 1000, 85_500_000, ""}, // 00:30 .. 23:45
		{"two_hour_not_crossing_midnight", 0, 2 * hour, ""},
		{"two_hour_crossing_midnight", day - hour, day + hour, "spans multiple UTC days"},
		{"twelve_hour_offset_block", 10*day + 12*hour, 11*day + 12*hour, "spans multiple UTC days"},
		{"one_ms_past_boundary", day - 1, day + 1, "spans multiple UTC days"},
		{"empty_range", 1000, 1000, "empty or inverted"},
		{"inverted_range", 2000, 1000, "empty or inverted"},
	}

	v := NewSingleUTCDayVerifier(log.NewNopLogger())
	ctx := context.Background()

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			meta := block.Meta{
				BlockMeta: tsdb.BlockMeta{
					ULID:    ulid.MustNew(uint64(tc.minTime+1), nil),
					MinTime: tc.minTime,
					MaxTime: tc.maxTime,
					Version: 1,
				},
			}
			err := v.Verify(ctx, "" /* blockDir unused */, meta)
			if tc.wantErrContains == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErrContains)
			}
		})
	}
}
