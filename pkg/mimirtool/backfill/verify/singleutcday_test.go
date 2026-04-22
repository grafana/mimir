// SPDX-License-Identifier: AGPL-3.0-only

package verify

import (
	"context"
	"testing"

	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunks"
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

	v := NewSingleUTCDayVerifier(log.NewNopLogger(), Medium)
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
			err := v.Verify(ctx, "" /* blockDir unused in Medium */, meta)
			if tc.wantErrContains == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErrContains)
			}
		})
	}
}

func TestSingleUTCDayVerifier_DeepSampleRange(t *testing.T) {
	ctx := context.Background()

	t.Run("all_samples_in_day_zero_pass", func(t *testing.T) {
		dir, meta := generateValidBlock(t, t.TempDir(), []chunks.Sample{
			sampleAt(1_000, 1.0),
			sampleAt(2_000, 2.0),
			sampleAt(3_000, 3.0),
		})
		// Override Meta to declare a header that sits inside day 0 so the header
		// check passes; deep check walks and confirms no chunks outside day 0.
		meta.MinTime = 0
		meta.MaxTime = msPerDay

		v := NewSingleUTCDayVerifier(log.NewNopLogger(), Deep)
		require.NoError(t, v.Verify(ctx, dir, *meta))
	})

	t.Run("samples_straddling_midnight_fail", func(t *testing.T) {
		// Samples: some just before midnight, some just after. Header is
		// overridden below to claim the block is in day 0 so the header check
		// passes; the deep walk should then flag the day 1 samples as
		// OutsideChunks against the UTC-day window.
		straddling := []chunks.Sample{
			sampleAt(msPerDay-2000, 1.0),
			sampleAt(msPerDay-1000, 2.0),
			sampleAt(msPerDay+1000, 3.0),
			sampleAt(msPerDay+2000, 4.0),
		}
		dir, meta := generateValidBlock(t, t.TempDir(), straddling)
		meta.MinTime = 0
		meta.MaxTime = msPerDay

		v := NewSingleUTCDayVerifier(log.NewNopLogger(), Deep)
		err := v.Verify(ctx, dir, *meta)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "chunks outside UTC day")
	})

	t.Run("medium_mode_does_not_walk_chunks", func(t *testing.T) {
		// Same straddling data — medium mode must pass because it only checks
		// the header arithmetic on the (overridden) Meta bounds.
		straddling := []chunks.Sample{
			sampleAt(msPerDay-2000, 1.0),
			sampleAt(msPerDay-1000, 2.0),
			sampleAt(msPerDay+1000, 3.0),
			sampleAt(msPerDay+2000, 4.0),
		}
		dir, meta := generateValidBlock(t, t.TempDir(), straddling)
		meta.MinTime = 0
		meta.MaxTime = msPerDay

		v := NewSingleUTCDayVerifier(log.NewNopLogger(), Medium)
		require.NoError(t, v.Verify(ctx, dir, *meta),
			"medium mode must NOT walk chunks; header-only check passes")
	})
}
