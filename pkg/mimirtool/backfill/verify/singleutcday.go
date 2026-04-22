// SPDX-License-Identifier: AGPL-3.0-only

package verify

import (
	"context"
	"fmt"

	"github.com/go-kit/log"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
)

// msPerDay is the number of milliseconds in one UTC day. All TSDB block
// MinTime/MaxTime values are milliseconds since Unix epoch UTC.
const msPerDay int64 = 24 * 60 * 60 * 1000 // 86_400_000

// SingleUTCDayVerifier enforces that a block's [MinTime, MaxTime) range lies
// entirely within a single calendar UTC day. It accepts sparse blocks whose
// span is less than 24 hours, and rejects 2-hour Prometheus-default blocks
// and any block crossing a UTC-midnight boundary.
//
// Header check (always): floor(MinTime / msPerDay) == floor((MaxTime-1) / msPerDay).
// MaxTime is exclusive (see pkg/storage/tsdb/block/block_generator.go:173
// "MaxTime: specs.MaxTime() + 1, // Not included.").
//
// Deep check (when constructed with Deep mode): delegates to block.VerifyBlock
// with (minTime=utcDayStart, maxTime=utcDayEnd, checkChunks=false) so any
// chunk outside the UTC-day window is flagged via HealthStats.OutsideChunks.
// checkChunks is false here because WellFormedVerifier already performs the
// CRC32 walk in deep mode; running it twice would double deep-mode IO cost.
// This costs ~1 extra postings walk per block in deep mode — acceptable for
// v1. If benchmarks ever need to collapse the two walks, a future plan can
// introduce a per-block HealthStats cache shared between verifiers.
type SingleUTCDayVerifier struct {
	logger log.Logger
	deep   bool
}

// NewSingleUTCDayVerifier constructs a SingleUTCDayVerifier configured for
// the given Mode. Deep mode enables the extra postings-walk check for chunks
// straddling a UTC-day boundary; Medium mode does only the header arithmetic.
func NewSingleUTCDayVerifier(logger log.Logger, mode Mode) *SingleUTCDayVerifier {
	return &SingleUTCDayVerifier{logger: logger, deep: mode == Deep}
}

// Name returns the stable check name used in log lines and Report entries.
func (v *SingleUTCDayVerifier) Name() string { return "single-utc-day" }

// Verify returns nil if the block at blockDir is confined to a single
// calendar UTC day. See the type comment for the exact formula and the deep
// vs medium behavior split.
func (v *SingleUTCDayVerifier) Verify(ctx context.Context, blockDir string, meta block.Meta) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if meta.MinTime >= meta.MaxTime {
		return fmt.Errorf("block time range is empty or inverted: MinTime=%d MaxTime=%d",
			meta.MinTime, meta.MaxTime)
	}

	startDay := meta.MinTime / msPerDay
	endDay := (meta.MaxTime - 1) / msPerDay
	if startDay != endDay {
		return fmt.Errorf("block [MinTime=%d, MaxTime=%d) spans multiple UTC days (%d..%d); blocks must cover exactly one calendar UTC day",
			meta.MinTime, meta.MaxTime, startDay, endDay)
	}

	if !v.deep {
		return nil
	}

	utcDayStart := startDay * msPerDay
	utcDayEnd := utcDayStart + msPerDay
	// checkChunks=false: WellFormedVerifier already did the CRC walk in deep mode.
	// We only need the postings walk to flag chunks outside the UTC-day window.
	if err := block.VerifyBlock(ctx, v.logger, blockDir, utcDayStart, utcDayEnd, false); err != nil {
		return fmt.Errorf("block has chunks outside UTC day [%d, %d): %w", utcDayStart, utcDayEnd, err)
	}
	return nil
}
