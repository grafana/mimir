// SPDX-License-Identifier: AGPL-3.0-only

package verify

import (
	"context"
	"fmt"
	"time"

	"github.com/go-kit/log"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
)

// SingleUTCDayVerifier enforces that a block's [MinTime, MaxTime) range lies
// entirely within a single calendar UTC day based on meta.json header data. It
// accepts sparse blocks whose span is less than 24 hours.
type SingleUTCDayVerifier struct {
	logger log.Logger
}

// NewSingleUTCDayVerifier constructs a SingleUTCDayVerifier. Verification depth
// has no effect on this verifier: it always does the same header arithmetic.
func NewSingleUTCDayVerifier(logger log.Logger) *SingleUTCDayVerifier {
	return &SingleUTCDayVerifier{logger: logger}
}

// Name returns the stable check name used in log lines and Report entries.
func (v *SingleUTCDayVerifier) Name() string { return "single-utc-day" }

// Verify returns nil if the block's declared [MinTime, MaxTime) range is
// confined to a single calendar UTC day. See the type comment for the exact
// formula.
func (v *SingleUTCDayVerifier) Verify(_ context.Context, _ string, meta block.Meta) error {
	if meta.MinTime >= meta.MaxTime {
		return fmt.Errorf("block time range is empty or inverted: MinTime=%d MaxTime=%d",
			meta.MinTime, meta.MaxTime)
	}

	// Note: very small negative timestamps will map to the same 0 bucket as small
	// positive timestamps.
	startDay := time.UnixMilli(meta.MinTime).UTC().Truncate(24 * time.Hour)
	endDay := time.UnixMilli(meta.MaxTime - 1).UTC().Truncate(24 * time.Hour)
	if !startDay.Equal(endDay) {
		return fmt.Errorf("block [MinTime=%d, MaxTime=%d) spans multiple UTC days (%s..%s); blocks must cover exactly one calendar UTC day",
			meta.MinTime, meta.MaxTime, startDay.Format("2006-01-02"), endDay.Format("2006-01-02"))
	}

	return nil
}
