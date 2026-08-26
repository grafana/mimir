// SPDX-License-Identifier: AGPL-3.0-only

package verify

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/go-kit/log"
)

// DuplicateDayVerifier rejects a batch that contains two or more blocks whose
// [MinTime, MaxTime) ranges share the same UTC-day key. Uploading two blocks
// that cover the same UTC day creates guaranteed compaction conflicts on the
// server side, so this client-side batch check fails fast before any upload.
//
// Day-key formula: day = meta.MinTime / msPerDay. The msPerDay constant is
// defined in singleutcday.go and reused here (same package).
//
// SingleUTCDayVerifier dependency: DuplicateDayVerifier assumes each block
// already passed SingleUTCDayVerifier, so its [MinTime, MaxTime) fits entirely
// inside one UTC day and MinTime uniquely identifies that day. If a caller
// registers DuplicateDayVerifier without SingleUTCDayVerifier, a block that
// spans two UTC days will be keyed only on its MinTime day — we deliberately
// do NOT re-check here, because splitting one bad block across two day
// buckets would mask genuine single-day collisions for the other blocks.
//
// yoloString safety: the ULIDs recorded in the error message come from
// meta.ULID.String(), where ulid.ULID is a fixed-size [16]byte array. String()
// allocates a fresh 26-byte string that is not backed by any pooled request
// buffer, so retaining these strings in the returned error (which may outlive
// the verifier call) is safe without strings.Clone.
type DuplicateDayVerifier struct {
	logger log.Logger
}

// NewDuplicateDayVerifier constructs a DuplicateDayVerifier.
func NewDuplicateDayVerifier(logger log.Logger) *DuplicateDayVerifier {
	return &DuplicateDayVerifier{logger: logger}
}

// Name returns the stable check name used in log lines and Report entries.
func (v *DuplicateDayVerifier) Name() string { return "duplicate-day" }

// Verify returns nil if every block in the batch covers a distinct UTC day.
// On collision it returns a single error enumerating every colliding day and
// the ULIDs of all blocks sharing that day, with days sorted ascending and
// ULIDs sorted lexicographically within each day for deterministic output.
func (v *DuplicateDayVerifier) Verify(_ context.Context, blocks []BlockRef) error {
	if len(blocks) < 2 {
		return nil
	}
	dayToULIDs := make(map[int64][]string, len(blocks))
	for _, blk := range blocks {
		day := blk.Meta.MinTime / msPerDay
		dayToULIDs[day] = append(dayToULIDs[day], blk.Meta.ULID.String())
	}
	var collidingDays []int64
	for day, ulids := range dayToULIDs {
		if len(ulids) > 1 {
			collidingDays = append(collidingDays, day)
		}
	}
	if len(collidingDays) == 0 {
		return nil
	}
	slices.Sort(collidingDays)
	parts := make([]string, 0, len(collidingDays))
	for _, day := range collidingDays {
		ulids := dayToULIDs[day]
		slices.Sort(ulids)
		parts = append(parts, fmt.Sprintf("day %d: [%s]", day, strings.Join(ulids, ", ")))
	}
	return fmt.Errorf("multiple blocks cover the same UTC day: %s", strings.Join(parts, "; "))
}
