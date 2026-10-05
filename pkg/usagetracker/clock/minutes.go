// SPDX-License-Identifier: AGPL-3.0-only

package clock

import (
	"fmt"
	"time"
)

func AreInValidSpanToCompareMinutes(a, b time.Time) bool {
	if a.After(b) {
		a, b = b, a
	}
	return b.Sub(a) < time.Hour
}

func ToMinutes(t time.Time) Minutes {
	return Minutes(t.Sub(t.Truncate(2 * time.Hour)).Minutes())
}

// Minutes represents the Minutes passed since the last 2-hour boundary (00:00, 02:00, 04:00, etc.).
// This value only makes sense within the last hour.
type Minutes uint8

// aheadOf returns how many minutes m is ahead of other on the two-hour clock face, in [0, 120) for valid values.
func (m Minutes) aheadOf(other Minutes) int {
	d := int(m) - int(other)
	// d>>63 is -1 (all bits set) if d is negative, and 0 otherwise: this adds 120 only when d is negative.
	return d + (d>>63)&120
}

// GreaterThan returns true if this value is greater than other on a two-hour clock face assuming that none of the values is ever older than 1h.
func (m Minutes) GreaterThan(other Minutes) bool {
	// When aheadOf is 0, subtracting 1 wraps around to a large uint, so this checks 0 < aheadOf < 60.
	return uint(m.aheadOf(other)-1) < 59
}

// GreaterOrEqualThan returns true if this value is greater or equal than other on a two-hour clock face assuming that none of the values is ever older than 1h.
func (m Minutes) GreaterOrEqualThan(other Minutes) bool {
	return uint(m.aheadOf(other)) < 60
}

func (m Minutes) String() string { return fmt.Sprintf("%dm", m) }
