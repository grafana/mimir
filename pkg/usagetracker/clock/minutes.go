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

// GreaterThan returns true if this value is greater than other on a four-hour clock face assuming that none of the values is ever older than 1h.
func (m Minutes) GreaterThan(other Minutes) bool {
	if m > other {
		return m-other < 60
	}
	return int(m+(2*60))-int(other) < 60
}

// GreaterOrEqualThan returns true if this value is greater or equal than other on a four-hour clock face assuming that none of the values is ever older than 1h.
func (m Minutes) GreaterOrEqualThan(other Minutes) bool {
	return m == other || m.GreaterThan(other)
}

func (m Minutes) String() string { return fmt.Sprintf("%dm", m) }
