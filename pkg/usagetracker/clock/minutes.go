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

// sub subtracts other from m, taking into account the 2-hour clock face, assuming both values aren't more than 1h apart.
// It does *not* return *Minutes* because it returns a duration, while Minutes is a timestamp.
func (m Minutes) sub(other Minutes) int {
	sign := 1
	if m < other {
		m, other, sign = other, m, -1
	}

	if m-other < 60 {
		return sign * int(m-other)
	}
	return sign * int(m-other-2*60)
}

// minutesGreater returns true if this value is greater than other on a four-hour clock face assuming that none of the values is ever older than 1h.
func (m Minutes) GreaterThan(other Minutes) bool {
	if m > other {
		return m-other < 60
	}
	return m+(2*60)-other < 60
}

// greaterOrEqualThan returns true if this value is greater or equal than other on a four-hour clock face assuming that none of the values is ever older than 1h.
func (m Minutes) GreaterOrEqualThan(other Minutes) bool {
	return m == other || m.GreaterThan(other)
}

func (m Minutes) String() string { return fmt.Sprintf("%dm", m) }
