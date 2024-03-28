// SPDX-License-Identifier: AGPL-3.0-only

package operator

import "time"

func stepCount(start, end, interval int64) int {
	return int((end-start)/interval) + 1
}

func durationMilliseconds(d time.Duration) int64 {
	return int64(d / (time.Millisecond / time.Nanosecond))
}
