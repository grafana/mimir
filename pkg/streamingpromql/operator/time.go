// SPDX-License-Identifier: AGPL-3.0-only

package operator

func stepCount(start, end, interval int64) int {
	return int((end-start)/interval) + 1
}
