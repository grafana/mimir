// SPDX-License-Identifier: AGPL-3.0-only

package operators

func stepCount(start, end, interval int64) int {
	return int((end-start)/interval) + 1
}
