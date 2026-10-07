// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import "time"

// markAllInactive makes every series inactive, as time passing until the tracker's purge at now would: until the
// next sample of each.
func (u *userTSDB) markAllInactive(now time.Time) {
	if u.trackerActive() {
		u.activeSeries.Purge(now, nil)
		return
	}
	u.nativeActive.DeactivateAll()
}
