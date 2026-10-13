// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import "sync"

// tenantInflightBytes tracks the in-flight push request bytes held by each tenant on this distributor.
type tenantInflightBytes struct {
	mu    sync.Mutex
	bytes map[string]int64
}

func newTenantInflightBytes() *tenantInflightBytes {
	return &tenantInflightBytes{bytes: map[string]int64{}}
}

// add adds delta to the tenant's in-flight bytes and returns the new total.
func (t *tenantInflightBytes) add(userID string, delta int64) int64 {
	t.mu.Lock()
	defer t.mu.Unlock()

	total := t.bytes[userID] + delta
	if total <= 0 {
		delete(t.bytes, userID)
		return 0
	}
	t.bytes[userID] = total
	return total
}

func (t *tenantInflightBytes) get(userID string) int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.bytes[userID]
}

// chargeTenantInflightBytes charges the request's compressed size to its tenant, and rejects the request if that takes
// the tenant over its per-instance limit. Requests whose size isn't known before decompression aren't charged.
// The charge is released by decreaseInflightPushRequestCounters.
func (d *Distributor) chargeTenantInflightBytes(rs *requestState, userID string, compressedSize int64) error {
	limit := int64(d.getInstanceLimits().MaxInflightPushRequestsBytesPerTenant)
	if limit <= 0 || compressedSize <= 0 || rs.tenantInflightBytes > 0 {
		return nil
	}

	rs.tenantUserID = userID
	rs.tenantInflightBytes = compressedSize
	if d.tenantInflightBytes.add(userID, compressedSize) > limit {
		d.rejectedRequests.WithLabelValues(reasonDistributorMaxInflightPushRequestsBytesPerTenant).Inc()
		d.discardedRequestsInflightBytesPerTenant.WithLabelValues(userID).Inc()
		return errMaxInflightRequestsBytesPerTenantReached
	}
	return nil
}
