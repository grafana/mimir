package ingest

import (
	"sync"
)

type TenantSyncer interface {
	TrackLatestAttemptedOffset(tenantID string, offset int64)
	SyncPreCommit(committedOffset int64)
}

type IngesterTenantSyncer struct {
	tenantOffsets map[string]int64
	mu            sync.Mutex
	SyncFunc      func(string) error
}

// TrackLatestAttemptedOffset tracks the latest offset attempted
// This should be set before writing to the TSDB
func (t *IngesterTenantSyncer) TrackLatestAttemptedOffset(tenantID string, offset int64) {
	t.mu.Lock()
	t.tenantOffsets[tenantID] = offset
	t.mu.Unlock()
}

func (t *IngesterTenantSyncer) SyncPreCommit(committedOffset int64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	toDelete := []string{}
	for tenant, offset := range t.tenantOffsets {
		t.SyncFunc(tenant)
		if offset <= committedOffset {
			toDelete = append(toDelete, tenant)
		}
	}

	for _, tenant := range toDelete {
		// if the offset is being committed and the attempted offset < committed, it means the attempted offset must have succeeded being written to the WAL
		// TODO: double check this delete won't cause race conditions
		delete(t.tenantOffsets, tenant)
	}
}

type NoOpTenantSyncer struct {
}

func (n NoOpTenantSyncer) TrackLatestAttemptedOffset(_ string, _ int64) {
}

func (n NoOpTenantSyncer) SyncPreCommit(_ int64) {
}
