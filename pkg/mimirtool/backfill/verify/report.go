// SPDX-License-Identifier: AGPL-3.0-only

package verify

import (
	"fmt"
	"sync"
)

// Failure describes a single per-block, per-check failure.
type Failure struct {
	BlockULID string // "" for meta-parse failures or batch-level failures
	BlockDir  string // "" for batch-level failures
	Check     string // verifier Name()
	Err       error
}

// Report aggregates verification failures across all blocks and checks.
// Methods are safe for concurrent use.
type Report struct {
	mu           sync.Mutex
	totalBlocks  int
	failures     []Failure
	failedBlocks map[string]struct{} // keyed by blockULID; "" (meta/batch) counted once under empty key
}

func newReport(totalBlocks int) *Report {
	return &Report{
		totalBlocks:  totalBlocks,
		failedBlocks: make(map[string]struct{}),
	}
}

// Add records a failure. blockULID may be "" for meta-parse or batch-level
// failures; blockDir may be "" for batch failures.
func (r *Report) Add(blockULID, checkName, blockDir string, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.failures = append(r.failures, Failure{
		BlockULID: blockULID,
		BlockDir:  blockDir,
		Check:     checkName,
		Err:       err,
	})
	r.failedBlocks[blockULID] = struct{}{}
}

// HasFailures reports whether any failure has been recorded.
func (r *Report) HasFailures() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.failures) > 0
}

// Failures returns a copy of the recorded failures.
func (r *Report) Failures() []Failure {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Failure, len(r.failures))
	copy(out, r.failures)
	return out
}

// Summary returns (totalBlocks, failedBlocks, totalFailures). failedBlocks
// excludes the empty-ULID bucket used for batch/meta failures.
func (r *Report) Summary() (int, int, int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.totalBlocks, r.uniqueBlockCountLocked(), len(r.failures)
}

// uniqueBlockCountLocked returns the number of distinct failing blocks by
// ULID, excluding the empty-ULID bucket reserved for batch/meta failures.
// Caller must hold r.mu.
func (r *Report) uniqueBlockCountLocked() int {
	n := 0
	for ulid := range r.failedBlocks {
		if ulid != "" {
			n++
		}
	}
	return n
}

// Err returns a single aggregated error suitable for propagating from
// MimirClient.Backfill. The error message is a summary only ("verification
// failed: N failure(s) across M block(s)") — per-failure detail is already
// emitted as individual log lines during Run; enumerating them here would
// produce unreadably long error strings for large batches (e.g. 500 blocks
// × 2 checks = 1000+ lines). Returns nil if no failures.
func (r *Report) Err() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.failures) == 0 {
		return nil
	}
	return fmt.Errorf("verification failed: %d failure(s) across %d block(s)",
		len(r.failures), r.uniqueBlockCountLocked())
}
