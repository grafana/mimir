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
	failedBlocks map[string]struct{} // keyed by blockULID, or by blockDir when the ULID is unknown; batch-level failures are not counted
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
	// Only add to r.failedBlocks for valid block / dirs.
	switch {
	case blockULID != "":
		r.failedBlocks[blockULID] = struct{}{}
	case blockDir != "":
		r.failedBlocks[blockDir] = struct{}{}
	}
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
// counts distinct blocks, so it excludes batch-level failures, which describe
// the batch rather than any one block.
func (r *Report) Summary() (int, int, int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.totalBlocks, len(r.failedBlocks), len(r.failures)
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
	if len(r.failedBlocks) == 0 {
		return fmt.Errorf("verification failed: %d failure(s)", len(r.failures))
	}
	return fmt.Errorf("verification failed: %d failure(s) across %d block(s)",
		len(r.failures), len(r.failedBlocks))
}
