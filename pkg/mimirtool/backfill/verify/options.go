// SPDX-License-Identifier: AGPL-3.0-only

package verify

// options is the internal aggregated configuration of a Verifier.
type options struct {
	blockChecks []BlockVerifier
	batchChecks []BatchVerifier
	mode        Mode
	failFast    bool
	concurrency int
}

// Option configures a Verifier via NewVerifier.
type Option func(*options)

// WithBlockCheck appends a per-block verifier to the verifier chain.
// Multiple WithBlockCheck options are allowed; checks run in order per block.
func WithBlockCheck(v BlockVerifier) Option {
	return func(o *options) { o.blockChecks = append(o.blockChecks, v) }
}

// WithBatchCheck appends a cross-block verifier. Batch checks run after all
// per-block checks complete. Ships with zero implementations in v1.
func WithBatchCheck(v BatchVerifier) Option {
	return func(o *options) { o.batchChecks = append(o.batchChecks, v) }
}

// WithMode sets the verification depth mode. Default: Deep.
func WithMode(m Mode) Option { return func(o *options) { o.mode = m } }

// WithFailFast toggles fail-fast behavior. Default: true.
// When false, all blocks and all checks run to completion ("full report").
func WithFailFast(b bool) Option { return func(o *options) { o.failFast = b } }

// WithConcurrency sets the maximum number of blocks verified in parallel.
// n <= 0 selects auto: min(GOMAXPROCS, 4) at Run time.
// n == 1 is strict serial execution.
func WithConcurrency(n int) Option {
	return func(o *options) {
		if n < 0 {
			n = 0
		}
		o.concurrency = n
	}
}
