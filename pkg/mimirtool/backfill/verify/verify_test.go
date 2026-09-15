// SPDX-License-Identifier: AGPL-3.0-only

package verify

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-kit/log"
	"github.com/grafana/regexp"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
)

// --- Test doubles ---

type passingBlockVerifier struct {
	name  string
	calls atomic.Int64
}

func (p *passingBlockVerifier) Name() string { return p.name }
func (p *passingBlockVerifier) Verify(_ context.Context, _ string, _ block.Meta) error {
	p.calls.Add(1)
	return nil
}

type failingBlockVerifier struct {
	name string
	err  error
}

func (f *failingBlockVerifier) Name() string { return f.name }
func (f *failingBlockVerifier) Verify(_ context.Context, _ string, _ block.Meta) error {
	return f.err
}

type rejectMultiBatchVerifier struct{ calls atomic.Int64 }

func (r *rejectMultiBatchVerifier) Name() string { return "reject-multi-batch" }
func (r *rejectMultiBatchVerifier) Verify(_ context.Context, blocks []BlockRef, _ *Report) error {
	r.calls.Add(1)
	if len(blocks) > 1 {
		return fmt.Errorf("rejecting multi-block batch of size %d", len(blocks))
	}
	return nil
}

// --- Fixture helper ---

// writeMinimalBlockDir creates a block directory with only a valid meta.json.
// Sufficient for Verifier.Run's ReadMetaFromDir call; tests that need real
// block data should use block.GenerateBlockFromSpec instead.
func writeMinimalBlockDir(t *testing.T, parent string, minTime, maxTime int64) string {
	t.Helper()
	id := ulid.MustNew(uint64(minTime), nil) // any ULID; deterministic from minTime
	dir := filepath.Join(parent, id.String())
	require.NoError(t, os.MkdirAll(dir, 0o755))

	m := block.Meta{
		BlockMeta: tsdb.BlockMeta{
			ULID:    id,
			MinTime: minTime,
			MaxTime: maxTime,
			Version: 1,
		},
	}
	buf, err := json.Marshal(m)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, block.MetaFilename), buf, 0o644))
	return dir
}

// --- Tests ---

func TestNewVerifier_Defaults(t *testing.T) {
	v := NewVerifier(log.NewNopLogger())
	report := v.Run(context.Background(), nil)
	require.NotNil(t, report)
	assert.False(t, report.HasFailures())
	total, failedBlocks, failures := report.Summary()
	assert.Equal(t, 0, total)
	assert.Equal(t, 0, failedBlocks)
	assert.Equal(t, 0, failures)
}

func TestBlockVerifierSeam_BothImplsInvoked(t *testing.T) {
	tempRoot := t.TempDir()
	const day = int64(86_400_000)
	dir1 := writeMinimalBlockDir(t, tempRoot, 0, day)
	dir2 := writeMinimalBlockDir(t, tempRoot, day, 2*day)

	passing := &passingBlockVerifier{name: "passing"}
	failing := &failingBlockVerifier{name: "failing", err: fmt.Errorf("synthetic")}

	v := NewVerifier(log.NewNopLogger(),
		WithBlockCheck(passing),
		WithBlockCheck(failing),
		WithConcurrency(1),
		WithFailFast(false),
	)
	report := v.Run(context.Background(), []string{dir1, dir2})

	assert.EqualValues(t, 2, passing.calls.Load(), "passing verifier should have been called once per block")
	require.True(t, report.HasFailures())
	assert.Len(t, report.Failures(), 2, "failing verifier should have produced one failure per block")
}

func TestBatchVerifierSeam_InvokedAfterPerBlock(t *testing.T) {
	tempRoot := t.TempDir()
	const day = int64(86_400_000)
	dir1 := writeMinimalBlockDir(t, tempRoot, 0, day)
	dir2 := writeMinimalBlockDir(t, tempRoot, day, 2*day)

	batch := &rejectMultiBatchVerifier{}
	v := NewVerifier(log.NewNopLogger(), WithBatchCheck(batch))
	report := v.Run(context.Background(), []string{dir1, dir2})

	assert.EqualValues(t, 1, batch.calls.Load(), "batch verifier should run exactly once")
	require.True(t, report.HasFailures(), "batch rejection should appear in report")
	_, _, failures := report.Summary()
	assert.Equal(t, 1, failures)
}

func TestBatchVerifierFailFast_StopsAfterFirstVerdict(t *testing.T) {
	tempRoot := t.TempDir()
	const day = int64(86_400_000)
	dirs := []string{
		writeMinimalBlockDir(t, tempRoot, 0, day),
		writeMinimalBlockDir(t, tempRoot, day, 2*day),
	}

	for _, tc := range []struct {
		name          string
		failFast      bool
		wantSecondRun int64
		wantFailures  int
	}{
		{name: "fail_fast_skips_later_batch_checks", failFast: true, wantSecondRun: 0, wantFailures: 1},
		{name: "full_report_runs_every_batch_check", failFast: false, wantSecondRun: 1, wantFailures: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			first, second := &rejectMultiBatchVerifier{}, &rejectMultiBatchVerifier{}
			v := NewVerifier(log.NewNopLogger(),
				WithFailFast(tc.failFast),
				WithBatchCheck(first),
				WithBatchCheck(second),
			)
			report := v.Run(context.Background(), dirs)

			assert.EqualValues(t, 1, first.calls.Load(), "the first batch check always runs")
			assert.EqualValues(t, tc.wantSecondRun, second.calls.Load())
			_, _, failures := report.Summary()
			assert.Equal(t, tc.wantFailures, failures)
		})
	}
}

func TestFailFast_HaltsProcessing(t *testing.T) {
	tempRoot := t.TempDir()
	const day = int64(86_400_000)
	dirs := make([]string, 5)
	for i := range dirs {
		dirs[i] = writeMinimalBlockDir(t, tempRoot, int64(i)*day, int64(i+1)*day)
	}

	t.Run("fail_fast_true", func(t *testing.T) {
		passing := &passingBlockVerifier{name: "counter"}
		failing := &failingBlockVerifier{name: "always-fails", err: fmt.Errorf("boom")}
		v := NewVerifier(log.NewNopLogger(),
			WithBlockCheck(failing), // order: failing runs first
			WithBlockCheck(passing),
			WithConcurrency(1), // force serial so fail-fast is deterministic
			WithFailFast(true),
		)
		_ = v.Run(context.Background(), dirs)
		assert.Less(t, passing.calls.Load(), int64(5), "fail-fast should stop before all 5 blocks processed")
	})

	t.Run("full_report_false", func(t *testing.T) {
		passing := &passingBlockVerifier{name: "counter"}
		failing := &failingBlockVerifier{name: "always-fails", err: fmt.Errorf("boom")}
		v := NewVerifier(log.NewNopLogger(),
			WithBlockCheck(failing),
			WithBlockCheck(passing),
			WithConcurrency(1),
			WithFailFast(false),
		)
		_ = v.Run(context.Background(), dirs)
		assert.EqualValues(t, 5, passing.calls.Load(), "fail-fast disabled should process all 5 blocks")
	})
}

func TestReport_SummaryAndErr(t *testing.T) {
	r := newReport(3)
	r.Add("01FAKE000000000000000000A", "check-x", "/dir/a", fmt.Errorf("err-a1"))
	r.Add("01FAKE000000000000000000A", "check-y", "/dir/a", fmt.Errorf("err-a2"))
	r.Add("01FAKE000000000000000000B", "check-x", "/dir/b", fmt.Errorf("err-b1"))

	total, failedBlocks, failures := r.Summary()
	assert.Equal(t, 3, total)
	assert.Equal(t, 2, failedBlocks)
	assert.Equal(t, 3, failures)

	err := r.Err()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "verification failed")
}

// TestReport_ErrFormat asserts the aggregated error is a summary only to
// prevent unreadable errors enumerating every single failure for large batches.
func TestReport_ErrFormat(t *testing.T) {
	r := newReport(10)
	for i := 0; i < 7; i++ {
		r.Add(fmt.Sprintf("01FAKE00000000000000000%02d", i), "check-x", "/dir", fmt.Errorf("detail-%d", i))
	}

	err := r.Err()
	require.Error(t, err)

	got := err.Error()
	summaryRE := regexp.MustCompile(`^verification failed: \d+ failure\(s\) across \d+ block\(s\)$`)
	assert.Regexp(t, summaryRE, got, "Err() must return summary-only format")

	// Explicitly assert per-failure detail is NOT present in the aggregated error.
	assert.NotContains(t, got, "detail-0", "Err() must not enumerate individual failure messages")
	assert.NotContains(t, got, "check=", "Err() must not enumerate per-check detail")
}
