// SPDX-License-Identifier: AGPL-3.0-only

package client

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirtool/backfill/verify"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	testutil "github.com/grafana/mimir/pkg/util/test"
)

const msPerDay = int64(86_400_000)

// uploadCountingServer returns an httptest.Server that 200s every request.
// It routes by URL suffix so the backfill client's poll loop terminates
// correctly: /check endpoints return {"result":"complete"}, while /start,
// /files, and /finish only need a 200 status.
//
// The returned counter is incremented once per /api/v1/upload/block/...
// request, regardless of suffix.
func uploadCountingServer(t *testing.T) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	var counter atomic.Int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/api/v1/upload/block/") {
			counter.Add(1)
		}
		if strings.HasSuffix(r.URL.Path, "/check") {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"result":"complete"}`))
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(ts.Close)
	return ts, &counter
}

func newTestClient(t *testing.T, ts *httptest.Server) *MimirClient {
	t.Helper()
	cli, err := New(Config{Address: ts.URL, ID: "test-tenant"}, log.NewNopLogger())
	require.NoError(t, err)
	return cli
}

// sampleAt returns a chunks.Sample using the canonical Mimir test.Sample type
// (pkg/util/test/tsdb.go). Duplicated from the verify package's internal
// testhelper_test.go — Go does not share *_test.go helpers across packages.
func sampleAt(ts int64, v float64) chunks.Sample {
	return testutil.Sample{TS: ts, Val: v}
}

// generateValidBlockInline creates a 3-series block with the given samples.
// Duplicated from pkg/mimirtool/backfill/verify/testhelper_test.go for the
// same cross-package reason noted on sampleAt.
func generateValidBlockInline(t *testing.T, parent string, samples []chunks.Sample) (string, *block.Meta) {
	t.Helper()
	require.GreaterOrEqual(t, len(samples), 1)
	makeChunk := func() chunks.Meta {
		c, err := chunks.ChunkFromSamples(samples)
		require.NoError(t, err)
		return c
	}
	specs := []*block.SeriesSpec{
		{Labels: labels.FromStrings("__name__", "metric_a"), Chunks: []chunks.Meta{makeChunk()}},
		{Labels: labels.FromStrings("__name__", "metric_b"), Chunks: []chunks.Meta{makeChunk()}},
		{Labels: labels.FromStrings("__name__", "metric_c"), Chunks: []chunks.Meta{makeChunk()}},
	}
	meta, err := block.GenerateBlockFromSpec(parent, specs)
	require.NoError(t, err)
	return filepath.Join(parent, meta.ULID.String()), meta
}

func TestBackfillWithOptions_VerificationFailureZeroUploads(t *testing.T) {
	ts, uploadCount := uploadCountingServer(t)
	cli := newTestClient(t, ts)

	validDir, _ := generateValidBlockInline(t, t.TempDir(), []chunks.Sample{
		sampleAt(1_000, 1.0), sampleAt(2_000, 2.0), sampleAt(3_000, 3.0),
	})
	// Cross-midnight block: samples straddle msPerDay -> fails SingleUTCDayVerifier.
	crossDir, _ := generateValidBlockInline(t, t.TempDir(), []chunks.Sample{
		sampleAt(msPerDay-2_000, 1.0), sampleAt(msPerDay-1_000, 2.0),
		sampleAt(msPerDay+1_000, 3.0), sampleAt(msPerDay+2_000, 4.0),
	})

	v := verify.NewVerifier(log.NewNopLogger(),
		verify.WithBlockCheck(verify.NewSingleUTCDayVerifier(log.NewNopLogger(), verify.Medium)),
	)

	err := cli.BackfillWithOptions(context.Background(),
		[]string{validDir, crossDir}, time.Millisecond, v, false)
	require.Error(t, err, "verification failure must propagate as error")
	assert.Equal(t, int64(0), uploadCount.Load(),
		"no /api/v1/upload/block/... request should have been issued when any block fails verification")
}

func TestBackfillWithOptions_DryRunZeroUploads(t *testing.T) {
	ts, uploadCount := uploadCountingServer(t)
	cli := newTestClient(t, ts)

	validDir, _ := generateValidBlockInline(t, t.TempDir(), []chunks.Sample{
		sampleAt(1_000, 1.0), sampleAt(2_000, 2.0), sampleAt(3_000, 3.0),
	})

	v := verify.NewVerifier(log.NewNopLogger(),
		verify.WithBlockCheck(verify.NewSingleUTCDayVerifier(log.NewNopLogger(), verify.Medium)),
	)

	err := cli.BackfillWithOptions(context.Background(),
		[]string{validDir}, time.Millisecond, v, true /* dryRun */)
	require.NoError(t, err, "dry-run with valid batch must return nil")
	assert.Equal(t, int64(0), uploadCount.Load(),
		"--dry-run must issue zero upload requests")
}

func TestBackfillWithOptions_ValidBatchUploads(t *testing.T) {
	ts, uploadCount := uploadCountingServer(t)
	cli := newTestClient(t, ts)

	validDir, _ := generateValidBlockInline(t, t.TempDir(), []chunks.Sample{
		sampleAt(1_000, 1.0), sampleAt(2_000, 2.0), sampleAt(3_000, 3.0),
	})

	v := verify.NewVerifier(log.NewNopLogger(),
		verify.WithBlockCheck(verify.NewSingleUTCDayVerifier(log.NewNopLogger(), verify.Medium)),
	)

	err := cli.BackfillWithOptions(context.Background(),
		[]string{validDir}, time.Millisecond, v, false)
	require.NoError(t, err)
	assert.Greater(t, uploadCount.Load(), int64(0),
		"a verified block should produce at least one upload request")
}

func TestBackfill_LegacyWrapperBehavesAsBefore(t *testing.T) {
	ts, uploadCount := uploadCountingServer(t)
	cli := newTestClient(t, ts)

	validDir, _ := generateValidBlockInline(t, t.TempDir(), []chunks.Sample{
		sampleAt(1_000, 1.0), sampleAt(2_000, 2.0), sampleAt(3_000, 3.0),
	})

	err := cli.Backfill(context.Background(), []string{validDir}, time.Millisecond)
	require.NoError(t, err)
	assert.Greater(t, uploadCount.Load(), int64(0),
		"legacy Backfill wrapper must still upload when no-op verifier passes")
}
