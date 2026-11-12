// SPDX-License-Identifier: AGPL-3.0-only
//go:build rustengine && cgo

package rustengine

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
)

func TestRustConcurrentSyncAndAppend(t *testing.T) {
	dir := t.TempDir()
	c, err := Open(dir)
	require.NoError(t, err)
	t.Cleanup(c.Close)
	_, err = c.Call(Request{Op: "enable_buffered_wal"})
	require.NoError(t, err)
	for _, req := range []Request{
		{Op: "begin"},
		{Op: "append", Labels: []Label{{Name: "__name__", Value: "concurrent"}}, Sample: &Sample{Timestamp: 0, Kind: 1}},
		{Op: "commit"},
	} {
		_, err = c.Call(req)
		require.NoError(t, err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	wg.Go(func() {
		for range 100 {
			if err := c.Sync(); err != nil {
				errs <- err
				return
			}
		}
	})
	wg.Go(func() {
		for i := range 100 {
			_, err := c.Call(Request{Op: "ingest_floats", Maxt: 101, Batch: []FloatSeries{{Labels: []mimirpb.LabelAdapter{{Name: "__name__", Value: "concurrent"}}, Samples: []mimirpb.Sample{{TimestampMs: int64(i + 1), Value: float64(i)}}}}})
			if err != nil {
				errs <- err
				return
			}
		}
	})
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.NoError(t, c.Sync())
	c.Close()
	restored, err := Open(dir)
	require.NoError(t, err)
	defer restored.Close()
	got, err := restored.Call(Request{Op: "read", Mint: 0, Maxt: 101})
	require.NoError(t, err)
	require.Len(t, got.Series, 1)
	require.Len(t, got.Series[0].Samples, 101)
}
