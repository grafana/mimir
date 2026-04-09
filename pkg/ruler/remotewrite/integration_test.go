// SPDX-License-Identifier: AGPL-3.0-only

package remotewrite

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"testing"
	"time"

	config_util "github.com/prometheus/common/config"
	promconfig "github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/record"
	"github.com/prometheus/prometheus/tsdb/wlog"
	"github.com/prometheus/prometheus/util/compression"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"

	utillog "github.com/grafana/mimir/pkg/util/log"
)

// TestDirectWALRemoteWrite verifies that writing to a wlog.WL and calling
// rws.Notify() causes the QueueManager to POST data to the remote endpoint.
// This test validates the core WAL→remote write pipeline.
func TestDirectWALRemoteWrite(t *testing.T) {
	received := make(chan struct{}, 10)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Logf("HTTP server received POST to %s", r.URL.Path)
		received <- struct{}{}
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(srv.Close)

	baseDir := t.TempDir()
	// QueueManager's watcher looks for WAL at <baseDir>/wal (filepath.Join(dir, "wal")),
	// so we must create the WAL there too.
	walDir := filepath.Join(baseDir, "wal")
	slogger := utillog.SlogFromGoKit(log.NewNopLogger())
	reg := prometheus.NewRegistry()

	wal, err := wlog.New(slogger, reg, walDir, compression.None)
	require.NoError(t, err)
	t.Cleanup(func() { _ = wal.Close() })

	rws := remote.NewWriteStorage(slogger, reg, baseDir, 5*time.Second, nil, false)
	t.Cleanup(func() { _ = rws.Close() })

	rawURL, err := url.Parse(srv.URL)
	require.NoError(t, err)

	rwCfg := promconfig.DefaultRemoteWriteConfig
	rwCfg.Name = "test"
	rwCfg.URL = &config_util.URL{URL: rawURL}
	require.NoError(t, rws.ApplyConfig(&promconfig.Config{
		RemoteWriteConfigs: []*promconfig.RemoteWriteConfig{&rwCfg},
	}))

	// Give the WAL Watcher goroutine time to start and set startTimestamp = now.
	time.Sleep(200 * time.Millisecond)

	// Write a series + sample with a timestamp that is strictly after startTimestamp.
	l := labels.FromStrings("__name__", "integration_test_metric")
	ref := chunks.HeadSeriesRef(1)
	ts := time.Now().Add(2 * time.Second).UnixMilli()

	enc := record.Encoder{}
	require.NoError(t, wal.Log(enc.Series([]record.RefSeries{{Ref: ref, Labels: l}}, nil)))
	require.NoError(t, wal.Log(enc.Samples([]record.RefSample{{Ref: ref, T: ts, V: 42.0}}, nil)))

	t.Logf("Wrote series+sample to WAL (ts=%d), calling Notify...", ts)
	rws.Notify()

	select {
	case <-received:
		t.Log("SUCCESS: QueueManager POSTed to remote endpoint")
	case <-time.After(30 * time.Second):
		t.Fatal("timed out: QueueManager never POSTed to remote endpoint")
	}
}

// TestStorageViaNewStorage validates that the higher-level newStorage() API also
// delivers samples to the remote endpoint end-to-end.
func TestStorageViaNewStorage(t *testing.T) {
	received := make(chan struct{}, 10)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		received <- struct{}{}
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(srv.Close)

	cfg := Config{Name: "test"}
	require.NoError(t, cfg.URL.Set(srv.URL))

	s, err := newStorage(cfg, t.TempDir(), 5*time.Second, prometheus.NewRegistry(), log.NewNopLogger())
	require.NoError(t, err)
	t.Cleanup(s.Stop)

	// Give the WAL watcher goroutine time to enter its select loop.
	// Notify() is a non-blocking send on an unbuffered channel; if the watcher
	// hasn't started yet the notification is silently dropped and only the 15s
	// readTimeout ticker would wake it.
	time.Sleep(500 * time.Millisecond)
	ts := time.Now().Add(2 * time.Second).UnixMilli()

	app := s.Appender(nil) // nil context is fine for direct path
	_, err = app.Append(0, labels.FromStrings("__name__", "higher_level_test"), ts, 1.0)
	require.NoError(t, err)
	require.NoError(t, app.Commit())

	select {
	case <-received:
		// success
	case <-time.After(30 * time.Second):
		t.Fatal("timed out: higher-level Storage never delivered sample to remote endpoint")
	}
}

// TestWALRoundtrip verifies that data written to the WAL by wlog.WL
// can be read back by a WAL Watcher, confirming the write→read pipeline works.
func TestWALRoundtrip(t *testing.T) {
	dir := t.TempDir()
	slogger := utillog.SlogFromGoKit(log.NewNopLogger())
	reg := prometheus.NewRegistry()

	wal, err := wlog.New(slogger, reg, dir, compression.None)
	require.NoError(t, err)
	t.Cleanup(func() { _ = wal.Close() })

	// Write series + sample records.
	l := labels.FromStrings("__name__", "test")
	ref := chunks.HeadSeriesRef(99)
	ts := time.Now().Add(2 * time.Second).UnixMilli()
	enc := record.Encoder{}
	require.NoError(t, wal.Log(enc.Series([]record.RefSeries{{Ref: ref, Labels: l}}, nil)))
	require.NoError(t, wal.Log(enc.Samples([]record.RefSample{{Ref: ref, T: ts, V: 42.0}}, nil)))

	// Read back using the WAL's own reader.
	first, last, err := wlog.Segments(dir)
	require.NoError(t, err)
	t.Logf("WAL segments: first=%d last=%d", first, last)

	seg, err := wlog.OpenReadSegment(wlog.SegmentName(dir, 0))
	require.NoError(t, err)
	defer seg.Close()

	var gotSeries int
	var gotSamples int
	reader := wlog.NewLiveReader(slogger, wlog.NewLiveReaderMetrics(nil), seg)
	dec := record.Decoder{}
	for reader.Next() {
		rec := reader.Record()
		switch dec.Type(rec) {
		case record.Series:
			series, err := dec.Series(rec, nil)
			require.NoError(t, err)
			gotSeries += len(series)
		case record.Samples:
			samples, err := dec.Samples(rec, nil)
			require.NoError(t, err)
			gotSamples += len(samples)
		}
	}
	t.Logf("read %d series records and %d sample records from WAL", gotSeries, gotSamples)
	assert.Equal(t, 1, gotSeries)
	assert.Equal(t, 1, gotSamples)
}
