// SPDX-License-Identifier: AGPL-3.0-only

package remotewrite

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestStorage(t *testing.T, url string) *Storage {
	t.Helper()
	cfg := Config{Name: "test"}
	require.NoError(t, cfg.URL.Set(url))
	s, err := newStorage(cfg, t.TempDir(), 5*time.Second, prometheus.NewRegistry(), log.NewNopLogger())
	require.NoError(t, err)
	t.Cleanup(s.Stop)
	return s
}

func TestStorage_CommitSendsToRemote(t *testing.T) {
	received := make(chan struct{}, 10)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		received <- struct{}{}
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(srv.Close)

	s := newTestStorage(t, srv.URL)

	// Give the WAL watcher goroutine time to finish its initial scan and enter
	// its select loop. Notify() sends on an unbuffered channel — if the watcher
	// isn't waiting yet the notification is silently dropped and only the 15s
	// readTimeout ticker would wake it, which matches the test timeout.
	time.Sleep(500 * time.Millisecond)

	// Timestamp must be strictly after the watcher's startTimestamp (time.Now()
	// at goroutine start) so samples are not skipped during replay.
	ts := time.Now().Add(2 * time.Second).UnixMilli()
	app := s.Appender(context.Background())
	_, err := app.Append(0, labels.FromStrings("__name__", "foo"), ts, 1.0)
	require.NoError(t, err)
	require.NoError(t, app.Commit())

	select {
	case <-received:
		// QueueManager flushed the WAL and POSTed to the endpoint.
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for remote write request")
	}
}

func TestStorage_CommitNilSamples(t *testing.T) {
	s := newTestStorage(t, "http://localhost:9999")
	app := s.Appender(context.Background())
	require.NoError(t, app.Commit()) // no samples — must not error
}

func TestStorage_Rollback(t *testing.T) {
	s := newTestStorage(t, "http://localhost:9999")
	app := s.Appender(context.Background())
	_, err := app.Append(0, labels.FromStrings("__name__", "foo"), time.Now().UnixMilli(), 1.0)
	require.NoError(t, err)
	require.NoError(t, app.Rollback()) // discard without error
}

func TestStorage_URLFilter_Skips(t *testing.T) {
	called := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called = true
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(srv.Close)

	s := newTestStorage(t, srv.URL)

	// Inject a URL list that does NOT include this storage's URL.
	ctx := ContextWithRemoteWriteURLs(context.Background(), []string{"http://other-endpoint"})
	app := s.Appender(ctx)
	_, err := app.Append(0, labels.FromStrings("__name__", "foo"), time.Now().UnixMilli(), 1.0)
	require.NoError(t, err)
	require.NoError(t, app.Commit())

	// Allow brief time for the QueueManager to potentially flush (it should not).
	time.Sleep(200 * time.Millisecond)
	assert.False(t, called, "remote endpoint must not be called when URL is filtered out")
}

func TestStorage_URLFilter_Allows(t *testing.T) {
	received := make(chan struct{}, 10)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		received <- struct{}{}
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(srv.Close)

	s := newTestStorage(t, srv.URL)
	time.Sleep(500 * time.Millisecond) // wait for WAL watcher to enter its select loop

	// Inject a URL list that DOES include this storage's URL.
	ts := time.Now().Add(2 * time.Second).UnixMilli()
	ctx := ContextWithRemoteWriteURLs(context.Background(), []string{srv.URL})
	app := s.Appender(ctx)
	_, err := app.Append(0, labels.FromStrings("__name__", "bar"), ts, 2.0)
	require.NoError(t, err)
	require.NoError(t, app.Commit())

	select {
	case <-received:
		// success
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for remote write request")
	}
}

func TestStorage_SameSeriesGetsOneWALEntry(t *testing.T) {
	s := newTestStorage(t, "http://localhost:9999")

	app := s.Appender(context.Background())
	l := labels.FromStrings("__name__", "counter")
	ts := time.Now().UnixMilli()

	_, err := app.Append(0, l, ts, 1.0)
	require.NoError(t, err)
	_, err = app.Append(0, l, ts+1000, 2.0) // same series, next timestamp
	require.NoError(t, err)
	require.NoError(t, app.Commit())

	// Both samples share one WAL ref — the series map should have exactly one entry.
	s.seriesMu.Lock()
	seriesCount := len(s.series)
	s.seriesMu.Unlock()
	assert.Equal(t, 1, seriesCount)
}
