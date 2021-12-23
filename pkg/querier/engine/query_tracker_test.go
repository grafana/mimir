// SPDX-License-Identifier: AGPL-3.0-only

package engine

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/jaeger-client-go"
	"github.com/weaveworks/common/user"

	"github.com/grafana/mimir/pkg/util/activitytracker"
)

func setupActivityTracker(t *testing.T, maxEntries int) *activitytracker.ActivityTracker {
	d := t.TempDir()

	cfg := activitytracker.Config{
		Filepath:   filepath.Join(d, "activity"),
		MaxEntries: maxEntries,
	}

	at, err := activitytracker.NewActivityTracker(cfg, nil)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = at.Close()
	})

	return at
}

func TestQueryTrackerMaxConcurrency(t *testing.T) {
	for name, tracker := range map[string]*activitytracker.ActivityTracker{
		"nil tracker":     nil,
		"non-nil tracker": setupActivityTracker(t, 16),
	} {
		t.Run(name, func(t *testing.T) {
			const maxConcurrency = 5
			qt := newQueryTracker(maxConcurrency, tracker)

			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()

			for i := 0; i < maxConcurrency; i++ {
				_, err := qt.Insert(ctx, "test query")
				require.NoError(t, err)
			}

			_, err := qt.Insert(ctx, "another test query")
			require.Error(t, err) // Will wait until context timeout.

			// This decreases semaphore in QueryTracker, but doesn't remove activity from ActivityTracker.
			// For our tests, this is fine.
			qt.Delete(-1)

			_, err = qt.Insert(ctx, "another test query")
			require.NoError(t, err)
		})
	}
}

func TestQueryTrackerWithNilActivityTrackerInsertDoesntAllocate(t *testing.T) {
	qt := newQueryTracker(0, nil)

	assert.Zero(t, testing.AllocsPerRun(1000, func() {
		_, _ = qt.Insert(context.Background(), "query string")
	}))
}

func TestActivityDescription(t *testing.T) {
	ctx := context.Background()
	assert.Equal(t, "query=query string", generateActivityDescription(ctx, "query string"))
	assert.Equal(t, "tenant=user query=query string", generateActivityDescription(user.InjectOrgID(ctx, "user"), "query string"))

	tr, closers := jaeger.NewTracer("test", jaeger.NewConstSampler(true), jaeger.NewNullReporter())
	defer func() { _ = closers.Close() }()
	opentracing.SetGlobalTracer(tr)

	_, ctxWithTrace := opentracing.StartSpanFromContext(ctx, "operation")
	{
		activity := generateActivityDescription(ctxWithTrace, "query string")
		assert.Contains(t, activity, "traceID=")
		assert.Contains(t, activity, " query=query string")
	}

	{
		activity := generateActivityDescription(user.InjectOrgID(ctxWithTrace, "fake"), "query string")
		assert.Contains(t, activity, "traceID=")
		assert.Contains(t, activity, " tenant=fake query=query string")
	}
}
