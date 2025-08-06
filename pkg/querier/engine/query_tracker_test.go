// SPDX-License-Identifier: AGPL-3.0-only

package engine

import (
	"context"
	"os"
	"testing"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/tracing"
	"github.com/grafana/dskit/user"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	// Install OTel tracing, we need it for the tests.
	os.Setenv("OTEL_TRACES_EXPORTER", "none")
	_, err := tracing.NewOTelFromEnv("test", log.NewNopLogger())
	if err != nil {
		panic(err)
	}
}

func TestQueryTrackerUnlimitedMaxConcurrency(t *testing.T) {
	qt := newQueryTracker(nil)
	require.Equal(t, -1, qt.GetMaxConcurrent())
}

func TestQueryTrackerWithNilActivityTrackerInsertDoesntAllocate(t *testing.T) {
	qt := newQueryTracker(nil)

	assert.Zero(t, testing.AllocsPerRun(1000, func() {
		_, _ = qt.Insert(context.Background(), "query string")
	}))
}

func TestActivityDescription(t *testing.T) {
	ctx := context.Background()
	assert.Equal(t, "query=query string", generateActivityDescription(ctx, "query string"))
	assert.Equal(t, "tenant=user query=query string", generateActivityDescription(user.InjectOrgID(ctx, "user"), "query string"))

	ctxWithTrace, _ := tracer.Start(ctx, "operation")
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
