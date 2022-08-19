// SPDX-License-Identifier: AGPL-3.0-only

package engine

import (
	"context"
	"testing"

	"github.com/opentracing/opentracing-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/jaeger-client-go"
	"github.com/weaveworks/common/user"
)

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
