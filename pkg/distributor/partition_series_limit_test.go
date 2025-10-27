// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"context"
	"testing"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/distributor/hlltracker"
	"github.com/grafana/mimir/pkg/mimirpb"
)

func TestPreKafkaPartitionLimitMiddleware_Disabled(t *testing.T) {
	// When tracker is nil (disabled), middleware should pass through
	d := &Distributor{
		hllTracker: nil,
	}

	called := false
	next := func(ctx context.Context, pushReq *Request) error {
		called = true
		return nil
	}

	middleware := d.preKafkaPartitionLimitMiddleware(next)

	ctx := user.InjectOrgID(context.Background(), "user1")
	req := NewParsedRequest(&mimirpb.WriteRequest{
		Timeseries: []mimirpb.PreallocTimeseries{
			mockPreallocTimeseries("series_1"),
		},
	})

	err := middleware(ctx, req)
	require.NoError(t, err)
	assert.True(t, called, "next should be called when tracker is disabled")
}

func TestPreKafkaPartitionLimitMiddleware_LimitDisabled(t *testing.T) {
	// When limit is 0 or negative, middleware should pass through
	cfg := hlltracker.Config{
		Enabled:               true,
		MaxSeriesPerPartition: 0, // Disabled
		TimeWindowMinutes:     20,
		HLLPrecision:          11,
	}

	tracker, err := hlltracker.New(cfg, log.NewNopLogger(), prometheus.NewRegistry())
	require.NoError(t, err)
	require.NoError(t, tracker.StartAsync(context.Background()))
	defer func() {
		tracker.StopAsync()
		_ = tracker.AwaitTerminated(context.Background())
	}()

	d := &Distributor{
		hllTracker: tracker,
	}

	called := false
	next := func(ctx context.Context, pushReq *Request) error {
		called = true
		return nil
	}

	middleware := d.preKafkaPartitionLimitMiddleware(next)

	ctx := user.InjectOrgID(context.Background(), "user1")
	req := NewParsedRequest(&mimirpb.WriteRequest{
		Timeseries: []mimirpb.PreallocTimeseries{
			mockPreallocTimeseries("series_1"),
		},
	})

	err = middleware(ctx, req)
	require.NoError(t, err)
	assert.True(t, called, "next should be called when limit is disabled")
}

func TestPreKafkaPartitionLimitMiddleware_NoSeries(t *testing.T) {
	// When there are no timeseries, middleware should pass through
	cfg := hlltracker.Config{
		Enabled:               true,
		MaxSeriesPerPartition: 1000,
		TimeWindowMinutes:     20,
		HLLPrecision:          11,
	}

	tracker, err := hlltracker.New(cfg, log.NewNopLogger(), prometheus.NewRegistry())
	require.NoError(t, err)
	require.NoError(t, tracker.StartAsync(context.Background()))
	defer func() {
		tracker.StopAsync()
		_ = tracker.AwaitTerminated(context.Background())
	}()

	d := &Distributor{
		hllTracker: tracker,
	}

	called := false
	next := func(ctx context.Context, pushReq *Request) error {
		called = true
		return nil
	}

	middleware := d.preKafkaPartitionLimitMiddleware(next)

	ctx := user.InjectOrgID(context.Background(), "user1")
	req := NewParsedRequest(&mimirpb.WriteRequest{
		Timeseries: []mimirpb.PreallocTimeseries{},
	})

	err = middleware(ctx, req)
	require.NoError(t, err)
	assert.True(t, called, "next should be called when there are no series")
}

// Note: Integration tests with actual partition ring are in distributor_test.go.
// These are unit tests focusing on the middleware logic without needing complex ring setup.

// Helper functions

func mockPreallocTimeseries(name string) mimirpb.PreallocTimeseries {
	return mimirpb.PreallocTimeseries{
		TimeSeries: &mimirpb.TimeSeries{
			Labels: []mimirpb.LabelAdapter{
				{Name: "__name__", Value: name},
			},
			Samples: []mimirpb.Sample{
				{Value: 1.0, TimestampMs: 1000},
			},
		},
	}
}
