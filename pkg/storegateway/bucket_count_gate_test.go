// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"context"
	"testing"
	"time"

	"github.com/grafana/dskit/gate"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/storegateway/storegatewaypb"
)

func TestBucketStore_CountGate(t *testing.T) {
	store, b1, _ := prepareSeriesCountsStore(t)
	countGate := gate.NewBlocking(1)
	store.countGate = timeoutGate{delegate: countGate, timeout: 100 * time.Millisecond}
	// The query gate is full the whole time: the count requests must not
	// need one of its slots.
	queryGate := gate.NewBlocking(1)
	require.NoError(t, queryGate.Start(t.Context()))
	defer queryGate.Done()
	store.queryGate = queryGate

	seriesCounts := func(ctx context.Context) error {
		_, err := store.countSeries(ctx, &storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String()}, MinTime: 0, MaxTime: 4 * hourMs})
		return err
	}
	streamedSeriesCounts := func(ctx context.Context) error {
		return store.SeriesCounts(&storegatewaypb.SeriesCountsRequest{BlockIds: []string{b1.String()}, MinTime: 0, MaxTime: 4 * hourMs}, &seriesCountsServerMock{ctx: ctx})
	}

	for name, count := range map[string]func(context.Context) error{"SeriesCounts": seriesCounts, "SeriesCounts streamed": streamedSeriesCounts} {
		t.Run(name, func(t *testing.T) {
			require.NoError(t, count(t.Context()), "a free count gate lets the request run with the query gate full")

			// One request holds the only slot.
			require.NoError(t, countGate.Start(t.Context()))

			err := count(t.Context())
			assert.Equal(t, codes.Unavailable, status.Code(err), "past the queue timeout the querier is told to ask another store-gateway: %v", err)

			ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
			err = count(ctx)
			cancel()
			assert.Equal(t, codes.DeadlineExceeded, status.Code(err), "the request's own deadline ends the wait: %v", err)

			// A request that waits runs once the slot is freed.
			result := make(chan error, 1)
			go func() { result <- count(t.Context()) }()
			time.Sleep(20 * time.Millisecond)
			countGate.Done()
			require.NoError(t, <-result)
		})
	}
}
