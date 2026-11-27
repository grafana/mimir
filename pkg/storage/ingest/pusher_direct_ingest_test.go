// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/tenant"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/grpc/codes"

	"github.com/grafana/mimir/pkg/mimirpb"
)

type directIngestPusherFunc struct {
	pusherFunc
	ingest func(ctx context.Context, content []byte, version int) error
}

func (p directIngestPusherFunc) IngestRecord(ctx context.Context, content []byte, version int) error {
	return p.ingest(ctx, content, version)
}

// A pusher that accepts records gets each of them as it is, a tenant's in order and with its tenant, and the consumer
// skips those it couldn't read and those of client errors, and stops at a server error: on one goroutine all of them in
// order, on several those of the tenant with the error.
func TestPusherConsumer_DirectIngest(t *testing.T) {
	records := []testRecord{
		{version: 2, tenantID: "t1", content: []byte("first")},
		{version: 1, tenantID: "t2", content: []byte("malformed")},
		{version: 2, tenantID: "t1", content: []byte("client error")},
		{version: 2, tenantID: "t2", content: []byte("fourth")},
		{version: 2, tenantID: "t1", content: []byte("server error")},
		{version: 2, tenantID: "t1", content: []byte("never pushed")},
	}
	for _, concurrency := range []int{0, 1, 8} {
		t.Run(fmt.Sprintf("concurrency %d", concurrency), func(t *testing.T) {
			var (
				mu       sync.Mutex
				pushed   []string
				byTenant = map[string][]string{}
			)
			pusher := directIngestPusherFunc{
				pusherFunc: func(context.Context, *mimirpb.WriteRequest) error {
					t.Error("direct ingest should get raw records")
					return nil
				},
				ingest: func(ctx context.Context, content []byte, version int) error {
					tenantID, err := tenant.TenantID(ctx)
					if err != nil {
						return err
					}
					mu.Lock()
					pushed = append(pushed, fmt.Sprintf("%s/%d/%s", tenantID, version, content))
					byTenant[tenantID] = append(byTenant[tenantID], string(content))
					mu.Unlock()
					switch string(content) {
					case "malformed":
						return fmt.Errorf("%w: bad bytes", ErrMalformedRecord)
					case "client error":
						return ingesterError(mimirpb.ERROR_CAUSE_BAD_DATA, codes.InvalidArgument, "bad sample")
					case "server error":
						return errors.New("disk full")
					}
					return nil
				},
			}
			c := NewPusherConsumer(pusher, true, KafkaConfig{IngestionConcurrencyMax: concurrency}, NewPusherConsumerMetrics(prometheus.NewPedanticRegistry()), log.NewNopLogger())
			err := c.Consume(context.Background(), kafkaRecordsAll(t, records))
			require.ErrorContains(t, err, "disk full")
			require.Equal(t, []string{"first", "client error", "server error"}, byTenant["t1"])
			if concurrency <= 1 {
				require.Equal(t, []string{"t1/2/first", "t2/1/malformed", "t1/2/client error", "t2/2/fourth", "t1/2/server error"}, pushed)
			} else {
				// Another tenant's records may be pushed or not when the error stops the batch, in order.
				require.Contains(t, [][]string{nil, {"malformed"}, {"malformed", "fourth"}}, byTenant["t2"])
			}
		})
	}

	// With direct ingest disabled, records still use the decoded path.
	decoded := 0
	pusher := directIngestPusherFunc{
		pusherFunc: func(context.Context, *mimirpb.WriteRequest) error { decoded++; return nil },
		ingest: func(context.Context, []byte, int) error {
			t.Fatal("direct ingest is disabled")
			return nil
		},
	}
	content, err := (&mimirpb.WriteRequest{Timeseries: []mimirpb.PreallocTimeseries{mockPreallocTimeseries("series_1")}}).Marshal()
	require.NoError(t, err)
	c := NewPusherConsumer(pusher, false, KafkaConfig{}, NewPusherConsumerMetrics(prometheus.NewPedanticRegistry()), log.NewNopLogger())
	require.NoError(t, c.Consume(context.Background(), kafkaRecordsAll(t, []testRecord{{version: 1, tenantID: "t1", content: content}})))
	require.Equal(t, 1, decoded)
}

func TestPusherConsumer_DirectIngestStopsDispatchOnFailure(t *testing.T) {
	serverErr := errors.New("disk full")
	pusher := directIngestPusherFunc{
		ingest: func(context.Context, []byte, int) error { return serverErr },
	}
	c := NewPusherConsumer(pusher, true, KafkaConfig{IngestionConcurrencyMax: 8}, NewPusherConsumerMetrics(prometheus.NewPedanticRegistry()), log.NewNopLogger())

	records := make([]testRecord, 256)
	for i := range records {
		records[i] = testRecord{version: 2, tenantID: "t1", content: []byte("data")}
	}
	base := kafkaRecordsAll(t, records)
	visited := 0
	err := c.Consume(t.Context(), func(yield func(*kgo.Record) bool) {
		for rec := range base {
			visited++
			if !yield(rec) {
				return
			}
		}
	})

	require.ErrorIs(t, err, serverErr)
	require.Less(t, visited, len(records))
}
