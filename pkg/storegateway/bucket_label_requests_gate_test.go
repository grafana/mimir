// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/grafana/dskit/gate"
	"github.com/grafana/dskit/grpcutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
)

// exhaustedQueryGate returns a query gate whose only slot is already held, so the next
// acquisition times out immediately. Holding the slot directly keeps the test deterministic:
// there is no need to park a second request in flight and race against it.
func exhaustedQueryGate(t *testing.T) gate.Gate {
	t.Helper()

	g := timeoutGate{timeout: time.Millisecond, delegate: gate.NewBlocking(1)}
	require.NoError(t, g.Start(context.Background()))
	t.Cleanup(g.Done)

	return g
}

// TestBucketStore_LabelRequests_QueryGate asserts that the label and search endpoints acquire
// the query gate only when -blocks-storage.bucket-store.gate-label-requests is enabled, and
// that a gate timeout is reported as an instance limit (Unavailable) rather than as an
// internal error. Series has always been gated and is covered by
// TestBucketStore_Series_TimeoutGate.
func TestBucketStore_LabelRequests_QueryGate(t *testing.T) {
	ctx := context.Background()

	endpoints := map[string]func(bs *BucketStore) error{
		"LabelNames": func(bs *BucketStore) error {
			_, err := bs.LabelNames(ctx, &storepb.LabelNamesRequest{End: math.MaxInt64})
			return err
		},
		"LabelValues": func(bs *BucketStore) error {
			_, err := bs.LabelValues(ctx, &storepb.LabelValuesRequest{Label: "a", End: math.MaxInt64})
			return err
		},
		"SearchLabelNames": func(bs *BucketStore) error {
			req := &storepb.SearchLabelNamesRequest{End: math.MaxInt64, Filter: &storepb.SearchFilter{}}
			return bs.SearchLabelNames(req, &mockSearchLabelNamesServer{ctx: ctx})
		},
		"SearchLabelValues": func(bs *BucketStore) error {
			req := &storepb.SearchLabelValuesRequest{Label: "a", End: math.MaxInt64, Filter: &storepb.SearchFilter{}}
			return bs.SearchLabelValues(req, &mockSearchLabelValuesServer{ctx: ctx})
		},
	}

	for name, call := range endpoints {
		t.Run(name, func(t *testing.T) {
			t.Run("disabled: an exhausted gate does not affect the request", func(t *testing.T) {
				bs := prepareSearchTestStore(t)
				bs.gateLabelRequests = false
				bs.queryGate = exhaustedQueryGate(t)

				require.NoError(t, call(bs))
			})

			t.Run("enabled: a gate timeout is reported as an instance limit", func(t *testing.T) {
				bs := prepareSearchTestStore(t)
				bs.gateLabelRequests = true
				bs.queryGate = exhaustedQueryGate(t)

				err := call(bs)
				require.Error(t, err)

				s, ok := grpcutil.ErrorToStatus(err)
				require.True(t, ok, err)
				require.Equal(t, codes.Unavailable, s.Code(), err)
				require.Len(t, s.Details(), 1, err)
				require.Equal(t, mimirpb.ERROR_CAUSE_INSTANCE_LIMIT, s.Details()[0].(*mimirpb.ErrorDetails).GetCause(), err)
			})
		})
	}
}
