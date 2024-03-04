// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"
	"testing"

	"github.com/go-kit/log"
	"github.com/gogo/status"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"github.com/grafana/dskit/tenant"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/ingest/ingestpb"
)

type pusherFunc func(context.Context, *mimirpb.WriteRequest) (*mimirpb.WriteResponse, error)

func (p pusherFunc) Push(ctx context.Context, request *mimirpb.WriteRequest) (*mimirpb.WriteResponse, error) {
	return p(ctx, request)
}

func marshal(wr *mimirpb.WriteRequest) []byte {
	d, err := wr.Marshal()
	if err != nil {
		panic(err.Error())
	}
	return d
}

func unmarshal(data []byte) *mimirpb.WriteRequest {
	wr := &mimirpb.WriteRequest{}
	err := wr.Unmarshal(data)
	if err != nil {
		panic(err.Error())
	}
	return wr
}

func TestPusherConsumer(t *testing.T) {
	const tenantID = "t1"
	writeReqs := []*mimirpb.WriteRequest{
		{Timeseries: []mimirpb.PreallocTimeseries{mockPreallocTimeseries("series_1"), mockPreallocTimeseries("series_2")}},
		{Timeseries: []mimirpb.PreallocTimeseries{mockPreallocTimeseries("series_3")}},
		{Timeseries: []mimirpb.PreallocTimeseries{mockPreallocTimeseries("series_4")}},
		{Timeseries: []mimirpb.PreallocTimeseries{mockPreallocTimeseries("series_5")}},
		{Timeseries: []mimirpb.PreallocTimeseries{mockPreallocTimeseries("series_6")}},
	}

	type response struct {
		*mimirpb.WriteResponse
		err error
	}

	okResponse := response{WriteResponse: &mimirpb.WriteResponse{}}

	testCases := map[string]struct {
		segment     *Segment
		responses   []response
		expectedWRs []*mimirpb.WriteRequest
		expErr      string
	}{
		"single write request": {
			segment: &Segment{
				Data: &ingestpb.Segment{
					Pieces: []*ingestpb.Piece{
						{Data: marshal(writeReqs[0]), TenantId: tenantID},
					},
				},
			},
			responses: []response{
				okResponse,
			},
			expectedWRs: writeReqs[0:1],
		},
		"multiple write requests": {
			segment: &Segment{
				Data: &ingestpb.Segment{
					Pieces: []*ingestpb.Piece{
						{Data: marshal(writeReqs[0]), TenantId: tenantID},
						{Data: marshal(writeReqs[1]), TenantId: tenantID},
						{Data: marshal(writeReqs[2]), TenantId: tenantID},
					},
				},
			},
			responses: []response{
				okResponse,
				okResponse,
				okResponse,
			},
			expectedWRs: writeReqs[0:3],
		},
		"failed processing of write request": {
			segment: &Segment{
				Data: &ingestpb.Segment{
					Pieces: []*ingestpb.Piece{
						{Data: marshal(writeReqs[0]), TenantId: tenantID},
						{Data: marshal(writeReqs[1]), TenantId: tenantID},
						{Data: marshal(writeReqs[2]), TenantId: tenantID},
					},
				},
			},
			responses: []response{
				okResponse,
				{err: assert.AnError},
			},
			expectedWRs: writeReqs[0:2],
			expErr:      assert.AnError.Error(),
		},
		"failed processing of last write request": {
			segment: &Segment{
				Data: &ingestpb.Segment{
					Pieces: []*ingestpb.Piece{
						{Data: marshal(writeReqs[0]), TenantId: tenantID},
						{Data: marshal(writeReqs[1]), TenantId: tenantID},
					},
				},
			},
			responses: []response{
				okResponse,
				{err: assert.AnError},
			},
			expectedWRs: writeReqs[0:2],
			expErr:      assert.AnError.Error(),
		},
		"ingester client error": {
			segment: &Segment{
				Data: &ingestpb.Segment{
					Pieces: []*ingestpb.Piece{
						{Data: marshal(writeReqs[0]), TenantId: tenantID},
						{Data: marshal(writeReqs[1]), TenantId: tenantID},
						{Data: marshal(writeReqs[2]), TenantId: tenantID},
					},
				},
			},
			responses: []response{
				{err: ingesterError(mimirpb.BAD_DATA, codes.InvalidArgument, "ingester test error")},
				{err: ingesterError(mimirpb.BAD_DATA, codes.Unknown, "ingester test error")}, // status code doesn't matter
				okResponse,
			},
			expectedWRs: writeReqs[0:3],
			expErr:      "", // since all fof those were client errors, we don't return an error
		},
		"ingester server error": {
			segment: &Segment{
				Data: &ingestpb.Segment{
					Pieces: []*ingestpb.Piece{
						{Data: marshal(writeReqs[0]), TenantId: tenantID},
						{Data: marshal(writeReqs[1]), TenantId: tenantID},
						{Data: marshal(writeReqs[2]), TenantId: tenantID},
						{Data: marshal(writeReqs[3]), TenantId: tenantID},
						{Data: marshal(writeReqs[4]), TenantId: tenantID},
					},
				},
			},
			responses: []response{
				{err: ingesterError(mimirpb.BAD_DATA, codes.InvalidArgument, "ingester test error")},
				{err: ingesterError(mimirpb.TSDB_UNAVAILABLE, codes.Unavailable, "ingester internal error")},
			},
			expectedWRs: writeReqs[0:2], // the rest of the requests are not attempted
			expErr:      "ingester internal error",
		},
	}

	for name, tc := range testCases {
		tc := tc
		t.Run(name, func(t *testing.T) {
			receivedReqs := 0
			pusher := pusherFunc(func(ctx context.Context, request *mimirpb.WriteRequest) (*mimirpb.WriteResponse, error) {
				defer func() { receivedReqs++ }()
				require.GreaterOrEqualf(t, len(tc.expectedWRs), receivedReqs+1, "received more requests (%d) than expected (%d)", receivedReqs+1, len(tc.expectedWRs))

				expectedWR := tc.expectedWRs[receivedReqs]
				for i, ts := range request.Timeseries {
					assert.Truef(t, ts.Equal(expectedWR.Timeseries[i].TimeSeries), "timeseries %d not equal; got %v, expected %v", i, ts, expectedWR.Timeseries[i].TimeSeries)
				}

				actualTenantID, err := tenant.TenantID(ctx)
				assert.NoError(t, err)
				assert.Equal(t, tenantID, actualTenantID)

				return tc.responses[receivedReqs].WriteResponse, tc.responses[receivedReqs].err
			})
			c := newPusherConsumer(pusher, prometheus.NewPedanticRegistry(), log.NewNopLogger())
			err := c.consume(context.Background(), tc.segment)
			if tc.expErr == "" {
				assert.NoError(t, err)
			} else {
				assert.ErrorContains(t, err, tc.expErr)
			}
		})
	}
}

// ingesterError mimics how the ingester construct errors
func ingesterError(cause mimirpb.ErrorCause, statusCode codes.Code, message string) error {
	errorDetails := &mimirpb.ErrorDetails{Cause: cause}
	statWithDetails, err := status.New(statusCode, message).WithDetails(errorDetails)
	if err != nil {
		panic(err)
	}
	return statWithDetails.Err()
}
