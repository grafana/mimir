// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"

	"github.com/grafana/mimir/pkg/mimirpb"
)

type pusherFunc func(context.Context, *mimirpb.WriteRequest) (*mimirpb.WriteResponse, error)

func (p pusherFunc) Push(ctx context.Context, request *mimirpb.WriteRequest) (*mimirpb.WriteResponse, error) {
	return p(ctx, request)
}

//
// func TestPusherConsumer(t *testing.T) {
// 	const tenantID = "t1"
// 	writeReqs := []*mimirpb.WriteRequest{
// 		{Timeseries: []mimirpb.PreallocTimeseries{mockPreallocTimeseries("series_1"), mockPreallocTimeseries("series_2")}},
// 		{Timeseries: []mimirpb.PreallocTimeseries{mockPreallocTimeseries("series_3")}},
// 		{Timeseries: []mimirpb.PreallocTimeseries{mockPreallocTimeseries("series_4")}},
// 		{Timeseries: []mimirpb.PreallocTimeseries{mockPreallocTimeseries("series_5")}},
// 		{Timeseries: []mimirpb.PreallocTimeseries{mockPreallocTimeseries("series_6")}},
// 	}
//
// 	wrBytes := make([][]byte, len(writeReqs))
// 	for i, wr := range writeReqs {
// 		var err error
// 		wrBytes[i], err = wr.Marshal()
// 		require.NoError(t, err)
// 	}
//
// 	type response struct {
// 		*mimirpb.WriteResponse
// 		err error
// 	}
//
// 	okResponse := response{WriteResponse: &mimirpb.WriteResponse{}}
//
// 	testCases := map[string]struct {
// 		records     []record
// 		responses   []response
// 		expectedWRs []*mimirpb.WriteRequest
// 	}{
// 		"single record": {
// 			records: []record{
// 				{content: wrBytes[0], tenantID: tenantID},
// 			},
// 			responses: []response{
// 				okResponse,
// 			},
// 			expectedWRs: writeReqs[0:1],
// 		},
// 		"multiple writeRequests": {
// 			records: []record{
// 				{content: wrBytes[0], tenantID: tenantID},
// 				{content: wrBytes[1], tenantID: tenantID},
// 				{content: wrBytes[2], tenantID: tenantID},
// 			},
// 			responses: []response{
// 				okResponse,
// 				okResponse,
// 				okResponse,
// 			},
// 			expectedWRs: writeReqs[0:3],
// 		},
// 		"unparsable record": {
// 			records: []record{
// 				{content: wrBytes[0], tenantID: tenantID},
// 				{content: []byte{0}, tenantID: tenantID},
// 				{content: wrBytes[1], tenantID: tenantID},
// 			},
// 			responses: []response{
// 				okResponse,
// 				okResponse,
// 			},
// 			expectedWRs: writeReqs[0:2],
// 			expErr:      "",
// 		},
// 		"failed processing of record": {
// 			records: []record{
// 				{content: wrBytes[0], tenantID: tenantID},
// 				{content: wrBytes[1], tenantID: tenantID},
// 				{content: wrBytes[2], tenantID: tenantID},
// 			},
// 			responses: []response{
// 				okResponse,
// 				{err: assert.AnError},
// 			},
// 			expectedWRs: writeReqs[0:2],
// 			expErr:      assert.AnError.Error(),
// 		},
// 		"failed processing of last record": {
// 			records: []record{
// 				{content: wrBytes[0], tenantID: tenantID},
// 				{content: wrBytes[1], tenantID: tenantID},
// 			},
// 			responses: []response{
// 				okResponse,
// 				{err: assert.AnError},
// 			},
// 			expectedWRs: writeReqs[0:2],
// 			expErr:      assert.AnError.Error(),
// 		},
// 		"failed processing & failed unmarshalling": {
// 			records: []record{
// 				{content: wrBytes[0], tenantID: tenantID},
// 				{content: wrBytes[1], tenantID: tenantID},
// 				{content: []byte{0}, tenantID: tenantID},
// 			},
// 			responses: []response{
// 				okResponse,
// 				{err: assert.AnError},
// 			},
// 			expectedWRs: writeReqs[0:2],
// 			expErr:      assert.AnError.Error(),
// 		},
// 		"no writeRequests": {},
// 		"ingester client error": {
// 			records: []record{
// 				{content: wrBytes[0], tenantID: tenantID},
// 				{content: wrBytes[1], tenantID: tenantID},
// 				{content: wrBytes[2], tenantID: tenantID},
// 			},
// 			responses: []response{
// 				{err: ingesterError(mimirpb.BAD_DATA, codes.InvalidArgument, "ingester test error")},
// 				{err: ingesterError(mimirpb.BAD_DATA, codes.Unknown, "ingester test error")}, // status code doesn't matter
// 				okResponse,
// 			},
// 			expectedWRs: writeReqs[0:3],
// 			expErr:      "", // since all fof those were client errors, we don't return an error
// 		},
// 		"ingester server error": {
// 			records: []record{
// 				{content: wrBytes[0], tenantID: tenantID},
// 				{content: wrBytes[1], tenantID: tenantID},
// 				{content: wrBytes[2], tenantID: tenantID},
// 				{content: wrBytes[3], tenantID: tenantID},
// 				{content: wrBytes[4], tenantID: tenantID},
// 			},
// 			responses: []response{
// 				{err: ingesterError(mimirpb.BAD_DATA, codes.InvalidArgument, "ingester test error")},
// 				{err: ingesterError(mimirpb.TSDB_UNAVAILABLE, codes.Unavailable, "ingester internal error")},
// 			},
// 			expectedWRs: writeReqs[0:2], // the rest of the requests are not attempted
// 			expErr:      "ingester internal error",
// 		},
// 	}
//
// 	for name, tc := range testCases {
// 		tc := tc
// 		t.Run(name, func(t *testing.T) {
// 			receivedReqs := 0
// 			pusher := pusherFunc(func(ctx context.Context, request *mimirpb.WriteRequest) (*mimirpb.WriteResponse, error) {
// 				defer func() { receivedReqs++ }()
// 				require.GreaterOrEqualf(t, len(tc.expectedWRs), receivedReqs+1, "received more requests (%d) than expected (%d)", receivedReqs+1, len(tc.expectedWRs))
//
// 				expectedWR := tc.expectedWRs[receivedReqs]
// 				for i, ts := range request.Timeseries {
// 					assert.Truef(t, ts.Equal(expectedWR.Timeseries[i].TimeSeries), "timeseries %d not equal; got %v, expected %v", i, ts, expectedWR.Timeseries[i].TimeSeries)
// 				}
//
// 				actualTenantID, err := tenant.TenantID(ctx)
// 				assert.NoError(t, err)
// 				assert.Equal(t, tenantID, actualTenantID)
//
// 				return tc.responses[receivedReqs].WriteResponse, tc.responses[receivedReqs].err
// 			})
// 			c := newPusherConsumer(pusher, prometheus.NewPedanticRegistry(), log.NewNopLogger())
// 			err := c.consume(context.Background(), tc.records)
// 			if tc.expErr == "" {
// 				assert.NoError(t, err)
// 			} else {
// 				assert.ErrorContains(t, err, tc.expErr)
// 			}
// 		})
// 	}
// }
//
// // ingesterError mimics how the ingester construct errors
// func ingesterError(cause mimirpb.ErrorCause, statusCode codes.Code, message string) error {
// 	errorDetails := &mimirpb.ErrorDetails{Cause: cause}
// 	statWithDetails, err := status.New(statusCode, message).WithDetails(errorDetails)
// 	if err != nil {
// 		panic(err)
// 	}
// 	return statWithDetails.Err()
// }
