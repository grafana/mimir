package ingest

import (
	"context"
	"testing"

	"github.com/grafana/dskit/tenant"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
)

type pusherFunc func(context.Context, *mimirpb.WriteRequest) (*mimirpb.WriteResponse, error)

func (p pusherFunc) Push(ctx context.Context, request *mimirpb.WriteRequest) (*mimirpb.WriteResponse, error) {
	return p(ctx, request)
}

func TestIngesterConsumer(t *testing.T) {
	const tenantID = "t1"
	writeReqs := []*mimirpb.WriteRequest{
		{Timeseries: []mimirpb.PreallocTimeseries{mockPreallocTimeseries("series_1"), mockPreallocTimeseries("series_2")}},
		{Timeseries: []mimirpb.PreallocTimeseries{mockPreallocTimeseries("series_3")}},
		{Timeseries: []mimirpb.PreallocTimeseries{mockPreallocTimeseries("series_4")}},
	}

	wrBytes := make([][]byte, len(writeReqs))
	for i, wr := range writeReqs {
		var err error
		wrBytes[i], err = wr.Marshal()
		require.NoError(t, err)
	}

	type response struct {
		*mimirpb.WriteResponse
		err error
	}

	okResponse := response{WriteResponse: &mimirpb.WriteResponse{}}

	testCases := map[string]struct {
		records     []Record
		responses   []response
		expectedWRs []*mimirpb.WriteRequest
		expErr      string
	}{
		"single record": {
			records: []Record{
				{Content: wrBytes[0], TenantID: tenantID},
			},
			responses: []response{
				okResponse,
			},
			expectedWRs: writeReqs[0:1],
		},
		"multiple records": {
			records: []Record{
				{Content: wrBytes[0], TenantID: tenantID},
				{Content: wrBytes[1], TenantID: tenantID},
				{Content: wrBytes[2], TenantID: tenantID},
			},
			responses: []response{
				okResponse,
				okResponse,
				okResponse,
			},
			expectedWRs: writeReqs[0:3],
		},
		"unparsable record": {
			records: []Record{
				{Content: wrBytes[0], TenantID: tenantID},
				{Content: []byte{0}, TenantID: tenantID},
				{Content: wrBytes[1], TenantID: tenantID},
			},
			responses: []response{
				okResponse,
			},
			expectedWRs: writeReqs[0:1],
			expErr:      "parsing ingest consumer write request",
		},
		"failed processing of record": {
			records: []Record{
				{Content: wrBytes[0], TenantID: tenantID},
				{Content: wrBytes[1], TenantID: tenantID},
				{Content: wrBytes[2], TenantID: tenantID},
			},
			responses: []response{
				okResponse,
				{err: assert.AnError},
			},
			expectedWRs: writeReqs[0:2],
			expErr:      assert.AnError.Error(),
		},
		"failed processing of last record": {
			records: []Record{
				{Content: wrBytes[0], TenantID: tenantID},
				{Content: wrBytes[1], TenantID: tenantID},
			},
			responses: []response{
				okResponse,
				{err: assert.AnError},
			},
			expectedWRs: writeReqs[0:2],
			expErr:      assert.AnError.Error(),
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			receivedReqs := 0
			pusher := pusherFunc(func(ctx context.Context, request *mimirpb.WriteRequest) (*mimirpb.WriteResponse, error) {
				defer func() { receivedReqs++ }()
				require.GreaterOrEqual(t, len(tc.expectedWRs), receivedReqs+1)

				expectedWR := tc.expectedWRs[receivedReqs]
				for i, ts := range request.Timeseries {
					assert.Truef(t, ts.Equal(expectedWR.Timeseries[i].TimeSeries), "timeseries %d not equal; got %v, expected %v", i, ts, expectedWR.Timeseries[i].TimeSeries)
				}

				actualTenantID, err := tenant.TenantID(ctx)
				assert.NoError(t, err)
				assert.Equal(t, tenantID, actualTenantID)

				return tc.responses[receivedReqs].WriteResponse, tc.responses[receivedReqs].err
			})

			err := NewIngesterConsumer(pusher).Consume(context.Background(), tc.records)
			if tc.expErr == "" {
				assert.NoError(t, err)
			} else {
				assert.ErrorContains(t, err, tc.expErr)
			}
		})
	}
}
