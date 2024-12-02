package ingester

import (
	"context"
	"encoding/base64"
	"fmt"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/test"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/util/globalerror"

	"github.com/grafana/mimir/pkg/mimirpb"
)

func Test_PushSingleWriteRequest(t *testing.T) {
	userID := "190093"
	binaryData, err := base64.StdEncoding.DecodeString(dumpedTimeseries)
	require.NoError(t, err)

	var ts mimirpb.TimeSeries
	err = proto.Unmarshal(binaryData, &ts)
	require.NoError(t, err)

	labelAdapters := make([][]mimirpb.LabelAdapter, len(ts.Samples))
	for i := 0; i < len(labelAdapters); i++ {
		labelAdapters[i] = make([]mimirpb.LabelAdapter, len(ts.Labels))
		copy(labelAdapters[i], ts.Labels)
	}

	writeRequests := []*mimirpb.WriteRequest{
		mimirpb.ToWriteRequest(
			labelAdapters,
			ts.Samples,
			nil,
			nil,
			mimirpb.API,
		),
	}

	if err := push(t, userID, writeRequests); err != nil {
		handledErr := mapPushErrorToErrorWithStatus(err)
		errWithStatus, ok := handledErr.(globalerror.ErrorWithStatus)
		require.True(t, ok)
		fmt.Println(errWithStatus)
	}
}

func Test_PushMultipleWriteRequest(t *testing.T) {
	userID := "190093"
	binaryData, err := base64.StdEncoding.DecodeString(dumpedTimeseries)
	require.NoError(t, err)

	var ts mimirpb.TimeSeries
	err = proto.Unmarshal(binaryData, &ts)
	require.NoError(t, err)

	writeRequests := make([]*mimirpb.WriteRequest, len(ts.Samples))
	for i := 0; i < len(ts.Samples); i++ {
		labelAdapter := make([]mimirpb.LabelAdapter, len(ts.Labels))
		copy(labelAdapter, ts.Labels)
		writeRequests[i] = mimirpb.ToWriteRequest(
			[][]mimirpb.LabelAdapter{labelAdapter},
			[]mimirpb.Sample{ts.Samples[i]},
			nil,
			nil,
			mimirpb.API,
		)
	}

	if err := push(t, userID, writeRequests); err != nil {
		handledErr := mapPushErrorToErrorWithStatus(err)
		errWithStatus, ok := handledErr.(globalerror.ErrorWithStatus)
		require.True(t, ok)
		fmt.Println(errWithStatus)
	}

}

func push(t *testing.T, userID string, reqs []*mimirpb.WriteRequest) error {
	registry := prometheus.NewRegistry()

	// Create a mocked ingester
	cfg := defaultIngesterTestConfig(t)
	cfg.IngesterRing.ReplicationFactor = 1
	limits := defaultLimitsTestConfig()
	i, err := prepareIngesterWithBlocksStorageAndLimits(t, cfg, limits, nil, "", registry)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	ctx := user.InjectOrgID(context.Background(), userID)

	// Wait until the ingester is healthy and owns tokens. Note that the timeout here is set
	// such that it is longer than the MinReadyDuration configuration for the ingester ring.
	test.Poll(t, time.Second, nil, func() interface{} {
		return i.lifecycler.CheckReady(context.Background())
	})

	// Push timeseries
	for _, req := range reqs {
		// Push metrics to the ingester. Override the default cleanup method of mimirpb.ReuseSlice with a no-op one.
		err := i.PushWithCleanup(ctx, req, func() {})
		if err != nil {
			return err
		}
	}
	return nil
}
