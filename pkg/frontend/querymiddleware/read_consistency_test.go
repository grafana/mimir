// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/user"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"

	querierapi "github.com/grafana/mimir/pkg/querier/api"
	"github.com/grafana/mimir/pkg/storage/ingest"
	"github.com/grafana/mimir/pkg/util/testkafka"
)

func TestReadConsistencyRoundTripper(t *testing.T) {
	const (
		topic         = "test"
		numPartitions = 10
		tenantID      = "user-1"
	)

	tests := map[string]struct {
		limits          Limits
		reqConsistency  string
		expectedOffsets bool
	}{
		"should not inject offsets if default read consistency is 'eventual' and request has explicitly requested any consistency level": {
			limits:          mockLimits{ingestStorageReadConsistency: querierapi.ReadConsistencyEventual},
			expectedOffsets: false,
		},
		"should not inject offsets if default read consistency is 'strong' and request has explicitly requested 'eventual' consistency": {
			limits:          mockLimits{ingestStorageReadConsistency: querierapi.ReadConsistencyStrong},
			reqConsistency:  querierapi.ReadConsistencyEventual,
			expectedOffsets: false,
		},
		"should inject offsets if default read consistency is 'eventual' but request has explicitly requested 'strong' consistency": {
			limits:          mockLimits{ingestStorageReadConsistency: querierapi.ReadConsistencyEventual},
			reqConsistency:  querierapi.ReadConsistencyStrong,
			expectedOffsets: true,
		},
		"should inject offsets if default read consistency is 'strong' and request has not explicitly requested any consistency level": {
			limits:          mockLimits{ingestStorageReadConsistency: querierapi.ReadConsistencyStrong},
			expectedOffsets: true,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			// Capture the downstream HTTP request.
			var downstreamReq *http.Request
			downstream := RoundTripFunc(func(req *http.Request) (*http.Response, error) {
				downstreamReq = req
				return nil, nil
			})

			ctx := context.Background()
			logger := log.NewNopLogger()

			_, clusterAddr := testkafka.CreateCluster(t, numPartitions, topic)

			cfg := ingest.KafkaConfig{}
			flagext.DefaultValues(&cfg)
			cfg.Address = clusterAddr
			cfg.Topic = topic

			// Write some records to different partitions.
			writeClient, err := ingest.NewKafkaWriterClient(cfg, 1, logger, nil)
			require.NoError(t, err)

			writeRes := writeClient.ProduceSync(ctx,
				&kgo.Record{Partition: 0},
				&kgo.Record{Partition: 0},
				&kgo.Record{Partition: 0},
				&kgo.Record{Partition: 1},
				&kgo.Record{Partition: 1},
				&kgo.Record{Partition: 2},
			)
			require.NoError(t, writeRes.FirstErr())

			// Create the topic offsets reader.
			readClient, err := ingest.NewKafkaReaderClient(cfg, nil, logger)
			require.NoError(t, err)

			reader := ingest.NewTopicOffsetsReader(readClient, topic, 100*time.Millisecond, nil, logger)
			require.NoError(t, services.StartAndAwaitRunning(ctx, reader))
			t.Cleanup(func() {
				require.NoError(t, services.StopAndAwaitTerminated(ctx, reader))
			})

			// Send an HTTP request through the roundtripper.
			req := httptest.NewRequest("GET", "/", nil)
			req = req.WithContext(user.InjectOrgID(req.Context(), tenantID))

			if testData.reqConsistency != "" {
				req = req.WithContext(querierapi.ContextWithReadConsistency(req.Context(), testData.reqConsistency))
			}

			rt := newReadConsistencyRoundTripper(downstream, reader, testData.limits, log.NewNopLogger())
			_, err = rt.RoundTrip(req)
			require.NoError(t, err)

			require.NotNil(t, downstreamReq)

			if testData.expectedOffsets {
				offsets := querierapi.EncodedOffsets(downstreamReq.Header.Get(querierapi.ReadConsistencyOffsetsHeader))

				actual, ok := offsets.Lookup(0)
				assert.True(t, ok)
				assert.Equal(t, int64(2), actual)

				actual, ok = offsets.Lookup(1)
				assert.True(t, ok)
				assert.Equal(t, int64(1), actual)

				actual, ok = offsets.Lookup(2)
				assert.True(t, ok)
				assert.Equal(t, int64(0), actual)

				actual, ok = offsets.Lookup(3)
				assert.False(t, ok)
			} else {
				assert.Empty(t, downstreamReq.Header.Get(querierapi.ReadConsistencyOffsetsHeader))
			}
		})
	}
}
