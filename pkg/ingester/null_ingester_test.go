// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"strings"
	"testing"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/testkafka"
)

func TestNewNullIngester_RequiresIngestStorage(t *testing.T) {
	cfg := defaultIngesterTestConfig(t)
	_, err := NewNullIngester(cfg, log.NewNopLogger(), prometheus.NewRegistry())
	require.EqualError(t, err, "ingest storage must be enabled for null ingester")
}

func TestNewNullIngester_RequiresCompartments(t *testing.T) {
	cfg := defaultIngesterTestConfig(t)
	cfg.IngestStorageConfig.Enabled = true
	_, err := NewNullIngester(cfg, log.NewNopLogger(), prometheus.NewRegistry())
	require.EqualError(t, err, "compartments must be enabled for null ingester")
}

func TestNullIngester_StartsAndStops(t *testing.T) {
	ctx := context.Background()

	_, kafkaAddr := testkafka.CreateCluster(t, 1, "test-comp-0")

	cfg := defaultIngesterTestConfig(t)
	cfg.IngesterRing.InstanceID = "ingester-zone-a-0"
	cfg.IngestStorageConfig.Enabled = true
	cfg.IngestStorageConfig.Compartments.Enabled = true
	cfg.IngestStorageConfig.Compartments.NumCompartments = 1
	cfg.IngestStorageConfig.Compartments.TopicFormat = "test-comp-<compartment-id>"
	cfg.IngestStorageConfig.Compartments.ReadCompartmentID = 0
	cfg.IngestStorageConfig.Compartments.WriteKafkaAddressFormat = kafkaAddr

	ni, err := NewNullIngester(cfg, log.NewNopLogger(), prometheus.NewRegistry())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(ctx, ni))
	require.NoError(t, services.StopAndAwaitTerminated(ctx, ni))
}

func TestNullIngester_PushToStorageAndReleaseRequest_TracksIngestedSamples(t *testing.T) {
	reg := prometheus.NewRegistry()
	ni := &NullIngester{
		logger:  log.NewNopLogger(),
		metrics: newIngesterMetrics(reg, false, func() *InstanceLimits { return nil }, nil, nil, nil),
	}

	push := func(t *testing.T, tenantID string, req *mimirpb.WriteRequest) {
		t.Helper()
		ctx := user.InjectOrgID(context.Background(), tenantID)
		require.NoError(t, ni.PushToStorageAndReleaseRequest(ctx, req))
	}

	// Tenant A: 2 series, 3 float samples + 1 histogram = 4 samples.
	push(t, "tenant-a", &mimirpb.WriteRequest{
		Timeseries: []mimirpb.PreallocTimeseries{
			{TimeSeries: &mimirpb.TimeSeries{
				Samples: []mimirpb.Sample{{Value: 1, TimestampMs: 1}, {Value: 2, TimestampMs: 2}},
			}},
			{TimeSeries: &mimirpb.TimeSeries{
				Samples:    []mimirpb.Sample{{Value: 3, TimestampMs: 3}},
				Histograms: []mimirpb.Histogram{{Timestamp: 4}},
			}},
		},
	})

	// Tenant B: 1 series, 1 sample.
	push(t, "tenant-b", &mimirpb.WriteRequest{
		Timeseries: []mimirpb.PreallocTimeseries{
			{TimeSeries: &mimirpb.TimeSeries{
				Samples: []mimirpb.Sample{{Value: 1, TimestampMs: 1}},
			}},
		},
	})

	// Push again for tenant A to ensure the counter accumulates: 1 sample.
	push(t, "tenant-a", &mimirpb.WriteRequest{
		Timeseries: []mimirpb.PreallocTimeseries{
			{TimeSeries: &mimirpb.TimeSeries{
				Samples: []mimirpb.Sample{{Value: 5, TimestampMs: 5}},
			}},
		},
	})

	expected := `
		# HELP cortex_ingester_ingested_samples_total The total number of samples ingested per user.
		# TYPE cortex_ingester_ingested_samples_total counter
		cortex_ingester_ingested_samples_total{user="tenant-a"} 5
		cortex_ingester_ingested_samples_total{user="tenant-b"} 1
	`
	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(expected), "cortex_ingester_ingested_samples_total"))
}

func TestNullIngester_PushToStorageAndReleaseRequest_RequiresTenantInContext(t *testing.T) {
	ni := &NullIngester{
		logger:  log.NewNopLogger(),
		metrics: newIngesterMetrics(prometheus.NewRegistry(), false, func() *InstanceLimits { return nil }, nil, nil, nil),
	}

	err := ni.PushToStorageAndReleaseRequest(context.Background(), &mimirpb.WriteRequest{})
	require.Error(t, err)
}
