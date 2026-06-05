// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"os"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/tenant"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/ingest"
)

// NullIngester is a lightweight ingester that consumes data from all write compartment VCs for
// a single read compartment topic and discards all received data. It's designed for load testing
// the write path without incurring the cost of TSDB writes.
type NullIngester struct {
	services.Service

	logger  log.Logger
	readers *ingest.CompartmentReaders
	metrics *ingesterMetrics

	subservices        *services.Manager
	subservicesWatcher *services.FailureWatcher
}

// NewNullIngester creates a NullIngester that reads from all write compartment VCs for the
// read compartment topic assigned to this ingester set and discards all consumed data. The
// partition is derived from the ingester ring instance ID (e.g. "ingester-0" → partition 0).
func NewNullIngester(cfg Config, logger log.Logger, reg prometheus.Registerer) (*NullIngester, error) {
	if !cfg.IngestStorageConfig.Enabled {
		return nil, errors.New("ingest storage must be enabled for null ingester")
	}

	compartmentsCfg := cfg.IngestStorageConfig.Compartments
	if !compartmentsCfg.Enabled {
		return nil, errors.New("compartments must be enabled for null ingester")
	}

	partitionID, err := ingest.IngesterPartitionID(cfg.IngesterRing.InstanceID)
	if err != nil {
		return nil, errors.Wrap(err, "calculating ingester partition ID")
	}

	offsetDir := cfg.NullIngesterOffsetDir
	if offsetDir == "" {
		offsetDir = os.TempDir()
	}

	kafkaCfg := cfg.IngestStorageConfig.KafkaConfig
	kafkaCfg.FallbackClientErrorSampleRate = cfg.ErrorSampleRate

	ni := &NullIngester{
		logger:  logger,
		metrics: newIngesterMetrics(reg, false, func() *InstanceLimits { return nil }, nil, nil, nil),
	}

	readers, err := ingest.NewCompartmentReaders(kafkaCfg, compartmentsCfg, partitionID, cfg.IngesterRing.InstanceID, offsetDir, ni, logger, reg)
	if err != nil {
		return nil, errors.Wrap(err, "creating compartment readers")
	}
	ni.readers = readers

	partitionRingKV := cfg.IngesterPartitionRing.KVStore.Mock
	if partitionRingKV == nil {
		partitionRingKV, err = kv.NewClient(cfg.IngesterPartitionRing.KVStore, ring.GetPartitionRingCodec(), kv.RegistererWithKVName(reg, PartitionRingName+"-lifecycler"), logger)
		if err != nil {
			return nil, errors.Wrap(err, "creating KV store for ingester partition ring")
		}
	}

	// Register into the read compartment's partition ring so the write path can route to this ingester.
	partitionLifecycler := ring.NewPartitionInstanceLifecycler(
		cfg.IngesterPartitionRing.ToLifecyclerConfig(partitionID, cfg.IngesterRing.InstanceID),
		PartitionRingName,
		CompartmentPartitionRingKey(compartmentsCfg.ReadCompartmentID),
		partitionRingKV,
		logger,
		prometheus.WrapRegistererWithPrefix("cortex_", reg))
	partitionLifecycler.BasicService = partitionLifecycler.WithName("null-ingester-partition-instance-lifecycler")

	ni.subservices, err = services.NewManager(readers, partitionLifecycler)
	if err != nil {
		return nil, errors.Wrap(err, "creating null ingester subservices")
	}
	ni.subservicesWatcher = services.NewFailureWatcher()

	ni.Service = services.NewBasicService(ni.starting, ni.running, ni.stopping).WithName("null-ingester")
	return ni, nil
}

// PushToStorageAndReleaseRequest implements ingest.Pusher. It discards the request after
// counting its samples per tenant.
func (ni *NullIngester) PushToStorageAndReleaseRequest(ctx context.Context, req *mimirpb.WriteRequest) error {
	defer req.FreeBuffer()

	tenantID, err := tenant.TenantID(ctx)
	if err != nil {
		return errors.Wrap(err, "extracting tenant ID")
	}

	var samples int
	for _, ts := range req.Timeseries {
		samples += len(ts.Samples) + len(ts.Histograms)
	}
	ni.metrics.ingestedSamples.WithLabelValues(tenantID).Add(float64(samples))

	return nil
}

// NotifyPreCommit implements ingest.PreCommitNotifier.
func (ni *NullIngester) NotifyPreCommit(_ context.Context) error {
	return nil
}

func (ni *NullIngester) starting(ctx context.Context) error {
	ni.subservicesWatcher.WatchManager(ni.subservices)
	return services.StartManagerAndAwaitHealthy(ctx, ni.subservices)
}

func (ni *NullIngester) running(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return nil
	case err := <-ni.subservicesWatcher.Chan():
		return errors.Wrap(err, "null ingester subservice failed")
	}
}

func (ni *NullIngester) stopping(_ error) error {
	return services.StopManagerAndAwaitStopped(context.Background(), ni.subservices)
}
