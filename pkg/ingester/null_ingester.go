// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
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
	readers []*ingest.PartitionReader
	manager *services.Manager
	watcher *services.FailureWatcher
	metrics *ingesterMetrics
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

	if compartmentsCfg.WriteKafkaAddressFormat == "" {
		return nil, errors.New("ingest-storage.compartments.write-kafka-address-format must be configured for null ingester")
	}

	partitionID, err := ingest.IngesterPartitionID(cfg.IngesterRing.InstanceID)
	if err != nil {
		return nil, errors.Wrap(err, "calculating ingester partition ID")
	}

	router := ingest.NewCompartmentRouter(compartmentsCfg)
	readTopic := router.Topic(compartmentsCfg.ReadCompartmentID)

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

	// One reader per write compartment VC, all consuming the same read compartment topic.
	// Each VC has its own Kafka address (and optionally distinct SASL credentials).
	readers := make([]*ingest.PartitionReader, compartmentsCfg.NumCompartments)
	for writeCompartmentID := 0; writeCompartmentID < compartmentsCfg.NumCompartments; writeCompartmentID++ {
		vcKafkaCfg := kafkaCfg
		vcKafkaCfg.Topic = readTopic
		vcKafkaCfg.Address = flagext.StringSliceCSV{compartmentsCfg.WriteKafkaAddress(writeCompartmentID)}
		vcKafkaCfg.ConsumerGroupOffsetCommitFileEnforced = false
		if compartmentsCfg.WriteKafkaSASLUsernameFormat != "" {
			vcKafkaCfg.SASL.Username = compartmentsCfg.WriteKafkaSASLUsername(writeCompartmentID)
			vcKafkaCfg.SASL.Password = flagext.SecretWithValue(compartmentsCfg.WriteKafkaSASLPassword(writeCompartmentID))
		}

		offsetFilePath := filepath.Join(offsetDir, fmt.Sprintf("kafka-offset-write-vc-%d.json", writeCompartmentID))

		// Each write VC reader needs its own consumer group, so we embed the write VC index
		// in the instance ID. The partition ID stays the same across all write VCs.
		instanceID := fmt.Sprintf("%s-write-%d", cfg.IngesterRing.InstanceID, writeCompartmentID)

		vcReg := prometheus.WrapRegistererWith(prometheus.Labels{"write_compartment": strconv.Itoa(writeCompartmentID)}, reg)
		vcLogger := log.With(logger, "component", "null_ingest_reader", "write_compartment", writeCompartmentID)

		reader, err := ingest.NewPartitionReaderForPusher(vcKafkaCfg, partitionID, instanceID, offsetFilePath, ni, vcLogger, vcReg)
		if err != nil {
			return nil, errors.Wrapf(err, "creating partition reader for write compartment VC %d", writeCompartmentID)
		}
		readers[writeCompartmentID] = reader
	}

	ni.readers = readers
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
	svcs := make([]services.Service, 0, len(ni.readers))
	for _, r := range ni.readers {
		svcs = append(svcs, r)
	}

	var err error
	ni.manager, err = services.NewManager(svcs...)
	if err != nil {
		return errors.Wrap(err, "creating services manager")
	}

	ni.watcher = services.NewFailureWatcher()
	ni.watcher.WatchManager(ni.manager)

	return services.StartManagerAndAwaitHealthy(ctx, ni.manager)
}

func (ni *NullIngester) running(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return nil
	case err := <-ni.watcher.Chan():
		return errors.Wrap(err, "null ingester subservice failed")
	}
}

func (ni *NullIngester) stopping(_ error) error {
	ni.watcher.Close()
	if ni.manager != nil {
		ni.manager.StopAsync()
		return ni.manager.AwaitStopped(context.Background())
	}
	return nil
}
