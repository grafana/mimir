// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/services"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/grafana/mimir/pkg/storage/ingest"
)

// NullIngester is a lightweight ingester that consumes data from multiple Kafka compartment
// topics but discards all received data. It's designed for load testing the write path
// without incurring the cost of TSDB writes.
type NullIngester struct {
	services.Service

	logger  log.Logger
	readers []*ingest.PartitionReader
	manager *services.Manager
	watcher *services.FailureWatcher
}

// NewNullIngester creates a NullIngester that reads from all compartment topics for the
// partition assigned to this instance and discards all consumed data. The partition is
// derived from the ingester ring instance ID (e.g. "ingester-0" → partition 0).
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

	router := ingest.NewCompartmentRouter(compartmentsCfg)

	offsetDir := cfg.NullIngesterOffsetDir
	if offsetDir == "" {
		offsetDir = os.TempDir()
	}

	kafkaCfg := cfg.IngestStorageConfig.KafkaConfig
	kafkaCfg.FallbackClientErrorSampleRate = cfg.ErrorSampleRate

	nullPusher := &ingest.NullPusher{}

	readers := make([]*ingest.PartitionReader, compartmentsCfg.NumCompartments)
	for i := 0; i < compartmentsCfg.NumCompartments; i++ {
		compKafkaCfg := kafkaCfg
		compKafkaCfg.Topic = router.Topic(i)

		offsetFilePath := filepath.Join(offsetDir, fmt.Sprintf("kafka-offset-compartment-%d.json", i))

		// Each compartment reader needs its own consumer group, so we embed the compartment
		// index in the instance ID. The partition ID stays the same across all compartments.
		instanceID := fmt.Sprintf("%s-comp-%d", cfg.IngesterRing.InstanceID, i)

		compReg := prometheus.WrapRegistererWith(prometheus.Labels{"compartment": strconv.Itoa(i)}, reg)
		compLogger := log.With(logger, "component", "null_ingest_reader", "compartment", i)

		reader, err := ingest.NewPartitionReaderForPusher(compKafkaCfg, partitionID, instanceID, offsetFilePath, nullPusher, compLogger, compReg)
		if err != nil {
			return nil, errors.Wrapf(err, "creating partition reader for compartment %d", i)
		}
		readers[i] = reader
	}

	ni := &NullIngester{
		logger:  logger,
		readers: readers,
	}
	ni.Service = services.NewBasicService(ni.starting, ni.running, ni.stopping).WithName("null-ingester")
	return ni, nil
}

func (ni *NullIngester) starting(ctx context.Context) error {
	svcs := make([]services.Service, len(ni.readers))
	for i, r := range ni.readers {
		svcs[i] = r
	}

	var err error
	ni.manager, err = services.NewManager(svcs...)
	if err != nil {
		return errors.Wrap(err, "creating partition readers manager")
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
