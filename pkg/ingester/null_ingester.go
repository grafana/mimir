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
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/grafana/mimir/pkg/storage/ingest"
)

// NullIngester is a lightweight ingester that consumes data from all write compartment VCs for
// a single read compartment topic and discards all received data. It's designed for load testing
// the write path without incurring the cost of TSDB writes.
type NullIngester struct {
	services.Service

	logger              log.Logger
	readers             []*ingest.PartitionReader
	partitionLifecycler *ring.PartitionInstanceLifecycler
	manager             *services.Manager
	watcher             *services.FailureWatcher
}

// NewNullIngester creates a NullIngester that reads from all write compartment VCs for the
// read compartment topic assigned to this ingester set and discards all consumed data. The
// partition is derived from the ingester ring instance ID (e.g. "ingester-0" → partition 0).
// Each null ingester set registers in its own partition ring (keyed by read compartment ID)
// so distributors can route usage-tracker calls to the correct set.
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

	nullPusher := &ingest.NullPusher{}

	// One reader per write compartment VC, all consuming the same read compartment topic.
	// Each VC has its own Kafka address (and optionally distinct SASL credentials).
	readers := make([]*ingest.PartitionReader, compartmentsCfg.NumCompartments)
	for i := 0; i < compartmentsCfg.NumCompartments; i++ {
		vcKafkaCfg := kafkaCfg
		vcKafkaCfg.Topic = readTopic
		vcKafkaCfg.Address = flagext.StringSliceCSV{compartmentsCfg.WriteKafkaAddress(i)}
		vcKafkaCfg.ConsumerGroupOffsetCommitFileEnforced = false
		if compartmentsCfg.WriteKafkaSASLUsernameFormat != "" {
			vcKafkaCfg.SASL.Username = compartmentsCfg.WriteKafkaSASLUsername(i)
			vcKafkaCfg.SASL.Password = flagext.SecretWithValue(compartmentsCfg.WriteKafkaSASLPassword(i))
		}

		offsetFilePath := filepath.Join(offsetDir, fmt.Sprintf("kafka-offset-write-vc-%d.json", i))

		// Each write VC reader needs its own consumer group, so we embed the write VC index
		// in the instance ID. The partition ID stays the same across all write VCs.
		instanceID := fmt.Sprintf("%s-write-%d", cfg.IngesterRing.InstanceID, i)

		vcReg := prometheus.WrapRegistererWith(prometheus.Labels{"write_compartment": strconv.Itoa(i)}, reg)
		vcLogger := log.With(logger, "component", "null_ingest_reader", "write_compartment", i)

		reader, err := ingest.NewPartitionReaderForPusher(vcKafkaCfg, partitionID, instanceID, offsetFilePath, nullPusher, vcLogger, vcReg)
		if err != nil {
			return nil, errors.Wrapf(err, "creating partition reader for write compartment VC %d", i)
		}
		readers[i] = reader
	}

	// Each ingester set registers in its own partition ring (named by read compartment ID) so
	// distributors can route usage-tracker calls to the correct set without partition ID collisions.
	ringName := fmt.Sprintf("%s-rc-%d", PartitionRingName, compartmentsCfg.ReadCompartmentID)
	ringKey := fmt.Sprintf("%s-rc-%d", PartitionRingKey, compartmentsCfg.ReadCompartmentID)

	partitionRingKV := cfg.IngesterPartitionRing.KVStore.Mock
	if partitionRingKV == nil {
		partitionRingKV, err = kv.NewClient(cfg.IngesterPartitionRing.KVStore, ring.GetPartitionRingCodec(), kv.RegistererWithKVName(reg, ringName+"-lifecycler"), logger)
		if err != nil {
			return nil, errors.Wrap(err, "creating KV store for ingester partition ring")
		}
	}

	partitionLifecycler := ring.NewPartitionInstanceLifecycler(
		cfg.IngesterPartitionRing.ToLifecyclerConfig(partitionID, cfg.IngesterRing.InstanceID),
		ringName,
		ringKey,
		partitionRingKV,
		logger,
		prometheus.WrapRegistererWithPrefix("cortex_", reg),
	)
	partitionLifecycler.BasicService = partitionLifecycler.WithName("partition-instance-lifecycler")
	// Unlike real ingesters (which keep their ring entry across restarts), the null ingester
	// always removes itself on stop since it has no persistent state to protect.
	partitionLifecycler.SetRemoveOwnerOnShutdown(true)

	ni := &NullIngester{
		logger:              logger,
		readers:             readers,
		partitionLifecycler: partitionLifecycler,
	}
	ni.Service = services.NewBasicService(ni.starting, ni.running, ni.stopping).WithName("null-ingester")
	return ni, nil
}

func (ni *NullIngester) starting(ctx context.Context) error {
	svcs := make([]services.Service, 0, len(ni.readers)+1)
	svcs = append(svcs, ni.partitionLifecycler)
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
