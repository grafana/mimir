// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/services"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
)

// CompartmentReaders runs one PartitionReader per write compartment VC, all consuming the
// same read compartment topic and pushing into the supplied Pusher. It presents itself as
// a single services.Service so callers can manage it like a regular PartitionReader.
//
// Each reader has its own consumer group, offset file, and Kafka connection. The partition
// ID is shared across all VCs (e.g. "ingester-0" reads partition 0 from every VC).
type CompartmentReaders struct {
	services.Service

	logger  log.Logger
	readers []*PartitionReader
	manager *services.Manager
	watcher *services.FailureWatcher
}

// NewCompartmentReaders constructs a CompartmentReaders that consumes from every write
// compartment VC for the read compartment configured in cfg.
//
// offsetDir is the directory in which per-VC offset files are stored.
// instanceID is used as the base for each reader's consumer group, suffixed with the
// write compartment index so each VC tracks offsets independently.
func NewCompartmentReaders(
	kafkaCfg KafkaConfig,
	cfg CompartmentsConfig,
	partitionID int32,
	instanceID string,
	offsetDir string,
	pusher Pusher,
	logger log.Logger,
	reg prometheus.Registerer,
) (*CompartmentReaders, error) {
	if !cfg.Enabled {
		return nil, errors.New("compartments must be enabled")
	}
	if cfg.WriteKafkaAddressFormat == "" {
		return nil, errors.New("compartments.write_kafka_address_format must be configured")
	}

	router := NewCompartmentRouter(cfg)
	readTopic := router.Topic(cfg.ReadCompartmentID)

	readers := make([]*PartitionReader, cfg.NumCompartments)
	for writeCompartmentID := 0; writeCompartmentID < cfg.NumCompartments; writeCompartmentID++ {
		vcKafkaCfg := kafkaCfg
		vcKafkaCfg.Topic = readTopic
		vcKafkaCfg.Address = flagext.StringSliceCSV{cfg.WriteKafkaAddress(writeCompartmentID)}
		vcKafkaCfg.ConsumerGroupOffsetCommitFileEnforced = false
		if cfg.WriteKafkaSASLUsernameFormat != "" {
			vcKafkaCfg.SASL.Username = cfg.WriteKafkaSASLUsername(writeCompartmentID)
			vcKafkaCfg.SASL.Password = flagext.SecretWithValue(cfg.WriteKafkaSASLPassword(writeCompartmentID))
		}

		offsetFilePath := filepath.Join(offsetDir, fmt.Sprintf("kafka-offset-write-vc-%d.json", writeCompartmentID))
		readerInstanceID := fmt.Sprintf("%s-write-%d", instanceID, writeCompartmentID)

		vcReg := prometheus.WrapRegistererWith(prometheus.Labels{"write_compartment": strconv.Itoa(writeCompartmentID)}, reg)
		vcLogger := log.With(logger, "component", "compartment_reader", "write_compartment", writeCompartmentID)

		reader, err := NewPartitionReaderForPusher(vcKafkaCfg, partitionID, readerInstanceID, offsetFilePath, pusher, vcLogger, vcReg)
		if err != nil {
			return nil, errors.Wrapf(err, "creating partition reader for write compartment VC %d", writeCompartmentID)
		}
		readers[writeCompartmentID] = reader
	}

	cr := &CompartmentReaders{
		logger:  logger,
		readers: readers,
	}
	cr.Service = services.NewBasicService(cr.starting, cr.running, cr.stopping).WithName("compartment-readers")
	return cr, nil
}

func (cr *CompartmentReaders) starting(ctx context.Context) error {
	svcs := make([]services.Service, 0, len(cr.readers))
	for _, r := range cr.readers {
		svcs = append(svcs, r)
	}

	var err error
	cr.manager, err = services.NewManager(svcs...)
	if err != nil {
		return errors.Wrap(err, "creating services manager")
	}

	cr.watcher = services.NewFailureWatcher()
	cr.watcher.WatchManager(cr.manager)

	return services.StartManagerAndAwaitHealthy(ctx, cr.manager)
}

func (cr *CompartmentReaders) running(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return nil
	case err := <-cr.watcher.Chan():
		return errors.Wrap(err, "compartment reader subservice failed")
	}
}

func (cr *CompartmentReaders) stopping(_ error) error {
	cr.watcher.Close()
	if cr.manager != nil {
		cr.manager.StopAsync()
		return cr.manager.AwaitStopped(context.Background())
	}
	return nil
}
