// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/multierror"
	"github.com/grafana/dskit/services"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"
)

// CompartmentReaders runs one PartitionReader per write compartment VC, all consuming the
// same read compartment topic. Records from every VC are funneled through a shared
// HeapMerger that orders them by Kafka record timestamp before forwarding to the supplied
// Pusher. CompartmentReaders presents itself as a single services.Service so callers can
// manage it like a regular PartitionReader.
//
// Each reader has its own consumer group, offset file, and Kafka connection. The partition
// ID is shared across all VCs (e.g. "ingester-0" reads partition 0 from every VC).
type CompartmentReaders struct {
	services.Service

	logger  log.Logger
	readers []*PartitionReader
	merger  *HeapMerger
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

	// The merger's downstream consumer is a PusherConsumer that turns the merged record
	// stream into per-tenant WriteRequests and pushes them to the real Pusher. Metrics are
	// registered once at the CompartmentReaders level (not per-VC) since all VCs share this
	// downstream path.
	mergerLogger := log.With(logger, "component", "compartment_merger")
	pusherMetrics := NewPusherConsumerMetrics(reg)
	mergerDownstream := consumerFactoryFunc(func() RecordConsumer {
		return NewPusherConsumer(pusher, kafkaCfg, pusherMetrics, mergerLogger)
	})
	merger := NewHeapMerger(HeapMergerConfig{}, mergerDownstream, mergerLogger)

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

		// The per-VC PartitionReader's RecordConsumer streams records into the shared merger;
		// the real Pusher remains the PreCommitNotifier so offset commits still notify it directly.
		vcID := writeCompartmentID
		submitterFactory := consumerFactoryFunc(func() RecordConsumer {
			return merger.NewSubmittingConsumer(vcID)
		})
		reader, err := newPartitionReader(vcKafkaCfg, partitionID, readerInstanceID, offsetFilePath, submitterFactory, pusher, vcLogger, vcReg)
		if err != nil {
			return nil, errors.Wrapf(err, "creating partition reader for write compartment VC %d", writeCompartmentID)
		}
		readers[writeCompartmentID] = reader
	}

	cr := &CompartmentReaders{
		logger:  logger,
		readers: readers,
		merger:  merger,
	}
	cr.Service = services.NewBasicService(cr.starting, cr.running, cr.stopping).WithName("compartment-readers")
	return cr, nil
}

func (cr *CompartmentReaders) starting(ctx context.Context) error {
	svcs := make([]services.Service, 0, len(cr.readers)+1)
	svcs = append(svcs, cr.merger)
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

// LastSeenOffset is not meaningful across VCs since each VC has its own offset space. It
// returns -1 to signal the value is unavailable; callers that rely on it (notably the
// per-tenant offset catalogue) should not enable that feature when compartments are in use.
func (cr *CompartmentReaders) LastSeenOffset() int64 {
	return -1
}

// EnforceReadMaxDelay returns an error if any VC reader is lagging more than maxDelay.
func (cr *CompartmentReaders) EnforceReadMaxDelay(maxDelay time.Duration) error {
	var errs multierror.MultiError
	for i, r := range cr.readers {
		if err := r.EnforceReadMaxDelay(maxDelay); err != nil {
			errs.Add(errors.Wrapf(err, "write compartment %d", i))
		}
	}
	return errs.Err()
}

// WaitReadConsistencyUntilOffset is not directly representable across VCs since the
// supplied offset belongs to a single VC's offset space. We fall back to waiting until
// every VC has consumed up to its last-produced offset, which is the stronger guarantee.
func (cr *CompartmentReaders) WaitReadConsistencyUntilOffset(ctx context.Context, _ int64) error {
	return cr.WaitReadConsistencyUntilLastProducedOffset(ctx)
}

// WaitReadConsistencyUntilLastProducedOffset waits until every VC has consumed up to its
// last-produced offset, in parallel.
func (cr *CompartmentReaders) WaitReadConsistencyUntilLastProducedOffset(ctx context.Context) error {
	g, gctx := errgroup.WithContext(ctx)
	for i, r := range cr.readers {
		g.Go(func() error {
			if err := r.WaitReadConsistencyUntilLastProducedOffset(gctx); err != nil {
				return errors.Wrapf(err, "write compartment %d", i)
			}
			return nil
		})
	}
	return g.Wait()
}
