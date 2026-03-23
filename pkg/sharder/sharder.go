// SPDX-License-Identifier: AGPL-3.0-only

package sharder

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"path/filepath"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/user"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/ingest"
	"github.com/grafana/mimir/pkg/util"
)

// Config holds the configuration for the Sharder.
type Config struct {
	PartitionRing PartitionRingConfig `yaml:"partition_ring"`

	// InstanceID is the ID of this sharder instance, typically set from the hostname.
	InstanceID string `yaml:"instance_id" doc:"hidden"`

	// DataDir is the directory where offset files are stored.
	DataDir string `yaml:"data_dir"`
}

// RegisterFlags registers the flags for the sharder config.
func (cfg *Config) RegisterFlags(f *flag.FlagSet, logger log.Logger) {
	// Default instance ID to hostname.
	hostname, err := os.Hostname()
	if err != nil {
		level.Error(logger).Log("msg", "failed to get hostname for sharder instance-id default", "err", err)
	}
	cfg.InstanceID = hostname

	f.StringVar(&cfg.InstanceID, "sharder.instance-id", cfg.InstanceID, "Instance ID to register in the partition ring.")
	f.StringVar(&cfg.DataDir, "sharder.data-dir", "./sharder-data", "Directory to store offset files for the sharder.")

	cfg.PartitionRing.RegisterFlags(f)
}

// Sharder reads from per-compartment Kafka topics, re-shards by tenant+labels hash,
// and writes to a single ingester Kafka topic.
type Sharder struct {
	services.Service

	cfg    Config
	logger log.Logger
	reg    prometheus.Registerer

	// Partition ring lifecycle.
	partitionID        int32
	partitionLifecycler *ring.PartitionInstanceLifecycler

	// Ingester partition ring (watched, for re-sharding output).
	ingesterPartitionRing *ring.PartitionInstanceRing

	// Ingest storage config (for compartment Kafka configs).
	ingestCfg ingest.Config

	// Compartment readers: one PartitionReader per compartment.
	compartmentReaders []*ingest.PartitionReader

	// Writer for the output (ingester) Kafka topic.
	ingestStorageWriter *ingest.Writer

	subservices        *services.Manager
	subservicesWatcher *services.FailureWatcher
}

// New creates a new Sharder.
func New(cfg Config, ingestCfg ingest.Config, ingesterPartitionRing *ring.PartitionInstanceRing, logger log.Logger, reg prometheus.Registerer) (*Sharder, error) {
	// Derive partition ID from hostname using the same regex as ingesters.
	partitionID, err := ingest.IngesterPartitionID(cfg.InstanceID)
	if err != nil {
		return nil, errors.Wrap(err, "calculating sharder partition ID")
	}

	s := &Sharder{
		cfg:                   cfg,
		logger:                log.With(logger, "component", "sharder"),
		reg:                   reg,
		partitionID:           partitionID,
		ingesterPartitionRing: ingesterPartitionRing,
		ingestCfg:             ingestCfg,
	}

	// Create partition ring KV and lifecycler.
	partitionRingKV := cfg.PartitionRing.KVStore.Mock
	if partitionRingKV == nil {
		partitionRingKV, err = kv.NewClient(cfg.PartitionRing.KVStore, ring.GetPartitionRingCodec(), kv.RegistererWithKVName(reg, PartitionRingName+"-lifecycler"), logger)
		if err != nil {
			return nil, errors.Wrap(err, "creating KV store for sharder partition ring")
		}
	}

	s.partitionLifecycler = ring.NewPartitionInstanceLifecycler(
		cfg.PartitionRing.ToLifecyclerConfig(partitionID, cfg.InstanceID),
		PartitionRingName,
		PartitionRingKey,
		partitionRingKV,
		logger,
		prometheus.WrapRegistererWithPrefix("cortex_", reg),
	)

	// Create the output writer. The output writer writes to the base Kafka config (no compartment placeholders),
	// which is the ingester topic. We use a single compartment (compartmentID=0).
	outputCompartmentsCfg := ingest.CompartmentsConfig{Enabled: false}
	s.ingestStorageWriter = ingest.NewWriter(ingestCfg.KafkaConfig, outputCompartmentsCfg, logger, reg)

	// Create compartment readers.
	if ingestCfg.Compartments.Enabled {
		s.compartmentReaders = make([]*ingest.PartitionReader, ingestCfg.Compartments.NumCompartments)
		for i := 0; i < ingestCfg.Compartments.NumCompartments; i++ {
			compartmentKafkaCfg := ingest.KafkaConfigForCompartment(ingestCfg.KafkaConfig, i)
			offsetFilePath := filepath.Join(cfg.DataDir, fmt.Sprintf("kafka-offset-compartment-%d.json", i))

			pusher := newSharderPusher(s.ingesterPartitionRing, s.ingestStorageWriter, logger)

			reader, err := ingest.NewPartitionReaderForPusher(compartmentKafkaCfg, partitionID, cfg.InstanceID, offsetFilePath, pusher, log.With(logger, "compartment", i), reg)
			if err != nil {
				return nil, errors.Wrapf(err, "creating partition reader for compartment %d", i)
			}
			s.compartmentReaders[i] = reader
		}
	}

	// Set up subservices.
	var subservices []services.Service
	subservices = append(subservices, s.partitionLifecycler)
	subservices = append(subservices, s.ingestStorageWriter)
	for _, reader := range s.compartmentReaders {
		subservices = append(subservices, reader)
	}

	s.subservices, err = services.NewManager(subservices...)
	if err != nil {
		return nil, errors.Wrap(err, "creating sharder subservices manager")
	}
	s.subservicesWatcher = services.NewFailureWatcher()

	s.Service = services.NewBasicService(s.starting, s.running, s.stopping)
	return s, nil
}

func (s *Sharder) starting(ctx context.Context) error {
	s.subservicesWatcher.WatchManager(s.subservices)

	if err := services.StartManagerAndAwaitHealthy(ctx, s.subservices); err != nil {
		return errors.Wrap(err, "unable to start sharder subservices")
	}

	return nil
}

func (s *Sharder) running(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return nil
	case err := <-s.subservicesWatcher.Chan():
		return errors.Wrap(err, "sharder subservice failed")
	}
}

func (s *Sharder) stopping(_ error) error {
	if s.subservices != nil {
		return services.StopManagerAndAwaitStopped(context.Background(), s.subservices)
	}
	return nil
}

// PreparePartitionDownscaleHandler prepares the sharder's partition downscaling. The partition owned by the
// sharder will switch to INACTIVE state (read-only).
//
// Following methods are supported:
//
//   - GET
//     Returns timestamp when partition was switched to INACTIVE state, or 0, if partition is not in INACTIVE state.
//
//   - POST
//     Switches the partition to INACTIVE state (if not yet), and returns the timestamp when the switch to
//     INACTIVE state happened.
//
//   - DELETE
//     Sets partition back from INACTIVE to ACTIVE state.
func (s *Sharder) PreparePartitionDownscaleHandler(w http.ResponseWriter, r *http.Request) {
	logger := log.With(s.logger, "partition", s.partitionID)

	if s.State() != services.Running {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	switch r.Method {
	case http.MethodPost:
		state, _, err := s.partitionLifecycler.GetPartitionState(r.Context())
		if err != nil {
			level.Error(logger).Log("msg", "failed to check partition state in the ring", "err", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		if state == ring.PartitionPending {
			level.Warn(logger).Log("msg", "received a request to prepare partition for shutdown, but the request can't be satisfied because the partition is in PENDING state")
			w.WriteHeader(http.StatusConflict)
			return
		}

		if err := s.partitionLifecycler.ChangePartitionState(r.Context(), ring.PartitionInactive); err != nil {
			level.Error(logger).Log("msg", "failed to change partition state to inactive", "err", err)

			if errors.Is(err, ring.ErrPartitionStateChangeLocked) {
				http.Error(w, err.Error(), http.StatusConflict)
			} else {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
			return
		}

	case http.MethodDelete:
		state, _, err := s.partitionLifecycler.GetPartitionState(r.Context())
		if err != nil {
			level.Error(logger).Log("msg", "failed to check partition state in the ring", "err", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		if state == ring.PartitionInactive {
			if err := s.partitionLifecycler.ChangePartitionState(r.Context(), ring.PartitionActive); err != nil {
				level.Error(logger).Log("msg", "failed to change partition state to active", "err", err)

				if errors.Is(err, ring.ErrPartitionStateChangeLocked) {
					http.Error(w, err.Error(), http.StatusConflict)
				} else {
					http.Error(w, err.Error(), http.StatusInternalServerError)
				}
				return
			}
		}
	}

	state, stateTimestamp, err := s.partitionLifecycler.GetPartitionState(r.Context())
	if err != nil {
		level.Error(logger).Log("msg", "failed to check partition state in the ring", "err", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if state == ring.PartitionInactive {
		util.WriteJSONResponse(w, map[string]any{"timestamp": stateTimestamp.Unix()})
	} else {
		util.WriteJSONResponse(w, map[string]any{"timestamp": 0})
	}
}

// sharderPusher implements ingest.Pusher. It re-shards incoming WriteRequests
// to ingester partitions and writes them to the ingester Kafka topic.
type sharderPusher struct {
	ingesterPartitionRing *ring.PartitionInstanceRing
	writer                *ingest.Writer
	logger                log.Logger
}

func newSharderPusher(ingesterPartitionRing *ring.PartitionInstanceRing, writer *ingest.Writer, logger log.Logger) *sharderPusher {
	return &sharderPusher{
		ingesterPartitionRing: ingesterPartitionRing,
		writer:                writer,
		logger:                logger,
	}
}

// PushToStorageAndReleaseRequest implements ingest.Pusher.
func (p *sharderPusher) PushToStorageAndReleaseRequest(ctx context.Context, req *mimirpb.WriteRequest) error {
	tenantID, err := user.ExtractOrgID(ctx)
	if err != nil {
		level.Error(p.logger).Log("msg", "failed to extract tenant ID from context", "err", err)
		return err
	}

	// Compute tokens for all series and metadata.
	keys, initialMetadataIndex := getSeriesAndMetadataTokens(tenantID, req)

	// Get the active partition ring for ingester partitions.
	partitionRing := ring.NewActivePartitionBatchRing(p.ingesterPartitionRing.PartitionRing())

	// Use DoBatchWithOptions to shard to ingester partitions.
	err = ring.DoBatchWithOptions(ctx, ring.WriteNoExtend, partitionRing, keys,
		func(partition ring.InstanceDesc, tokenIndexes []int) error {
			subReq := req.ForIndexes(tokenIndexes, initialMetadataIndex)

			partitionID, parseErr := parsePartitionID(partition.Id)
			if parseErr != nil {
				return parseErr
			}

			// Write to the ingester topic (compartmentID=0, single topic).
			return p.writer.WriteSync(ctx, 0, partitionID, tenantID, subReq)
		},
		ring.DoBatchOptions{},
	)

	return err
}

// NotifyPreCommit implements ingest.Pusher (PreCommitNotifier).
func (p *sharderPusher) NotifyPreCommit(_ context.Context) error {
	return nil
}

// getSeriesAndMetadataTokens computes partition-ring tokens for series and metadata in a WriteRequest.
func getSeriesAndMetadataTokens(userID string, req *mimirpb.WriteRequest) ([]uint32, int) {
	seriesKeys := make([]uint32, 0, len(req.Timeseries))
	for _, ts := range req.Timeseries {
		seriesKeys = append(seriesKeys, mimirpb.ShardByAllLabelAdapters(userID, ts.Labels))
	}

	metadataKeys := make([]uint32, 0, len(req.Metadata))
	for _, m := range req.Metadata {
		metadataKeys = append(metadataKeys, mimirpb.ShardByMetricName(userID, m.MetricFamilyName))
	}

	keys := make([]uint32, len(seriesKeys)+len(metadataKeys))
	initialMetadataIndex := len(seriesKeys)
	copy(keys, seriesKeys)
	copy(keys[initialMetadataIndex:], metadataKeys)
	return keys, initialMetadataIndex
}

func parsePartitionID(id string) (int32, error) {
	var partitionID uint64
	_, err := fmt.Sscanf(id, "%d", &partitionID)
	if err != nil {
		return 0, fmt.Errorf("failed to parse partition ID %q: %w", id, err)
	}
	return int32(partitionID), nil
}
