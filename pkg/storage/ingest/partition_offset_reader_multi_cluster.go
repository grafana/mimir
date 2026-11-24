// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"
	"fmt"
	"slices"
	"strconv"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/services"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/sync/errgroup"

	"github.com/grafana/mimir/pkg/storage/ingest/kmeta"
)

// MultiClusterOffsetsReader monitors the last produced offsets of several topics across multiple Kafka
// clusters, one per write compartment.
type MultiClusterOffsetsReader struct {
	services.Service

	logger             log.Logger
	topics             []string
	getPartitionIDs    []GetPartitionIDsFunc
	clients            []*kgo.Client
	offsetClients      []*partitionOffsetClient
	offsetsReader      *GenericOffsetReader[kmeta.TopicsPartitionsOffsets]
	subservicesWatcher *services.FailureWatcher

	// additionalReadClusterTopic is empty if there is no additional read cluster. Otherwise, the last
	// offset client belongs to the additional read cluster.
	additionalReadClusterTopic string
}

// NewMultiClusterOffsetsReader creates a MultiClusterOffsetsReader. clusterConfigs holds one KafkaConfig
// per write compartment, with address and SASL credentials already resolved. topics and getPartitionIDs
// are indexed by read compartment ID, and are shared across Kafka clusters because every cluster holds the
// same read-compartment topics with the same partitions. component labels the per-cluster Kafka client
// metrics.
//
// additionalReadClusterConfig is optional. When set, the reader also monitors the topic of that Kafka
// cluster, which gets the Kafka cluster ID after the write compartments' clusters. Ingesters of all read
// compartments consume this same topic, so its offsets are returned for each read compartment's topic.
func NewMultiClusterOffsetsReader(clusterConfigs []KafkaConfig, additionalReadClusterConfig *KafkaConfig, topics []string, getPartitionIDs []GetPartitionIDsFunc, component string, reg prometheus.Registerer, logger log.Logger) (*MultiClusterOffsetsReader, error) {
	if len(clusterConfigs) < 1 {
		return nil, fmt.Errorf("at least one Kafka cluster must be configured, got %d", len(clusterConfigs))
	}
	if len(topics) < 1 {
		return nil, fmt.Errorf("at least one topic must be configured, got %d", len(topics))
	}
	if len(topics) != len(getPartitionIDs) {
		return nil, fmt.Errorf("the number of topics (%d) and partition ID functions (%d) must match", len(topics), len(getPartitionIDs))
	}

	clusterNames := make([]string, len(clusterConfigs))
	for writeCompartmentID := range clusterConfigs {
		clusterNames[writeCompartmentID] = strconv.Itoa(writeCompartmentID)
	}
	additionalReadClusterTopic := ""
	if additionalReadClusterConfig != nil {
		clusterConfigs = slices.Concat(clusterConfigs, []KafkaConfig{*additionalReadClusterConfig})
		clusterNames = append(clusterNames, additionalReadClusterName)
		additionalReadClusterTopic = additionalReadClusterConfig.Topic
	}

	clients := make([]*kgo.Client, 0, len(clusterConfigs))
	offsetClients := make([]*partitionOffsetClient, 0, len(clusterConfigs))
	for kafkaClusterID, clusterCfg := range clusterConfigs {
		clusterName := clusterNames[kafkaClusterID]
		clusterReg := prometheus.WrapRegistererWith(prometheus.Labels{"write_compartment": clusterName}, reg)
		clusterLogger := log.With(logger, "write_compartment", clusterName)

		client, err := newTopicOffsetsReaderKafkaClient(component, clusterCfg, clusterReg, clusterLogger)
		if err != nil {
			// Close the clients created so far, since they won't be started.
			for _, created := range clients {
				created.Close()
			}
			return nil, errors.Wrapf(err, "creating Kafka client for write compartment %s", clusterName)
		}

		clients = append(clients, client)
		offsetClients = append(offsetClients, newPartitionOffsetClient(client, clusterReg, clusterLogger))
	}

	r := &MultiClusterOffsetsReader{
		logger:             logger,
		topics:             topics,
		getPartitionIDs:    getPartitionIDs,
		clients:            clients,
		offsetClients:      offsetClients,
		subservicesWatcher: services.NewFailureWatcher(),

		additionalReadClusterTopic: additionalReadClusterTopic,
	}
	// All clusters share the same poll interval (same base KafkaConfig).
	r.offsetsReader = NewGenericOffsetReader[kmeta.TopicsPartitionsOffsets](r.fetchLastProducedOffsets, clusterConfigs[0].LastProducedOffsetPollInterval, logger)
	r.Service = services.NewBasicService(r.starting, r.running, r.stopping).WithName("multi-cluster-offsets-reader")
	return r, nil
}

func (r *MultiClusterOffsetsReader) starting(ctx context.Context) error {
	r.subservicesWatcher.WatchService(r.offsetsReader)
	return errors.Wrap(services.StartAndAwaitRunning(ctx, r.offsetsReader), "starting offsets reader")
}

func (r *MultiClusterOffsetsReader) running(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return nil
	case err := <-r.subservicesWatcher.Chan():
		return errors.Wrap(err, "multi-cluster offsets reader subservice failed")
	}
}

func (r *MultiClusterOffsetsReader) stopping(_ error) error {
	err := services.StopAndAwaitTerminated(context.Background(), r.offsetsReader)
	for _, client := range r.clients {
		client.Close()
	}
	return err
}

// fetchLastProducedOffsets resolves the partitions once (they're shared across clusters), fetches every
// cluster's last produced offsets with one ListOffsets request per cluster in parallel, and merges them
// into one kmeta.PartitionOffsets per (topic, partition).
func (r *MultiClusterOffsetsReader) fetchLastProducedOffsets(ctx context.Context) (kmeta.TopicsPartitionsOffsets, error) {
	partitionsByTopic := make(map[string][]int32, len(r.topics))
	for i, topic := range r.topics {
		partitionIDs, err := r.getPartitionIDs[i](ctx)
		if err != nil {
			return nil, errors.Wrapf(err, "listing partitions of topic %q", topic)
		}
		partitionsByTopic[topic] = partitionIDs
	}

	perCluster := make([]map[string]map[int32]int64, len(r.offsetClients))

	g, gctx := errgroup.WithContext(ctx)
	for kafkaClusterID, offsetClient := range r.offsetClients {
		g.Go(func() error {
			if r.additionalReadClusterTopic != "" && kafkaClusterID == len(r.offsetClients)-1 {
				fetched, err := fetchAdditionalReadClusterLastProducedOffsets(gctx, offsetClient, r.additionalReadClusterTopic, partitionsByTopic)
				if err != nil {
					return errors.Wrap(err, "additional read cluster")
				}
				perCluster[kafkaClusterID] = fetched
				return nil
			}

			fetched, err := offsetClient.FetchPartitionsLastProducedOffsets(gctx, partitionsByTopic)
			if err != nil {
				return errors.Wrapf(err, "write compartment %d", kafkaClusterID)
			}
			perCluster[kafkaClusterID] = fetched
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}

	return kmeta.MergeTopicsPartitionsOffsets(perCluster), nil
}

// fetchAdditionalReadClusterLastProducedOffsets fetches the last produced offsets of the partitions of all
// read compartments from the single topic of the additional read cluster. It returns these offsets for each
// read compartment's topic, so that they merge with the offsets of the write compartments' clusters.
func fetchAdditionalReadClusterLastProducedOffsets(ctx context.Context, offsetClient *partitionOffsetClient, additionalReadClusterTopic string, partitionsByTopic map[string][]int32) (map[string]map[int32]int64, error) {
	var allPartitionIDs []int32
	for _, partitionIDs := range partitionsByTopic {
		allPartitionIDs = append(allPartitionIDs, partitionIDs...)
	}
	slices.Sort(allPartitionIDs)
	allPartitionIDs = slices.Compact(allPartitionIDs)

	fetched, err := offsetClient.FetchPartitionsLastProducedOffsets(ctx, map[string][]int32{additionalReadClusterTopic: allPartitionIDs})
	if err != nil {
		return nil, err
	}

	offsetsByTopic := make(map[string]map[int32]int64, len(partitionsByTopic))
	for topic, partitionIDs := range partitionsByTopic {
		offsets := make(map[int32]int64, len(partitionIDs))
		for _, partitionID := range partitionIDs {
			if offset, ok := fetched[additionalReadClusterTopic][partitionID]; ok {
				offsets[partitionID] = offset
			}
		}
		offsetsByTopic[topic] = offsets
	}
	return offsetsByTopic, nil
}

// WaitNextFetchLastProducedOffset returns the result of the next "last produced offset" fetch across all
// Kafka clusters, indexed by topic, then partition. The kmeta.PartitionOffsets carries one offset per Kafka
// cluster (write compartment). Concurrent callers share the same in-flight fetch and the same returned
// value (single-flight), so the result must be treated as read-only. Mapping topics back to read
// compartments is left to the caller.
func (r *MultiClusterOffsetsReader) WaitNextFetchLastProducedOffset(ctx context.Context) (kmeta.TopicsPartitionsOffsets, error) {
	return r.offsetsReader.WaitNextFetchLastProducedOffset(ctx)
}

// Topics returns the monitored topics, indexed by read compartment ID.
func (r *MultiClusterOffsetsReader) Topics() []string {
	return r.topics
}
