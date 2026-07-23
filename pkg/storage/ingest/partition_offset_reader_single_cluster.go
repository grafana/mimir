// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"
	"fmt"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/services"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"
)

// SingleClusterTopicOffsetsReader reads the last produced offsets of the partitions of a single topic in a
// single Kafka cluster.
type SingleClusterTopicOffsetsReader struct {
	services.Service

	kafkaClient        *kgo.Client
	offsetClient       *partitionOffsetClient
	offsetsReader      *GenericOffsetReader[map[int32]int64]
	subservicesWatcher *services.FailureWatcher
	topic              string
	getPartitionIDs    GetPartitionIDsFunc
}

// NewSingleClusterTopicOffsetsReader creates a SingleClusterTopicOffsetsReader monitoring topic in the Kafka
// cluster addressed by cfg, polling at cfg.LastProducedOffsetPollInterval. getPartitionIDs returns the
// partitions of topic to read. component labels the Kafka client metrics.
func NewSingleClusterTopicOffsetsReader(cfg KafkaConfig, topic string, getPartitionIDs GetPartitionIDsFunc, component string, reg prometheus.Registerer, logger log.Logger) (*SingleClusterTopicOffsetsReader, error) {
	client, err := newTopicOffsetsReaderKafkaClient(component, cfg, reg, logger)
	if err != nil {
		return nil, errors.Wrap(err, "creating Kafka client")
	}

	r := &SingleClusterTopicOffsetsReader{
		kafkaClient:        client,
		offsetClient:       newPartitionOffsetClient(client, component, reg, logger),
		subservicesWatcher: services.NewFailureWatcher(),
		topic:              topic,
		getPartitionIDs:    getPartitionIDs,
	}
	r.offsetsReader = NewGenericOffsetReader[map[int32]int64](r.FetchLastProducedOffsets, cfg.LastProducedOffsetPollInterval, logger)
	r.Service = services.NewBasicService(r.starting, r.running, r.stopping).WithName("single-cluster-topic-offsets-reader")
	return r, nil
}

func (r *SingleClusterTopicOffsetsReader) starting(ctx context.Context) error {
	r.subservicesWatcher.WatchService(r.offsetsReader)
	return errors.Wrap(services.StartAndAwaitRunning(ctx, r.offsetsReader), "starting offsets reader")
}

func (r *SingleClusterTopicOffsetsReader) running(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return nil
	case err := <-r.subservicesWatcher.Chan():
		return errors.Wrap(err, "single-cluster topic offsets reader subservice failed")
	}
}

func (r *SingleClusterTopicOffsetsReader) stopping(_ error) error {
	err := services.StopAndAwaitTerminated(context.Background(), r.offsetsReader)
	r.kafkaClient.Close()
	return err
}

// FetchLastProducedOffsets fetches the last produced offset for each requested partition of the topic, in a
// single request, returning them indexed by partition. The offset is -1 if a partition has been created but
// no record has been produced yet.
func (r *SingleClusterTopicOffsetsReader) FetchLastProducedOffsets(ctx context.Context) (map[int32]int64, error) {
	partitionIDs, err := r.getPartitionIDs(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing partitions of topic %q: %w", r.topic, err)
	}

	offsetsByTopic, err := r.offsetClient.FetchPartitionsLastProducedOffsets(ctx, map[string][]int32{r.topic: partitionIDs})
	if err != nil {
		return nil, err
	}
	return offsetsByTopic[r.topic], nil
}

// WaitNextFetchLastProducedOffset returns the result of the next "last produced offset" fetch, indexed by
// partition. Concurrent callers share the same in-flight fetch and the same returned value (single-flight),
// so the result must be treated as read-only.
func (r *SingleClusterTopicOffsetsReader) WaitNextFetchLastProducedOffset(ctx context.Context) (map[int32]int64, error) {
	return r.offsetsReader.WaitNextFetchLastProducedOffset(ctx)
}

// Topic returns the monitored topic.
func (r *SingleClusterTopicOffsetsReader) Topic() string {
	return r.topic
}
