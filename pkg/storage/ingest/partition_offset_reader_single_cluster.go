// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"
)

// SingleClusterTopicOffsetsReader is responsible to read the offsets of partitions in a topic.
type SingleClusterTopicOffsetsReader struct {
	*GenericOffsetReader[map[int32]int64]

	client          *partitionOffsetClient
	topic           string
	getPartitionIDs GetPartitionIDsFunc
	logger          log.Logger
}

func NewSingleClusterTopicOffsetsReader(client *kgo.Client, topic string, getPartitionIDs GetPartitionIDsFunc, pollInterval time.Duration, reg prometheus.Registerer, logger log.Logger) *SingleClusterTopicOffsetsReader {
	r := &SingleClusterTopicOffsetsReader{
		client:          newPartitionOffsetClient(client, reg, logger),
		topic:           topic,
		getPartitionIDs: getPartitionIDs,
		logger:          logger,
	}

	r.GenericOffsetReader = NewGenericOffsetReader[map[int32]int64](r.FetchLastProducedOffset, pollInterval, logger)

	return r
}

// FetchLastProducedOffset fetches and returns the last produced offset for each requested partition in the topic.
// The offset is -1 if a partition has been created but no record has been produced yet.
func (p *SingleClusterTopicOffsetsReader) FetchLastProducedOffset(ctx context.Context) (map[int32]int64, error) {
	partitionIDs, err := p.getPartitionIDs(ctx)
	if err != nil {
		return nil, err
	}

	offsetsByTopic, err := p.client.FetchPartitionsLastProducedOffsets(ctx, map[string][]int32{p.topic: partitionIDs})
	if err != nil {
		return nil, err
	}

	return offsetsByTopic[p.topic], nil
}

func (p *SingleClusterTopicOffsetsReader) Topic() string {
	return p.topic
}
