// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// partitionOffsetClient is a client used to read partition offsets. It is topic-agnostic: the topic(s) to
// read are passed to each method, so a single client can serve any number of topics.
type partitionOffsetClient struct {
	client *kgo.Client
	admin  *kadm.Client
	logger log.Logger

	// Metrics.
	lastProducedOffsetRequestsTotal   *prometheus.CounterVec
	lastProducedOffsetFailuresTotal   *prometheus.CounterVec
	lastProducedOffsetLatency         *prometheus.HistogramVec
	partitionStartOffsetRequestsTotal *prometheus.CounterVec
	partitionStartOffsetFailuresTotal *prometheus.CounterVec
	partitionStartOffsetLatency       *prometheus.HistogramVec
}

// newPartitionOffsetClient creates a client to read partition offsets. The metrics it registers use variable
// "topic" and "partition" labels, so a given prometheus.Registerer must be used by at most one partitionOffsetClient:
// create a single client per registerer (e.g. one per Kafka cluster) and reuse it across topics by passing the topic
// to each method. Registering a second client on the same registerer panics with a duplicate-registration error.
func newPartitionOffsetClient(client *kgo.Client, reg prometheus.Registerer, logger log.Logger) *partitionOffsetClient {
	return &partitionOffsetClient{
		client: client,
		admin:  kadm.NewClient(client),
		logger: logger,

		lastProducedOffsetRequestsTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_ingest_storage_reader_last_produced_offset_requests_total",
			Help: "Total number of requests issued to get the last produced offset.",
		}, []string{"topic", "partition"}),
		lastProducedOffsetFailuresTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_ingest_storage_reader_last_produced_offset_failures_total",
			Help: "Total number of failed requests to get the last produced offset.",
		}, []string{"topic", "partition"}),
		lastProducedOffsetLatency: promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
			Name:                            "cortex_ingest_storage_reader_last_produced_offset_request_duration_seconds",
			Help:                            "The duration of requests to fetch the last produced offset of a given partition.",
			NativeHistogramBucketFactor:     1.1,
			NativeHistogramMaxBucketNumber:  100,
			NativeHistogramMinResetDuration: 1 * time.Hour,
			Buckets:                         prometheus.DefBuckets,
		}, []string{"topic", "partition"}),

		partitionStartOffsetRequestsTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_ingest_storage_reader_partition_start_offset_requests_total",
			Help: "Total number of requests issued to get the partition start offset.",
		}, []string{"topic", "partition"}),
		partitionStartOffsetFailuresTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_ingest_storage_reader_partition_start_offset_failures_total",
			Help: "Total number of failed requests to get the partition start offset.",
		}, []string{"topic", "partition"}),
		partitionStartOffsetLatency: promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
			Name:                            "cortex_ingest_storage_reader_partition_start_offset_request_duration_seconds",
			Help:                            "The duration of requests to fetch the start offset of a given partition.",
			NativeHistogramBucketFactor:     1.1,
			NativeHistogramMaxBucketNumber:  100,
			NativeHistogramMinResetDuration: 1 * time.Hour,
			Buckets:                         prometheus.DefBuckets,
		}, []string{"topic", "partition"}),
	}
}

// FetchPartitionLastProducedOffset fetches and returns the last produced offset for a partition, or -1 if no record has
// been ever produced in the partition. This function issues a single request, but the Kafka client used under the
// hood may retry a failed request until the retry timeout is hit.
func (p *partitionOffsetClient) FetchPartitionLastProducedOffset(ctx context.Context, topic string, partitionID int32) (_ int64, returnErr error) {
	var (
		startTime        = time.Now()
		partitionIDLabel = strconv.Itoa(int(partitionID))

		// Init metrics for a partition even if they're not tracked (e.g. failures).
		requestsTotal   = p.lastProducedOffsetRequestsTotal.WithLabelValues(topic, partitionIDLabel)
		requestsLatency = p.lastProducedOffsetLatency.WithLabelValues(topic, partitionIDLabel)
		failuresTotal   = p.lastProducedOffsetFailuresTotal.WithLabelValues(topic, partitionIDLabel)
	)

	requestsTotal.Inc()
	defer func() {
		// We track the latency also in case of error, so that if the request times out it gets
		// pretty clear looking at latency too.
		requestsLatency.Observe(time.Since(startTime).Seconds())

		if returnErr != nil {
			failuresTotal.Inc()
		}
	}()

	offset, err := p.fetchPartitionOffset(ctx, topic, partitionID, kafkaOffsetEnd)
	if err != nil {
		return 0, err
	}

	// The offset we get is the offset at which the next message will be written, so to get the last produced offset
	// we have to subtract 1. See DESIGN.md for more details.
	return offset - 1, nil
}

// FetchPartitionStartOffset fetches and returns the start offset for a partition. This function returns 0 if no record has
// been ever produced in the partition. This function issues a single request, but the Kafka client used under the
// hood may retry a failed request until the retry timeout is hit.
func (p *partitionOffsetClient) FetchPartitionStartOffset(ctx context.Context, topic string, partitionID int32) (_ int64, returnErr error) {
	var (
		startTime        = time.Now()
		partitionIDLabel = strconv.Itoa(int(partitionID))

		// Init metrics for a partition even if they're not tracked (e.g. failures).
		requestsTotal   = p.partitionStartOffsetRequestsTotal.WithLabelValues(topic, partitionIDLabel)
		requestsLatency = p.partitionStartOffsetLatency.WithLabelValues(topic, partitionIDLabel)
		failuresTotal   = p.partitionStartOffsetFailuresTotal.WithLabelValues(topic, partitionIDLabel)
	)

	requestsTotal.Inc()
	defer func() {
		// We track the latency also in case of error, so that if the request times out it gets
		// pretty clear looking at latency too.
		requestsLatency.Observe(time.Since(startTime).Seconds())

		if returnErr != nil {
			failuresTotal.Inc()
		}
	}()

	return p.fetchPartitionOffset(ctx, topic, partitionID, kafkaOffsetStart)
}

func (p *partitionOffsetClient) fetchPartitionOffset(ctx context.Context, topic string, partitionID int32, position int64) (int64, error) {
	// Create a custom request to fetch the latest offset of a specific partition.
	// We manually create a request so that we can request the offset for a single partition
	// only, which is more performant than requesting the offsets for all partitions.
	partitionReq := kmsg.NewListOffsetsRequestTopicPartition()
	partitionReq.Partition = partitionID
	partitionReq.Timestamp = position

	topicReq := kmsg.NewListOffsetsRequestTopic()
	topicReq.Topic = topic
	topicReq.Partitions = []kmsg.ListOffsetsRequestTopicPartition{partitionReq}

	req := kmsg.NewPtrListOffsetsRequest()
	req.IsolationLevel = 0 // 0 means READ_UNCOMMITTED.
	req.Topics = []kmsg.ListOffsetsRequestTopic{topicReq}

	// Even if we share the same client, other in-flight requests are not canceled once this context is canceled
	// (or its deadline is exceeded). We've verified it with a unit test.
	resps := p.client.RequestSharded(ctx, req)

	// Since we issued a request for only 1 partition, we expect exactly 1 response.
	if expected := 1; len(resps) != expected {
		return 0, fmt.Errorf("unexpected number of responses (expected: %d, got: %d)", expected, len(resps))
	}

	// Ensure no error occurred.
	res := resps[0]
	if res.Err != nil {
		return 0, res.Err
	}

	// Parse the response.
	listRes, ok := res.Resp.(*kmsg.ListOffsetsResponse)
	if !ok {
		return 0, errors.New("unexpected response type")
	}
	if expected, actual := 1, len(listRes.Topics); actual != expected {
		return 0, fmt.Errorf("unexpected number of topics in the response (expected: %d, got: %d)", expected, actual)
	}
	if expected, actual := topic, listRes.Topics[0].Topic; expected != actual {
		return 0, fmt.Errorf("unexpected topic in the response (expected: %s, got: %s)", expected, actual)
	}
	if expected, actual := 1, len(listRes.Topics[0].Partitions); actual != expected {
		return 0, fmt.Errorf("unexpected number of partitions in the response (expected: %d, got: %d)", expected, actual)
	}
	if expected, actual := partitionID, listRes.Topics[0].Partitions[0].Partition; actual != expected {
		return 0, fmt.Errorf("unexpected partition in the response (expected: %d, got: %d)", expected, actual)
	}
	if err := kerr.ErrorForCode(listRes.Topics[0].Partitions[0].ErrorCode); err != nil {
		return 0, err
	}

	return listRes.Topics[0].Partitions[0].Offset, nil
}

// FetchPartitionsLastProducedOffsets fetches and returns the last produced offsets for the input partitions of each
// input topic, in a single ListOffsets request. partitionsByTopic maps a topic to the partitions to look up (the
// partition sets may differ per topic). The returned offset is -1 if a partition has been created but no record has
// been produced yet. The returned offsets are guaranteed to be always updated (no stale or cached offsets returned).
//
// The Kafka client used under the hood may retry a failed request until the retry timeout is hit.
func (p *partitionOffsetClient) FetchPartitionsLastProducedOffsets(ctx context.Context, partitionsByTopic map[string][]int32) (_ map[string]map[int32]int64, returnErr error) {
	// Skip lookup and don't track any metric if no partition was requested.
	numPartitions := 0
	for _, partitionIDs := range partitionsByTopic {
		numPartitions += len(partitionIDs)
	}
	if numPartitions == 0 {
		return nil, nil
	}

	// The request covers all the requested partitions, so the "partition" label is tracked as "mixed".
	// When a single topic is requested we track its actual name; otherwise the "topic" label is "mixed".
	topicLabel := "mixed"
	if len(partitionsByTopic) == 1 {
		for topic := range partitionsByTopic {
			topicLabel = topic
		}
	}

	var (
		startTime = time.Now()

		requestsTotal   = p.lastProducedOffsetRequestsTotal.WithLabelValues(topicLabel, "mixed")
		requestsLatency = p.lastProducedOffsetLatency.WithLabelValues(topicLabel, "mixed")
		failuresTotal   = p.lastProducedOffsetFailuresTotal.WithLabelValues(topicLabel, "mixed")
	)

	requestsTotal.Inc()
	defer func() {
		// We track the latency also in case of error, so that if the request times out it gets
		// pretty clear looking at latency too.
		requestsLatency.Observe(time.Since(startTime).Seconds())

		if returnErr != nil {
			failuresTotal.Inc()
		}
	}()

	res, err := p.fetchPartitionsEndOffsets(ctx, partitionsByTopic)
	if err != nil {
		return nil, err
	}
	if err := res.Error(); err != nil {
		return nil, err
	}

	// Build a simple map for the offsets, indexed by topic and partition.
	offsets := make(map[string]map[int32]int64, len(res))
	res.Each(func(offset kadm.ListedOffset) {
		if offsets[offset.Topic] == nil {
			offsets[offset.Topic] = make(map[int32]int64, len(partitionsByTopic[offset.Topic]))
		}

		// The offset we get is the offset at which the next message will be written, so to get the last produced offset
		// we have to subtract 1. See DESIGN.md for more details.
		offsets[offset.Topic][offset.Partition] = offset.Offset - 1
	})

	return offsets, nil
}

// fetchPartitionsEndOffsets returns the end offset of each input partition of each input topic, in a single
// ListOffsets request. This function returns an error if it fails to get the offset of any partition (no partial
// result is returned).
func (p *partitionOffsetClient) fetchPartitionsEndOffsets(ctx context.Context, partitionsByTopic map[string][]int32) (kadm.ListedOffsets, error) {
	list := make(kadm.ListedOffsets, len(partitionsByTopic))

	// Prepare the request to list offsets, with one entry per topic.
	req := kmsg.NewPtrListOffsetsRequest()
	req.IsolationLevel = 0 // 0 means READ_UNCOMMITTED.

	for topic, partitionIDs := range partitionsByTopic {
		list[topic] = make(map[int32]kadm.ListedOffset, len(partitionIDs))

		topicReq := kmsg.NewListOffsetsRequestTopic()
		topicReq.Topic = topic
		for _, partitionID := range partitionIDs {
			partitionReq := kmsg.NewListOffsetsRequestTopicPartition()
			partitionReq.Partition = partitionID
			partitionReq.Timestamp = kafkaOffsetEnd

			topicReq.Partitions = append(topicReq.Partitions, partitionReq)
		}

		req.Topics = append(req.Topics, topicReq)
	}

	// Execute the request. The Kafka client shards it per broker (a request may touch partitions led by
	// different brokers), so a single response shard may carry a subset of the requested topics/partitions.
	shards := p.client.RequestSharded(ctx, req)

	for _, shard := range shards {
		if shard.Err != nil {
			return nil, shard.Err
		}

		res := shard.Resp.(*kmsg.ListOffsetsResponse)
		for _, topic := range res.Topics {
			topicOffsets, ok := list[topic.Topic]
			if !ok {
				return nil, fmt.Errorf("unexpected topic in the response: %s", topic.Topic)
			}

			for _, pt := range topic.Partitions {
				if err := kerr.ErrorForCode(pt.ErrorCode); err != nil {
					return nil, err
				}

				topicOffsets[pt.Partition] = kadm.ListedOffset{
					Topic:       topic.Topic,
					Partition:   pt.Partition,
					Timestamp:   pt.Timestamp,
					Offset:      pt.Offset,
					LeaderEpoch: pt.LeaderEpoch,
				}
			}
		}
	}

	// Ensure we got an offset for every requested topic-partition. A successful response that silently
	// omits a topic-partition (no error code) would otherwise yield an incomplete result without any error.
	for topic, partitionIDs := range partitionsByTopic {
		for _, partitionID := range partitionIDs {
			if _, ok := list[topic][partitionID]; !ok {
				return nil, fmt.Errorf("the offset for topic %q partition %d is missing in the response", topic, partitionID)
			}
		}
	}

	return list, nil
}

// ListTopicPartitionIDs returns a list of all partition IDs in the input topic.
func (p *partitionOffsetClient) ListTopicPartitionIDs(ctx context.Context, topic string) ([]int32, error) {
	topics, err := p.admin.ListTopics(ctx, topic)
	if err != nil {
		return nil, err
	}
	if err := topics.Error(); err != nil {
		return nil, err
	}

	// We expect 1 topic in the response.
	if len(topics) != 1 {
		return nil, fmt.Errorf("unexpected number of topics in the response (expected: %d, got: %d)", 1, len(topics))
	}

	// Get the expected topic.
	topicDetail, ok := topics[topic]
	if !ok {
		return nil, fmt.Errorf("unexpected topic in the response (expected: %s)", topic)
	}

	// Extract the partition IDs from the response.
	ids := make([]int32, 0, len(topicDetail.Partitions))
	for _, partition := range topicDetail.Partitions {
		// We don't check partition.Err because all we need is the partition ID;
		// kadm.Client.ListEndOffsets() doesn't check it neither.
		ids = append(ids, partition.Partition)
	}

	// It's not required to sort the partition IDs, but we do it to simplify tests and troubleshooting.
	slices.Sort(ids)

	return ids, nil
}
