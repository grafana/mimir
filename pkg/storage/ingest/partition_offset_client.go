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

// partitionOffsetClient is a client used to read partition offsets.
type partitionOffsetClient struct {
	client *kgo.Client
	admin  *kadm.Client
	logger log.Logger
	topic  string

	// Metrics.
	lastProducedOffsetRequestsTotal   *prometheus.CounterVec
	lastProducedOffsetFailuresTotal   *prometheus.CounterVec
	lastProducedOffsetLatency         *prometheus.HistogramVec
	partitionStartOffsetRequestsTotal *prometheus.CounterVec
	partitionStartOffsetFailuresTotal *prometheus.CounterVec
	partitionStartOffsetLatency       *prometheus.HistogramVec
}

func newPartitionOffsetClient(client *kgo.Client, topic string, reg prometheus.Registerer, logger log.Logger) *partitionOffsetClient {
	return &partitionOffsetClient{
		client: client,
		admin:  kadm.NewClient(client),
		logger: logger,
		topic:  topic,

		lastProducedOffsetRequestsTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_ingest_storage_reader_last_produced_offset_requests_total",
			Help: "Total number of requests issued to get the last produced offset.",
		}, []string{"partition"}),
		lastProducedOffsetFailuresTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_ingest_storage_reader_last_produced_offset_failures_total",
			Help: "Total number of failed requests to get the last produced offset.",
		}, []string{"partition"}),
		lastProducedOffsetLatency: promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
			Name:                            "cortex_ingest_storage_reader_last_produced_offset_request_duration_seconds",
			Help:                            "The duration of requests to fetch the last produced offset of a given partition.",
			NativeHistogramBucketFactor:     1.1,
			NativeHistogramMaxBucketNumber:  100,
			NativeHistogramMinResetDuration: 1 * time.Hour,
			Buckets:                         prometheus.DefBuckets,
		}, []string{"partition"}),

		partitionStartOffsetRequestsTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_ingest_storage_reader_partition_start_offset_requests_total",
			Help: "Total number of requests issued to get the partition start offset.",
		}, []string{"partition"}),
		partitionStartOffsetFailuresTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_ingest_storage_reader_partition_start_offset_failures_total",
			Help: "Total number of failed requests to get the partition start offset.",
		}, []string{"partition"}),
		partitionStartOffsetLatency: promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
			Name:                            "cortex_ingest_storage_reader_partition_start_offset_request_duration_seconds",
			Help:                            "The duration of requests to fetch the start offset of a given partition.",
			NativeHistogramBucketFactor:     1.1,
			NativeHistogramMaxBucketNumber:  100,
			NativeHistogramMinResetDuration: 1 * time.Hour,
			Buckets:                         prometheus.DefBuckets,
		}, []string{"partition"}),
	}
}

// FetchPartitionLastProducedOffset fetches and returns the last produced offset for a partition, or -1 if no record has
// been ever produced in the partition. This function issues a single request, but the Kafka client used under the
// hood may retry a failed request until the retry timeout is hit.
func (p *partitionOffsetClient) FetchPartitionLastProducedOffset(ctx context.Context, partitionID int32) (_ int64, returnErr error) {
	var (
		startTime        = time.Now()
		partitionIDLabel = strconv.Itoa(int(partitionID))

		// Init metrics for a partition even if they're not tracked (e.g. failures).
		requestsTotal   = p.lastProducedOffsetRequestsTotal.WithLabelValues(partitionIDLabel)
		requestsLatency = p.lastProducedOffsetLatency.WithLabelValues(partitionIDLabel)
		failuresTotal   = p.lastProducedOffsetFailuresTotal.WithLabelValues(partitionIDLabel)
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

	offset, err := p.fetchPartitionOffset(ctx, partitionID, kafkaOffsetEnd)
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
func (p *partitionOffsetClient) FetchPartitionStartOffset(ctx context.Context, partitionID int32) (_ int64, returnErr error) {
	var (
		startTime        = time.Now()
		partitionIDLabel = strconv.Itoa(int(partitionID))

		// Init metrics for a partition even if they're not tracked (e.g. failures).
		requestsTotal   = p.partitionStartOffsetRequestsTotal.WithLabelValues(partitionIDLabel)
		requestsLatency = p.partitionStartOffsetLatency.WithLabelValues(partitionIDLabel)
		failuresTotal   = p.partitionStartOffsetFailuresTotal.WithLabelValues(partitionIDLabel)
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

	return p.fetchPartitionOffset(ctx, partitionID, kafkaOffsetStart)
}

func (p *partitionOffsetClient) fetchPartitionOffset(ctx context.Context, partitionID int32, position int64) (int64, error) {
	// Create a custom request to fetch the latest offset of a specific partition.
	// We manually create a request so that we can request the offset for a single partition
	// only, which is more performant than requesting the offsets for all partitions.
	partitionReq := kmsg.NewListOffsetsRequestTopicPartition()
	partitionReq.Partition = partitionID
	partitionReq.Timestamp = position

	topicReq := kmsg.NewListOffsetsRequestTopic()
	topicReq.Topic = p.topic
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
	if expected, actual := p.topic, listRes.Topics[0].Topic; expected != actual {
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

// FetchPartitionsLastProducedOffsets fetches and returns the last produced offsets for all input partitions. The offset is
// -1 if a partition has been created but no record has been produced yet. The returned offsets for each partition
// are guaranteed to be always updated (no stale or cached offsets returned).
//
// The Kafka client used under the hood may retry a failed request until the retry timeout is hit.
func (p *partitionOffsetClient) FetchPartitionsLastProducedOffsets(ctx context.Context, partitionIDs []int32) (_ map[int32]int64, returnErr error) {
	// Skip lookup and don't track any metric if no partition was requested.
	if len(partitionIDs) == 0 {
		return nil, nil
	}

	var (
		startTime = time.Now()

		// Init metrics for a partition even if they're not tracked (e.g. failures).
		requestsTotal   = p.lastProducedOffsetRequestsTotal.WithLabelValues("mixed")
		requestsLatency = p.lastProducedOffsetLatency.WithLabelValues("mixed")
		failuresTotal   = p.lastProducedOffsetFailuresTotal.WithLabelValues("mixed")
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

	res, err := p.fetchPartitionsEndOffsets(ctx, partitionIDs)
	if err != nil {
		return nil, err
	}
	if err := res.Error(); err != nil {
		return nil, err
	}

	// Build a simple map for the offsets.
	offsets := make(map[int32]int64, len(res[p.topic]))
	res.Each(func(offset kadm.ListedOffset) {
		// The offsets we get is the offset at which the next message will be written, so to get the last produced offset
		// we have to subtract 1. See DESIGN.md for more details.
		offsets[offset.Partition] = offset.Offset - 1
	})

	return offsets, nil
}

// fetchPartitionsEndOffsets returns the end offset of each partition in input. This function returns
// error if fails to get the offset of any partition (no partial result is returned).
func (p *partitionOffsetClient) fetchPartitionsEndOffsets(ctx context.Context, partitionIDs []int32) (kadm.ListedOffsets, error) {
	list := kadm.ListedOffsets{
		p.topic: make(map[int32]kadm.ListedOffset, len(partitionIDs)),
	}

	// Prepare the request to list offsets.
	topicReq := kmsg.NewListOffsetsRequestTopic()
	topicReq.Topic = p.topic
	for _, partitionID := range partitionIDs {
		partitionReq := kmsg.NewListOffsetsRequestTopicPartition()
		partitionReq.Partition = partitionID
		partitionReq.Timestamp = kafkaOffsetEnd

		topicReq.Partitions = append(topicReq.Partitions, partitionReq)
	}

	req := kmsg.NewPtrListOffsetsRequest()
	req.IsolationLevel = 0 // 0 means READ_UNCOMMITTED.
	req.Topics = []kmsg.ListOffsetsRequestTopic{topicReq}

	// Execute the request.
	shards := p.client.RequestSharded(ctx, req)

	for _, shard := range shards {
		if shard.Err != nil {
			return nil, shard.Err
		}

		res := shard.Resp.(*kmsg.ListOffsetsResponse)
		if len(res.Topics) != 1 {
			return nil, fmt.Errorf("unexpected number of topics in the response (expected: %d, got: %d)", 1, len(res.Topics))
		}
		if res.Topics[0].Topic != p.topic {
			return nil, fmt.Errorf("unexpected topic in the response (expected: %s, got: %s)", p.topic, res.Topics[0].Topic)
		}

		for _, pt := range res.Topics[0].Partitions {
			if err := kerr.ErrorForCode(pt.ErrorCode); err != nil {
				return nil, err
			}

			list[p.topic][pt.Partition] = kadm.ListedOffset{
				Topic:       p.topic,
				Partition:   pt.Partition,
				Timestamp:   pt.Timestamp,
				Offset:      pt.Offset,
				LeaderEpoch: pt.LeaderEpoch,
			}
		}
	}

	return list, nil
}

// ListTopicPartitionIDs returns a list of all partition IDs in the topic.
func (p *partitionOffsetClient) ListTopicPartitionIDs(ctx context.Context) ([]int32, error) {
	topics, err := p.admin.ListTopics(ctx, p.topic)
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
	topic, ok := topics[p.topic]
	if !ok {
		return nil, fmt.Errorf("unexpected topic in the response (expected: %s)", p.topic)
	}

	// Extract the partition IDs from the response.
	ids := make([]int32, 0, len(topic.Partitions))
	for _, partition := range topic.Partitions {
		// We don't check partition.Err because all we need is the partition ID;
		// kadm.Client.ListEndOffsets() doesn't check it neither.
		ids = append(ids, partition.Partition)
	}

	// It's not required to sort the partition IDs, but we do it to simplify tests and troubleshooting.
	slices.Sort(ids)

	return ids, nil
}
