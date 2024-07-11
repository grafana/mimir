// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"context"
	"errors"
	"fmt"
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

// FetchLastProducedOffset fetches and returns the last produced offset for a partition, or -1 if no record has
// been ever produced in the partition. This function issues a single request, but the Kafka client used under the
// hood may retry a failed request until the retry timeout is hit.
func (p *partitionOffsetClient) FetchLastProducedOffset(ctx context.Context, partitionID int32) (_ int64, returnErr error) {
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
