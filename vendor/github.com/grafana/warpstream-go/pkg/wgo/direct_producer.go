package wgo

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// DirectProducer produces pre-encoded partitions to a specific Warpstream agent
// by its Kafka NodeID. Implementations must be safe for concurrent use. The wire
// request is assembled at the lowest layer (KafkaDirectProducer) from the
// already-encoded batches; upper layers just shuffle partition groups around.
//
// The signature takes bare encodedTopicPartitionRecords (without nodeID, done,
// or nodeState) so the "which agent does this batch go to" decision lives
// in exactly one place — the nodeID parameter — and the function cannot
// be called with a mismatched per-partition nodeID.
type DirectProducer interface {
	ProduceSync(ctx context.Context, nodeID int32, partitions []encodedTopicPartitionRecords) ProduceResult
}

// KafkaDirectProducerConfig holds the per-request timings for
// KafkaDirectProducer.
type KafkaDirectProducerConfig struct {
	// ProduceRequestTimeout is the agent-side deadline written into
	// ProduceRequest.TimeoutMillis. The agent replies with REQUEST_TIMED_OUT
	// after this much wall-clock without committing.
	ProduceRequestTimeout time.Duration

	// ProduceRequestTimeoutOverhead is added on top of ProduceRequestTimeout
	// to compute the client-side ctx deadline. Always strictly greater than zero
	// so the client never kills a connection while the agent is still legitimately
	// working.
	ProduceRequestTimeoutOverhead time.Duration
}

// Validate returns an error if the config is invalid.
func (c *KafkaDirectProducerConfig) Validate() error {
	if c.ProduceRequestTimeout <= 0 {
		return errors.New("produce request timeout must be positive")
	}
	if c.ProduceRequestTimeoutOverhead <= 0 {
		return errors.New("produce request timeout overhead must be positive")
	}
	return nil
}

// KafkaDirectProducer implements DirectProducer using kgo.Client. Wire request
// assembly (buildMultiTopicProduceRequestFromEncoded) and per-request timing are
// enforced here, not at a higher layer.
//
// This is the only type in this package that depends on kgo.Client directly.
type KafkaDirectProducer struct {
	client  *kgo.Client
	topicID func(string) ([16]byte, bool)
	version int16
	cfg     KafkaDirectProducerConfig
	metrics *metrics

	// onProduceResponse is a generic post-response hook invoked after
	// broker.Request returns and before the response is propagated up
	// the chain. The hook receives the per-attempt ctx, the response (may
	// be nil on transport error) and the error. Used by tests to inject
	// behavior such as artificial latency. Production code leaves it nil.
	onProduceResponse func(ctx context.Context, nodeID int32, resp *kmsg.ProduceResponse, err error)
}

// NewKafkaDirectProducer returns a KafkaDirectProducer backed by client.
// topicID resolves a topic name to its Kafka UUID at request-build time.
func NewKafkaDirectProducer(client *kgo.Client, topicID func(string) ([16]byte, bool), version int16, cfg KafkaDirectProducerConfig, m *metrics) *KafkaDirectProducer {
	return &KafkaDirectProducer{client: client, topicID: topicID, version: version, cfg: cfg, metrics: m}
}

// ProduceSync implements DirectProducer.
func (s *KafkaDirectProducer) ProduceSync(ctx context.Context, nodeID int32, partitions []encodedTopicPartitionRecords) (retResult ProduceResult) {
	req, reqStats, err := buildMultiTopicProduceRequestFromEncoded(s.version, s.topicID, partitions)
	if err != nil {
		return ProduceResult{err: err}
	}
	req.SetTimeout(int32(s.cfg.ProduceRequestTimeout.Milliseconds()))

	attemptCtx, cancel := context.WithTimeout(ctx, s.cfg.ProduceRequestTimeout+s.cfg.ProduceRequestTimeoutOverhead)
	defer cancel()

	s.metrics.produceDirectRequestsTotal.Inc()

	// Observe per-attempt latency and the failure reason at the single exit.
	// Classify via the returned ProduceResult, not the transport error, so a
	// response carrying a per-partition error code (with no transport error)
	// is counted as a failure.
	reqStart := time.Now()
	defer func() {
		latencySeconds := time.Since(reqStart).Seconds()
		if retResult.succeeded() {
			s.metrics.produceDirectRequestLatencySuccess.Observe(latencySeconds)

			// Producer-state counters: count the request's batches once, only
			// when the whole request is acked. The request is all-or-nothing
			// (see produceResultAccumulator), so a failed attempt counts
			// nothing and is retried; this mirrors franz-go, which counts each
			// batch once on success.
			s.metrics.produceWireRecordsTotal.Add(float64(reqStats.records))
			s.metrics.produceWireBatchesTotal.Add(float64(reqStats.batches))
			s.metrics.produceWireBytesTotal.Add(float64(reqStats.uncompressedBytes))
			s.metrics.produceWireCompressedBytesTotal.Add(float64(reqStats.compressedBytes))
		} else {
			reason := getProduceResultErr(retResult.error()).reason
			s.metrics.produceDirectRequestLatencyFailure.WithLabelValues(reason).Observe(latencySeconds)
			s.metrics.produceDirectRequestsFailedTotal.WithLabelValues(reason).Inc()
		}
	}()

	// Uses Broker.Request (not RetriableRequest) so franz-go's internal retry
	// loop stays out of the way: retries are owned by the caller.
	resp, err := s.client.Broker(int(nodeID)).Request(attemptCtx, req)

	// Decode the response up front so the hook always observes the same
	// typed value the caller would see.
	var pr *kmsg.ProduceResponse
	if err == nil {
		var ok bool
		pr, ok = resp.(*kmsg.ProduceResponse)
		if !ok {
			return ProduceResult{err: fmt.Errorf("unexpected response type %T", resp)}
		}
	}

	if s.onProduceResponse != nil {
		s.onProduceResponse(attemptCtx, nodeID, pr, err)
		// The hook may have slept past the attempt deadline.
		if ctxErr := attemptCtx.Err(); ctxErr != nil {
			return ProduceResult{err: ctxErr}
		}
	}

	if err != nil {
		return ProduceResult{err: err}
	}
	return ProduceResult{resp: pr}
}
