// SPDX-License-Identifier: AGPL-3.0-only

package warpstreamclient

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// DirectProducer produces partitions to a specific Warpstream agent by its
// Kafka NodeID. Implementations must be safe for concurrent use. The wire
// encoding of partitions into a ProduceRequest happens at the lowest layer
// (KafkaDirectProducer); upper layers just shuffle partition groups around.
//
// The signature takes bare topicPartitionRecords (without nodeID, done,
// or nodeState) so the "which agent does this batch go to" decision lives
// in exactly one place — the nodeID parameter — and the function cannot
// be called with a mismatched per-RTP nodeID.
type DirectProducer interface {
	ProduceSync(ctx context.Context, nodeID int32, partitions []topicPartitionRecords) (*kmsg.ProduceResponse, error)
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

// KafkaDirectProducer implements DirectProducer using kgo.Client. Wire
// encoding (buildMultiTopicProduceRequest) and per-request timing are
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
func (s *KafkaDirectProducer) ProduceSync(ctx context.Context, nodeID int32, partitions []topicPartitionRecords) (*kmsg.ProduceResponse, error) {
	var records []*kgo.Record
	for _, p := range partitions {
		records = append(records, p.records...)
	}
	req, err := buildMultiTopicProduceRequest(s.version, s.topicID, records)
	if err != nil {
		return nil, err
	}
	req.SetTimeout(int32(s.cfg.ProduceRequestTimeout.Milliseconds()))

	attemptCtx, cancel := context.WithTimeout(ctx, s.cfg.ProduceRequestTimeout+s.cfg.ProduceRequestTimeoutOverhead)
	defer cancel()

	// Uses Broker.Request (not RetriableRequest) so franz-go's internal retry
	// loop stays out of the way: retries are owned by the caller.
	s.metrics.produceRequestsTotal.Inc()
	resp, err := s.client.Broker(int(nodeID)).Request(attemptCtx, req)

	// Decode the response up front so the hook always observes the same
	// typed value the caller would see.
	var pr *kmsg.ProduceResponse
	if err == nil {
		var ok bool
		pr, ok = resp.(*kmsg.ProduceResponse)
		if !ok {
			s.metrics.produceRequestsFailedTotal.WithLabelValues("unknown").Inc()
			return nil, fmt.Errorf("unexpected response type %T", resp)
		}
	}

	if s.onProduceResponse != nil {
		s.onProduceResponse(attemptCtx, nodeID, pr, err)
		// The hook may have slept past the attempt deadline.
		if ctxErr := attemptCtx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
	}

	if err != nil {
		s.metrics.produceRequestsFailedTotal.WithLabelValues(getProduceResultErr(err).reason).Inc()
		return nil, err
	}
	return pr, nil
}
