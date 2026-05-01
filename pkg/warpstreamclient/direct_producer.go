// SPDX-License-Identifier: AGPL-3.0-only

package warpstreamclient

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// DirectProducer produces to a specific Warpstream agent by its Kafka NodeID.
// Implementations must be safe for concurrent use.
type DirectProducer interface {
	Produce(ctx context.Context, nodeID int32, req *kmsg.ProduceRequest) (*kmsg.ProduceResponse, error)
}

// KafkaDirectProducer implements DirectProducer using kgo.Client.
// This is the only type in this package that depends on kgo.Client directly.
type KafkaDirectProducer struct {
	client *kgo.Client
}

// NewKafkaDirectProducer returns a KafkaDirectProducer backed by client.
func NewKafkaDirectProducer(client *kgo.Client) *KafkaDirectProducer {
	return &KafkaDirectProducer{client: client}
}

// Produce implements DirectProducer.
func (s *KafkaDirectProducer) Produce(ctx context.Context, nodeID int32, req *kmsg.ProduceRequest) (*kmsg.ProduceResponse, error) {
	resp, err := s.client.Broker(int(nodeID)).RetriableRequest(ctx, req)
	if err != nil {
		return nil, err
	}
	pr, ok := resp.(*kmsg.ProduceResponse)
	if !ok {
		return nil, fmt.Errorf("unexpected response type %T", resp)
	}
	return pr, nil
}
