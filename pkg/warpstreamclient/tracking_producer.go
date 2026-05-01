// SPDX-License-Identifier: AGPL-3.0-only

package warpstreamclient

import (
	"context"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// TrackingProducer is a DirectProducer decorator that records the outcome —
// observed latency and error — of every Produce call into an AgentStatsTracker.
type TrackingProducer struct {
	inner   DirectProducer
	tracker AgentStatsTracker
}

// NewTrackingProducer returns a TrackingProducer that forwards to inner and
// records each call's latency and error to tracker.
func NewTrackingProducer(inner DirectProducer, tracker AgentStatsTracker) *TrackingProducer {
	return &TrackingProducer{inner: inner, tracker: tracker}
}

// ProduceSync implements DirectProducer.
func (p *TrackingProducer) ProduceSync(ctx context.Context, nodeID int32, partitions []topicPartitionRecords) (*kmsg.ProduceResponse, error) {
	start := time.Now()
	resp, err := p.inner.ProduceSync(ctx, nodeID, partitions)
	// Use produceResult.error() so the tracked error is consistent with what
	// the Hedger sees: per-partition response codes count as agent errors,
	// not just transport-level failures.
	p.tracker.TrackAgentRequest(time.Now(), nodeID, time.Since(start), produceResult{resp: resp, err: err}.error())
	return resp, err
}
