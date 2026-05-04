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

// Produce implements DirectProducer.
func (p *TrackingProducer) Produce(ctx context.Context, nodeID int32, req *kmsg.ProduceRequest) (*kmsg.ProduceResponse, error) {
	start := time.Now()
	resp, err := p.inner.Produce(ctx, nodeID, req)
	p.tracker.TrackAgentRequest(time.Now(), nodeID, time.Since(start), err)
	return resp, err
}
