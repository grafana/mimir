package wgo

import "time"

// ObservedAgentStatsTracker decorates an AgentStatsTracker, recording the
// cluster-view gauges on each ClusterStats it forwards. Every other method is
// a plain passthrough.
type ObservedAgentStatsTracker struct {
	inner   AgentStatsTracker
	metrics *metrics
}

func NewObservedAgentStatsTracker(inner AgentStatsTracker, m *metrics) *ObservedAgentStatsTracker {
	return &ObservedAgentStatsTracker{inner: inner, metrics: m}
}

func (o *ObservedAgentStatsTracker) TrackAgentRequest(now time.Time, nodeID int32, latency time.Duration, err error) {
	o.inner.TrackAgentRequest(now, nodeID, latency, err)
}

func (o *ObservedAgentStatsTracker) AgentStats(now time.Time, nodeID int32) (AgentStats, bool) {
	return o.inner.AgentStats(now, nodeID)
}

func (o *ObservedAgentStatsTracker) PurgeAgents(nodeIDs []int32) {
	o.inner.PurgeAgents(nodeIDs)
}

func (o *ObservedAgentStatsTracker) ClusterStats(now time.Time, slowMultiplier, faultyThreshold float64) (ClusterStats, bool) {
	stats, ok := o.inner.ClusterStats(now, slowMultiplier, faultyThreshold)
	o.metrics.observeClusterStats(stats, ok)
	return stats, ok
}
