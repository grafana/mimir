package wgo

import (
	"errors"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// demotionSuppressionReason is why demotion is suppressed. Its string value is
// the metric label exposed by demotionSuppressedMetric; the empty value means
// demotion is not suppressed.
type demotionSuppressionReason string

const (
	demotionNotSuppressed demotionSuppressionReason = ""
	// demotionSuppressedNoClusterStats: no usable cluster view yet (cold start
	// or too few agents have enough data to qualify), so there's nothing to
	// judge an agent against.
	demotionSuppressedNoClusterStats demotionSuppressionReason = "no_cluster_stats"
	// demotionSuppressedManyFaultyAgents: too many agents are faulty (over the
	// configured MaxFaultyFraction), so demoting any of them would just dump
	// their traffic onto the survivors.
	demotionSuppressedManyFaultyAgents demotionSuppressionReason = "many_faulty_agents"
	// demotionSuppressedManyFaultyAgentsSmallCluster: same as above, but the
	// cluster is small enough that the 1/N floor raised the effective threshold
	// above MaxFaultyFraction and the faulty fraction still exceeded it.
	demotionSuppressedManyFaultyAgentsSmallCluster demotionSuppressionReason = "many_faulty_agents_small_cluster"
)

// DemoterConfig holds the Demoter-specific knobs. The health
// classification thresholds (FaultyThreshold and MaxFaultyFraction) live
// on HealthCheckConfig and are shared with the Hedger so both components
// agree on "is agent X faulty?".
//
// Demotion is intentionally driven only by error rate. Latency-based
// demotion was deliberately removed for two reasons. First, the Hedger
// already handles "slow but working" agents via duplicate-request racing,
// which is cheaper than rerouting plus probe sampling. Second, error rate
// is the safer demotion signal: its threshold is absolute (a fixed
// FaultyThreshold), whereas a latency threshold has to be relative to the
// cluster baseline, which drifts as load redistributes; and error-based
// demotion doesn't cascade, whereas latency does (demote slow agent → load
// redistributes → next slowest deteriorates → demote that one → …).
// Recovery is window-based either way — a demoted agent clears once its bad
// observations age out of the stats window and probes refill it — but once
// demoted the per-agent sample gate relaxes to a single request, so sparse
// probe traffic is enough to re-evaluate and restore it.
type DemoterConfig struct {
	// ProbeInterval is the minimum wall-clock gap between probes to the
	// same demoted agent. Within an interval, the first routing decision
	// that *would* have landed on the demoted agent is converted into a
	// probe; the rest of the decisions in that interval skip past the
	// demoted agent to the next healthy candidate.
	ProbeInterval time.Duration
}

// Validate returns an error if the config is invalid.
func (c *DemoterConfig) Validate() error {
	if c.ProbeInterval <= 0 {
		return errors.New("demoter probe interval must be positive")
	}
	return nil
}

// Demoter wraps a PartitionAssignmentStrategy with a "skip demoted agents,
// occasionally probe them" policy. It exists because of the way the routing
// decision is shaped today: the strategy is consulted on every Produce call
// (and on every cascade retry) for the candidate list, and downstream layers
// (the router that stamps RTPs, the Hedger that picks fallbacks) treat that
// list as ground truth. So the natural place to do "route around this sick
// agent" is right at the candidate-list source, before anyone else reads it.
//
// The probe sampling is per-agent, not per-partition. Two partitions whose
// natural primary is the same demoted agent compete for one probe slot in
// each ProbeInterval window: the first to ask gets the probe, every
// subsequent partition until the next interval boundary is rerouted to its
// secondary candidate. That guarantees a bounded amount of traffic to a
// demoted agent regardless of how many partitions naturally route to it,
// which is the whole point — we want enough probe traffic to know if the
// agent has recovered, not enough to amplify a problem.
//
// Demotion is classified purely on error rate (ErrorRate above
// FaultyThreshold, gated by a minimum request count). Cold-start behaviour
// is fail-open: if cluster stats aren't available yet, no agent is demoted,
// so every candidate flows through unchanged.
type Demoter struct {
	inner      PartitionAssignmentStrategy
	tracker    AgentStatsReader
	healthCfg  HealthCheckConfig
	demoterCfg DemoterConfig
	logger     log.Logger

	// now is injectable for testing; defaults to time.Now.
	now func() time.Time

	// lastDemotedProbe maps nodeID → the last probe timestamp. Entry
	// presence is the "this agent has been classified as demoted" signal
	// that drives the hysteresis branch of isDemoted. isDemoted creates the
	// entry (zero timestamp, so the first probe is immediately due) on the
	// demotion edge and removes it on recovery; shouldProbe and the
	// forced-probe fallback in Candidates stamp it with each probe time.
	// Refresh prunes entries for agents no longer in the pool.
	lastDemotedProbeMu sync.Mutex
	lastDemotedProbe   map[int32]time.Time
}

// NewDemoter wraps inner with the demotion policy described on Demoter.
func NewDemoter(inner PartitionAssignmentStrategy, tracker AgentStatsReader, health HealthCheckConfig, cfg DemoterConfig, logger log.Logger, reg prometheus.Registerer) *Demoter {
	if logger == nil {
		logger = log.NewNopLogger()
	}
	d := &Demoter{
		inner:            inner,
		tracker:          tracker,
		healthCfg:        health,
		demoterCfg:       cfg,
		logger:           logger,
		now:              time.Now,
		lastDemotedProbe: make(map[int32]time.Time),
	}

	promauto.With(reg).NewGaugeFunc(prometheus.GaugeOpts{
		Name: "demoter_demoted_agents",
		Help: "Number of Warpstream agents currently demoted by the Demoter.",
	}, d.demotedAgentsCount)
	newDemotionSuppressedMetric(d, reg)

	return d
}

// Candidates returns the candidate list with demoted agents either elided
// (probe not due) or surfaced as AgentStateDemoted in the primary slot
// (using up that agent's per-interval probe slot). If every candidate is
// demoted and none is due for a probe, the natural primary is surfaced as a
// forced probe so we never refuse to route.
func (d *Demoter) Candidates(topic string, partition int32, maxCandidates int) []Agent {
	if maxCandidates <= 0 {
		return nil
	}
	// Use the shared HealthCheckConfig parameters so the Demoter and the
	// Hedger hit the same ClusterStats cache entry. cluster.SlowThreshold
	// is unused here (latency-based demotion is intentionally not
	// implemented; see DemoterConfig), but passing the shared
	// SlowMultiplier keeps the cache key aligned with the Hedger.
	now := d.now()
	clusterStats, hasClusterStats := d.tracker.ClusterStats(now, d.healthCfg.SlowMultiplier, d.healthCfg.FaultyThreshold)

	// The returned list holds up to maxCandidates entries total
	// (non-demoted alternates plus at most one demoted probe in the
	// primary slot). Worst case we need maxCandidates non-demoted in raw;
	// with a probe filling the primary slot, maxCandidates-1 is enough.
	//
	// Ask for a small headroom and double it each retry, capping at 6
	// doublings (2^6 = 64). Only shouldProbe (the probe-timestamp stamping)
	// is deferred to the build pass below; this loop still calls isDemoted,
	// which can flip lastDemotedProbe membership and log a transition.
	const maxRetries = 6
	var agents []Agent
	for retry, extra := 0, 2; retry < maxRetries; retry, extra = retry+1, extra*2 {
		var (
			asked      = maxCandidates + extra
			nonDemoted = 0
		)

		agents = d.inner.Candidates(topic, partition, asked)

		for _, c := range agents {
			if !d.isDemoted(now, c.NodeID, clusterStats, hasClusterStats) {
				nonDemoted++
			}
		}

		// Stop once we have a full maxCandidates of non-demoted alternates,
		// or we've exhausted the pool. We want the full count even when a
		// demoted probe ends up in the primary slot: a healthy primary
		// followed by a clustered run of demoted agents must still leave
		// enough healthy alternates behind them to fill the list.
		if nonDemoted >= maxCandidates || len(agents) < asked {
			break
		}
	}
	if len(agents) == 0 {
		return nil
	}

	candidates := make([]Agent, 0, maxCandidates)
	for _, agent := range agents {
		if len(candidates) >= maxCandidates {
			break
		}

		// Keep any non-demoted agent.
		if !d.isDemoted(now, agent.NodeID, clusterStats, hasClusterStats) {
			candidates = append(candidates, agent)
			continue
		}

		// Keep a demoted agent only if it's primary and should be probed.
		if primaryAssigned := len(candidates) > 0; primaryAssigned || !d.shouldProbe(now, agent.NodeID) {
			continue
		}

		candidates = append(candidates, agent.cloneWithState(AgentStateDemoted))
	}

	// If every candidate was demoted and none were due for a probe, fall
	// back to the natural primary as a forced probe. We'd rather send
	// traffic to a sick agent than refuse to route at all.
	if len(candidates) == 0 {
		// agents[0] was classified demoted by isDemoted above (which emits the
		// demote log on the demotion edge); here we only record the forced
		// probe time so it counts against the probe interval.
		forced := agents[0].cloneWithState(AgentStateDemoted)
		d.lastDemotedProbeMu.Lock()
		d.lastDemotedProbe[forced.NodeID] = now
		d.lastDemotedProbeMu.Unlock()
		candidates = append(candidates, forced)
	}

	return candidates
}

// isDemoted reports whether agent nodeID currently meets the demotion
// criteria. It also applies the demotion/recovery edge transition: it
// updates lastDemotedProbe (inserting on demote, deleting on recovery) and
// logs the transition once.
func (d *Demoter) isDemoted(now time.Time, nodeID int32, clusterStats ClusterStats, hasClusterStats bool) bool {
	// Suppression guard, which also covers cold-start (no cluster view): we'd
	// rather route fresh traffic to an unknown agent than declare the cluster
	// unusable, and when too many agents are already faulty, demoting more would
	// just dump load onto the survivors.
	if suppressed, _ := d.isDemotionSuppressed(clusterStats, hasClusterStats); suppressed {
		return false
	}
	stats, ok := d.tracker.AgentStats(now, nodeID)
	if !ok {
		return false
	}

	// Hysteresis on the error-rate gate: a never-demoted agent must
	// accumulate at least errorRateMinRequests observations before its
	// ErrorRate counts (suppresses noise on low-volume agents). Once
	// demoted, that protection drops to 1 so the probe-only traffic we
	// route doesn't fall under the gate and oscillate the agent back to
	// healthy. On confirmed recovery the probe state is cleared so the
	// strict gate applies next time.
	minRequests := errorRateMinRequests(clusterStats)

	// Read the demoted state, classify, and apply the transition under a
	// single lock so the read and the act are atomic. Because the act flips
	// the map membership that wasDemoted reads, every later call — a repeat
	// within this Candidates pass or a concurrent one — sees the new state and
	// is a no-op, so each transition is logged exactly once.
	var isFaulty, demoted, restored bool
	{
		d.lastDemotedProbeMu.Lock()

		_, wasDemoted := d.lastDemotedProbe[nodeID]
		if wasDemoted {
			minRequests = 1
		}

		isFaulty = stats.RequestCount >= minRequests && stats.ErrorRate > clusterStats.FaultyThreshold

		switch {
		case isFaulty && !wasDemoted:
			// Register the agent with a zero probe timestamp so its first probe is
			// immediately due (the zero time is always older than ProbeInterval).
			d.lastDemotedProbe[nodeID] = time.Time{}
			demoted = true
		case wasDemoted && !isFaulty:
			// Recovery: clear the probe state so the strict gate applies again
			// next time the agent's health deteriorates.
			delete(d.lastDemotedProbe, nodeID)
			restored = true
		}

		d.lastDemotedProbeMu.Unlock()
	}

	switch {
	case demoted:
		level.Info(d.logger).Log("msg", "warpstream agent demoted", "node_id", nodeID, "error_rate", stats.ErrorRate, "request_count", stats.RequestCount, "min_requests", minRequests, "faulty_threshold", clusterStats.FaultyThreshold)
	case restored:
		level.Info(d.logger).Log("msg", "warpstream agent restored", "node_id", nodeID)
	}

	return isFaulty
}

// isDemotionSuppressed reports whether demotion is currently suppressed, and why:
// either there is no cluster view (hasClusterStats is false), or too many agents
// are already faulty so demoting any of them would just dump their traffic onto
// the survivors. Applies the same scale-aware 1/N floor as the Hedger so a single
// bad agent in a tiny cluster never trips the suppression.
func (d *Demoter) isDemotionSuppressed(clusterStats ClusterStats, hasClusterStats bool) (bool, demotionSuppressionReason) {
	if !hasClusterStats {
		return true, demotionSuppressedNoClusterStats
	}
	floor := maxFractionFloor(d.healthCfg.MaxFaultyFraction, clusterStats.FaultyContributorsCount)
	if clusterStats.FaultyFraction <= floor {
		return false, demotionNotSuppressed
	}
	if floor > d.healthCfg.MaxFaultyFraction {
		return true, demotionSuppressedManyFaultyAgentsSmallCluster
	}
	return true, demotionSuppressedManyFaultyAgents
}

// Refresh reconciles demotion state against the agents currently in the pool,
// dropping per-agent probe entries for any node ID no longer present. Called
// on every metadata refresh, it clears state for departed agents and, by
// reconciling against ground truth rather than a removed-diff, also serves as
// a backstop so a leaked entry can't keep inflating demoter_demoted_agents.
func (d *Demoter) Refresh(currentAgents []int32) {
	current := make(map[int32]struct{}, len(currentAgents))
	for _, id := range currentAgents {
		current[id] = struct{}{}
	}
	d.lastDemotedProbeMu.Lock()
	defer d.lastDemotedProbeMu.Unlock()
	for nodeID := range d.lastDemotedProbe {
		if _, ok := current[nodeID]; !ok {
			delete(d.lastDemotedProbe, nodeID)
		}
	}
}

func (d *Demoter) demotedAgentsCount() float64 {
	// While demotion is suppressed isDemoted treats every agent as non-demoted,
	// so no agent is actually being routed around: report 0 rather than the
	// stale lastDemotedProbe entries left from before suppression tripped, which
	// would otherwise contradict demoter_demotion_suppressed.
	clusterStats, hasClusterStats := d.tracker.ClusterStats(d.now(), d.healthCfg.SlowMultiplier, d.healthCfg.FaultyThreshold)
	if suppressed, _ := d.isDemotionSuppressed(clusterStats, hasClusterStats); suppressed {
		return 0
	}
	d.lastDemotedProbeMu.Lock()
	defer d.lastDemotedProbeMu.Unlock()
	return float64(len(d.lastDemotedProbe))
}

// shouldProbe returns true if the caller should issue the probe, atomically
// taking the slot in that case. A freshly demoted agent carries a zero
// timestamp, so its first probe is always due.
func (d *Demoter) shouldProbe(now time.Time, nodeID int32) bool {
	d.lastDemotedProbeMu.Lock()
	defer d.lastDemotedProbeMu.Unlock()
	if now.Sub(d.lastDemotedProbe[nodeID]) < d.demoterCfg.ProbeInterval {
		return false
	}
	d.lastDemotedProbe[nodeID] = now
	return true
}

// demotionSuppressedMetric is a prometheus.Collector that exposes
// demoter_demotion_suppressed, one series per suppression reason. It takes a
// single ClusterStats snapshot per scrape and sets at most one series to 1 (the
// active reason); all others are 0, so the breakdown is always mutually
// consistent and no series is ever missing. sum() over the series reproduces the
// plain "is demotion suppressed" 0/1 signal.
type demotionSuppressedMetric struct {
	d    *Demoter
	desc *prometheus.Desc
}

func newDemotionSuppressedMetric(d *Demoter, reg prometheus.Registerer) *demotionSuppressedMetric {
	m := &demotionSuppressedMetric{
		d: d,
		desc: prometheus.NewDesc(
			"demoter_demotion_suppressed",
			"Whether the Demoter is currently suppressing all demotions (1) and why, broken down by reason; 0 for inactive reasons.",
			[]string{"reason"}, nil,
		),
	}
	if reg != nil {
		reg.MustRegister(m)
	}
	return m
}

func (m *demotionSuppressedMetric) Describe(ch chan<- *prometheus.Desc) {
	ch <- m.desc
}

func (m *demotionSuppressedMetric) Collect(ch chan<- prometheus.Metric) {
	clusterStats, hasClusterStats := m.d.tracker.ClusterStats(m.d.now(), m.d.healthCfg.SlowMultiplier, m.d.healthCfg.FaultyThreshold)
	_, actualReason := m.d.isDemotionSuppressed(clusterStats, hasClusterStats)

	for _, reason := range []demotionSuppressionReason{
		demotionSuppressedNoClusterStats,
		demotionSuppressedManyFaultyAgents,
		demotionSuppressedManyFaultyAgentsSmallCluster,
	} {
		value := 0.0
		if reason == actualReason {
			value = 1
		}
		ch <- prometheus.MustNewConstMetric(m.desc, prometheus.GaugeValue, value, string(reason))
	}
}
