// SPDX-License-Identifier: AGPL-3.0-only

package costattribution

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/grafana/mimir/pkg/costattribution/costattributionmodel"
	"github.com/grafana/mimir/pkg/util/validation"
)

const (
	trackerLabel        = "tracker"
	tenantLabel         = "tenant"
	reasonLabel         = "reason"
	defaultTrackerName  = "cost-attribution"
	missingValue        = "__missing__"
	overflowValue       = "__overflow__"
	activeSeriesTracker = "active-series"
	samplesTracker      = "samples"
)

// resolvedTrackerConfig is a TrackerConfig with all defaults filled in.
type resolvedTrackerConfig struct {
	labels           costattributionmodel.Labels
	maxCardinality   int
	cooldownDuration time.Duration
}

type Manager struct {
	services.Service
	logger log.Logger
	limits *validation.Overrides

	sampleTrackerCardinalityDesc       *descriptor
	sampleTrackerOverflowDesc          *descriptor
	activeSeriesTrackerCardinalityDesc *descriptor
	activeSeriesTrackerOverflowDesc    *descriptor
	trackerCreationErrors              *prometheus.CounterVec

	inactiveTimeout time.Duration

	// userID → trackerName → *SampleTracker
	stmtx                  sync.RWMutex
	sampleTrackersByUserID map[string]map[string]*SampleTracker

	// userID → trackerName → *ActiveSeriesTracker
	atmtx                  sync.RWMutex
	activeTrackersByUserID map[string]map[string]*ActiveSeriesTracker
}

func NewManager(cleanupInterval, inactiveTimeout time.Duration, logger log.Logger, limits *validation.Overrides, reg, costAttributionReg prometheus.Registerer) (*Manager, error) {
	m := &Manager{
		stmtx:                  sync.RWMutex{},
		sampleTrackersByUserID: make(map[string]map[string]*SampleTracker),

		atmtx:                  sync.RWMutex{},
		activeTrackersByUserID: make(map[string]map[string]*ActiveSeriesTracker),

		trackerCreationErrors: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_cost_attribution_tracker_creation_errors_total",
			Help: "The total number of errors creating cost attribution trackers for each user.",
		}, []string{"user", "tracker_type", "tracker_name"}),

		limits:          limits,
		inactiveTimeout: inactiveTimeout,
		logger:          logger,
	}

	if err := m.createAndValidateDescriptors(); err != nil {
		return nil, err
	}

	m.Service = services.NewTimerService(cleanupInterval, nil, m.iteration, nil).WithName("cost attribution manager")
	if err := reg.Register(m); err != nil {
		return nil, fmt.Errorf("can't register operational metrics: %w", err)
	}
	if err := costAttributionReg.Register(costAttributionCollector{m}); err != nil {
		return nil, fmt.Errorf("can't register cost attribution metrics: %w", err)
	}
	return m, nil
}

func (m *Manager) createAndValidateDescriptors() error {
	var err error
	if m.sampleTrackerCardinalityDesc, err = newDescriptor("cortex_cost_attribution_sample_tracker_cardinality",
		"The cardinality of a cost attribution sample tracker for each user.",
		[]string{"user", trackerLabel},
		nil); err != nil {
		return err
	}
	if m.sampleTrackerOverflowDesc, err = newDescriptor("cortex_cost_attribution_sample_tracker_overflown",
		"This metric is exported with value 1 when a sample tracker for a user is overflown. It's not exported otherwise.",
		[]string{"user", trackerLabel},
		nil); err != nil {
		return err
	}
	if m.activeSeriesTrackerCardinalityDesc, err = newDescriptor("cortex_cost_attribution_active_series_tracker_cardinality",
		"The cardinality of a cost attribution active series tracker for each user.",
		[]string{"user", trackerLabel},
		nil); err != nil {
		return err
	}
	if m.activeSeriesTrackerOverflowDesc, err = newDescriptor("cortex_cost_attribution_active_series_tracker_overflown",
		"This metric is exported with value 1 when an active series tracker for a user is overflown. It's not exported otherwise.",
		[]string{"user", trackerLabel},
		nil); err != nil {
		return err
	}
	return nil
}

func (m *Manager) iteration(_ context.Context) error {
	m.purgeInactiveAttributionsUntil(time.Now())
	return nil
}

// effectiveTrackerConfigs resolves the effective tracker configs for a user,
// merging the legacy single-tracker config with the new multi-tracker map.
func (m *Manager) effectiveTrackerConfigs(userID string) map[string]resolvedTrackerConfig {
	if m == nil {
		return nil
	}
	legacyLabels := m.limits.CostAttributionLabelsStructured(userID)
	trackers := m.limits.AdditionalCostAttributionTrackers(userID)
	defaultMaxCardinality := m.limits.MaxCostAttributionCardinality(userID)
	defaultCooldown := m.limits.CostAttributionCooldown(userID)

	result := make(map[string]resolvedTrackerConfig, len(trackers)+1)

	if len(legacyLabels) > 0 {
		result[defaultTrackerName] = resolvedTrackerConfig{
			labels:           legacyLabels,
			maxCardinality:   defaultMaxCardinality,
			cooldownDuration: defaultCooldown,
		}
	}

	for name, cfg := range trackers {
		if len(cfg.Labels) == 0 {
			continue
		}
		maxCard := defaultMaxCardinality
		if cfg.MaxCardinality > 0 {
			maxCard = cfg.MaxCardinality
		}
		cooldown := defaultCooldown
		if cfg.Cooldown > 0 {
			cooldown = time.Duration(cfg.Cooldown)
		}
		result[name] = resolvedTrackerConfig{
			labels:           cfg.Labels,
			maxCardinality:   maxCard,
			cooldownDuration: cooldown,
		}
	}

	return result
}

func sortLabels(labels costattributionmodel.Labels) costattributionmodel.Labels {
	slices.SortFunc(labels, func(a, b costattributionmodel.Label) int {
		return strings.Compare(a.Input, b.Input)
	})
	return labels
}

func (m *Manager) SampleTracker(userID string) *CompositeSampleTracker {
	configs := m.effectiveTrackerConfigs(userID)
	if len(configs) == 0 {
		return nil
	}

	m.stmtx.RLock()
	userTrackers := m.sampleTrackersByUserID[userID]
	if userTrackers != nil && len(userTrackers) == len(configs) {
		allExist := true
		for name := range configs {
			if _, ok := userTrackers[name]; !ok {
				allExist = false
				break
			}
		}
		if allExist {
			trackers := make([]*SampleTracker, 0, len(userTrackers))
			for _, name := range sortedKeys(configs) {
				trackers = append(trackers, userTrackers[name])
			}
			m.stmtx.RUnlock()
			return newCompositeSampleTracker(trackers)
		}
	}
	m.stmtx.RUnlock()

	m.stmtx.Lock()
	defer m.stmtx.Unlock()

	if m.sampleTrackersByUserID[userID] == nil {
		m.sampleTrackersByUserID[userID] = make(map[string]*SampleTracker, len(configs))
	}
	userTrackers = m.sampleTrackersByUserID[userID]

	for name, cfg := range configs {
		if _, exists := userTrackers[name]; exists {
			continue
		}
		labels := sortLabels(cfg.labels)
		tracker, err := newSampleTracker(userID, name, labels, cfg.maxCardinality, cfg.cooldownDuration, m.logger)
		if err != nil {
			m.trackerCreationErrors.WithLabelValues(userID, samplesTracker, name).Inc()
			continue
		}
		userTrackers[name] = tracker
	}

	trackers := make([]*SampleTracker, 0, len(userTrackers))
	for _, name := range sortedKeys(configs) {
		if t, ok := userTrackers[name]; ok {
			trackers = append(trackers, t)
		}
	}
	return newCompositeSampleTracker(trackers)
}

func (m *Manager) ActiveSeriesTracker(userID string) *CompositeActiveSeriesTracker {
	configs := m.effectiveTrackerConfigs(userID)
	if len(configs) == 0 {
		return nil
	}

	m.atmtx.RLock()
	userTrackers := m.activeTrackersByUserID[userID]
	if userTrackers != nil && len(userTrackers) == len(configs) {
		allExist := true
		for name := range configs {
			if _, ok := userTrackers[name]; !ok {
				allExist = false
				break
			}
		}
		if allExist {
			trackers := make([]*ActiveSeriesTracker, 0, len(userTrackers))
			for _, name := range sortedKeys(configs) {
				trackers = append(trackers, userTrackers[name])
			}
			m.atmtx.RUnlock()
			return NewCompositeActiveSeriesTracker(trackers)
		}
	}
	m.atmtx.RUnlock()

	m.atmtx.Lock()
	defer m.atmtx.Unlock()

	if m.activeTrackersByUserID[userID] == nil {
		m.activeTrackersByUserID[userID] = make(map[string]*ActiveSeriesTracker, len(configs))
	}
	userTrackers = m.activeTrackersByUserID[userID]

	for name, cfg := range configs {
		if _, exists := userTrackers[name]; exists {
			continue
		}
		labels := sortLabels(cfg.labels)
		tracker, err := NewActiveSeriesTracker(userID, name, labels, cfg.maxCardinality, cfg.cooldownDuration, m.logger)
		if err != nil {
			m.trackerCreationErrors.WithLabelValues(userID, activeSeriesTracker, name).Inc()
			continue
		}
		userTrackers[name] = tracker
	}

	trackers := make([]*ActiveSeriesTracker, 0, len(userTrackers))
	for _, name := range sortedKeys(configs) {
		if t, ok := userTrackers[name]; ok {
			trackers = append(trackers, t)
		}
	}
	return NewCompositeActiveSeriesTracker(trackers)
}

func (m *Manager) Collect(out chan<- prometheus.Metric) {
	m.stmtx.RLock()
	sampleTrackersByUserID := make(map[string]map[string]*SampleTracker, len(m.sampleTrackersByUserID))
	for userID, trackers := range m.sampleTrackersByUserID {
		sampleTrackersByUserID[userID] = maps.Clone(trackers)
	}
	m.stmtx.RUnlock()

	m.atmtx.RLock()
	activeTrackersByUserID := make(map[string]map[string]*ActiveSeriesTracker, len(m.activeTrackersByUserID))
	for userID, trackers := range m.activeTrackersByUserID {
		activeTrackersByUserID[userID] = maps.Clone(trackers)
	}
	m.atmtx.RUnlock()

	for userID, trackers := range sampleTrackersByUserID {
		for trackerName, tracker := range trackers {
			cardinality, overflown := tracker.cardinality()
			out <- m.sampleTrackerCardinalityDesc.gauge(float64(cardinality), userID, trackerName)
			if overflown {
				out <- m.sampleTrackerOverflowDesc.gauge(1, userID, trackerName)
			}
		}
	}

	for userID, trackers := range activeTrackersByUserID {
		for trackerName, tracker := range trackers {
			cardinality, overflown := tracker.cardinality()
			out <- m.activeSeriesTrackerCardinalityDesc.gauge(float64(cardinality), userID, trackerName)
			if overflown {
				out <- m.activeSeriesTrackerOverflowDesc.gauge(1, userID, trackerName)
			}
		}
	}
}

func (m *Manager) Describe(chan<- *prometheus.Desc) {
	// Describe is not implemented because the metrics include dynamic labels. The Manager functions as an unchecked exporter.
	// For more details, refer to the documentation: https://pkg.go.dev/github.com/prometheus/client_golang/prometheus#hdr-Custom_Collectors_and_constant_Metrics
}

func (m *Manager) collectCostAttribution(out chan<- prometheus.Metric) {
	m.stmtx.RLock()
	sampleTrackersByUserID := make(map[string]map[string]*SampleTracker, len(m.sampleTrackersByUserID))
	for userID, trackers := range m.sampleTrackersByUserID {
		sampleTrackersByUserID[userID] = maps.Clone(trackers)
	}
	m.stmtx.RUnlock()

	m.atmtx.RLock()
	activeTrackersByUserID := make(map[string]map[string]*ActiveSeriesTracker, len(m.activeTrackersByUserID))
	for userID, trackers := range m.activeTrackersByUserID {
		activeTrackersByUserID[userID] = maps.Clone(trackers)
	}
	m.atmtx.RUnlock()

	for _, trackers := range sampleTrackersByUserID {
		for _, tracker := range trackers {
			tracker.collectCostAttribution(out)
		}
	}

	for _, trackers := range activeTrackersByUserID {
		for _, tracker := range trackers {
			tracker.Collect(out)
		}
	}
}

func (m *Manager) describeCostAttribution(chan<- *prometheus.Desc) {
	// Describe is not implemented because the metrics include dynamic labels. The Manager functions as an unchecked exporter.
	// For more details, refer to the documentation: https://pkg.go.dev/github.com/prometheus/client_golang/prometheus#hdr-Custom_Collectors_and_constant_Metrics
}

func (m *Manager) deleteSampleTracker(userID, trackerName string) {
	m.stmtx.Lock()
	if trackers, ok := m.sampleTrackersByUserID[userID]; ok {
		delete(trackers, trackerName)
		if len(trackers) == 0 {
			delete(m.sampleTrackersByUserID, userID)
		}
	}
	m.stmtx.Unlock()
}

func (m *Manager) deleteActiveTracker(userID, trackerName string) {
	m.atmtx.Lock()
	if trackers, ok := m.activeTrackersByUserID[userID]; ok {
		delete(trackers, trackerName)
		if len(trackers) == 0 {
			delete(m.activeTrackersByUserID, userID)
		}
	}
	m.atmtx.Unlock()
}

func (m *Manager) deleteAllTrackersForUser(userID string) {
	m.stmtx.Lock()
	delete(m.sampleTrackersByUserID, userID)
	m.stmtx.Unlock()

	m.atmtx.Lock()
	delete(m.activeTrackersByUserID, userID)
	m.atmtx.Unlock()
}

// updateTrackers ensures trackers for userID match the current config.
// Returns per-tracker-name sample and active series trackers that are still active.
func (m *Manager) updateTrackers(userID string) (map[string]*SampleTracker, map[string]*ActiveSeriesTracker) {
	configs := m.effectiveTrackerConfigs(userID)
	if len(configs) == 0 {
		m.deleteAllTrackersForUser(userID)
		return nil, nil
	}

	// Ensure all configured trackers exist and are up-to-date.
	// Force creation via SampleTracker/ActiveSeriesTracker.
	m.SampleTracker(userID)
	m.ActiveSeriesTracker(userID)

	// Remove trackers no longer in config.
	m.stmtx.RLock()
	existingST := maps.Clone(m.sampleTrackersByUserID[userID])
	m.stmtx.RUnlock()

	m.atmtx.RLock()
	existingAT := maps.Clone(m.activeTrackersByUserID[userID])
	m.atmtx.RUnlock()

	for name := range existingST {
		if _, ok := configs[name]; !ok {
			m.deleteSampleTracker(userID, name)
			delete(existingST, name)
		}
	}
	for name := range existingAT {
		if _, ok := configs[name]; !ok {
			m.deleteActiveTracker(userID, name)
			delete(existingAT, name)
		}
	}

	// Check if config changed for existing trackers; recreate if needed.
	for name, cfg := range configs {
		labels := sortLabels(cfg.labels)

		if st, ok := existingST[name]; ok {
			if !st.hasSameLabels(labels) || st.maxCardinality != cfg.maxCardinality || st.cooldownDuration != cfg.cooldownDuration {
				m.stmtx.Lock()
				newST, err := newSampleTracker(userID, name, labels, cfg.maxCardinality, cfg.cooldownDuration, m.logger)
				if err != nil {
					m.trackerCreationErrors.WithLabelValues(userID, samplesTracker, name).Inc()
					delete(m.sampleTrackersByUserID[userID], name)
					delete(existingST, name)
				} else {
					m.sampleTrackersByUserID[userID][name] = newST
					existingST[name] = newST
				}
				m.stmtx.Unlock()
			}
		}

		if at, ok := existingAT[name]; ok {
			if !at.hasSameLabels(labels) || at.maxCardinality != cfg.maxCardinality || at.cooldownDuration != cfg.cooldownDuration {
				m.atmtx.Lock()
				newAT, err := NewActiveSeriesTracker(userID, name, labels, cfg.maxCardinality, cfg.cooldownDuration, m.logger)
				if err != nil {
					m.trackerCreationErrors.WithLabelValues(userID, activeSeriesTracker, name).Inc()
					delete(m.activeTrackersByUserID[userID], name)
					delete(existingAT, name)
				} else {
					m.activeTrackersByUserID[userID][name] = newAT
					existingAT[name] = newAT
				}
				m.atmtx.Unlock()
			}
		}
	}

	return existingST, existingAT
}

func (m *Manager) purgeInactiveAttributionsUntil(now time.Time) {
	deadline := now.Add(-m.inactiveTimeout)

	// Collect all userIDs that have trackers.
	m.stmtx.RLock()
	userIDs := make(map[string]struct{}, len(m.sampleTrackersByUserID)+len(m.activeTrackersByUserID))
	for userID := range m.sampleTrackersByUserID {
		userIDs[userID] = struct{}{}
	}
	m.stmtx.RUnlock()

	m.atmtx.RLock()
	for userID := range m.activeTrackersByUserID {
		userIDs[userID] = struct{}{}
	}
	m.atmtx.RUnlock()

	for userID := range userIDs {
		sampleTrackers, activeTrackers := m.updateTrackers(userID)
		if sampleTrackers == nil && activeTrackers == nil {
			continue
		}

		for name, st := range sampleTrackers {
			st.cleanupInactiveObservations(deadline)
			if st.recoveredFromOverflow(deadline) {
				m.deleteSampleTracker(userID, name)
			}
		}

		for name, at := range activeTrackers {
			at.observedMtx.RLock()
			isOverflowedAndShouldCheck := !at.overflowSince.IsZero() && at.overflowSince.Add(at.cooldownDuration).Before(deadline)
			recovered := len(at.observed) <= at.maxCardinality
			at.observedMtx.RUnlock()

			if isOverflowedAndShouldCheck {
				if recovered {
					m.deleteActiveTracker(userID, name)
				} else {
					at.observedMtx.Lock()
					at.overflowSince = now
					at.observedMtx.Unlock()
				}
			}
		}
	}
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

var _ prometheus.Collector = (*costAttributionCollector)(nil)

// costAttributionCollector is a prometheus collector that collects cost attribution metrics.
// It collects metrics from methods that are explicit on their purpose: they are cost attribution metrics.
// This way it's clear which are the usual operational metrics and which ones are the cost attribution metrics.
type costAttributionCollector struct {
	ca interface {
		describeCostAttribution(chan<- *prometheus.Desc)
		collectCostAttribution(chan<- prometheus.Metric)
	}
}

func (c costAttributionCollector) Describe(descs chan<- *prometheus.Desc) {
	c.ca.describeCostAttribution(descs)
}

func (c costAttributionCollector) Collect(metrics chan<- prometheus.Metric) {
	c.ca.collectCostAttribution(metrics)
}
