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
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/grafana/mimir/pkg/costattribution/costattributionmodel"
	"github.com/grafana/mimir/pkg/util/validation"
)

const (
	trackerLabel            = "tracker"
	tenantLabel             = "tenant"
	reasonLabel             = "reason"
	missingValue            = "__missing__"
	overflowValue           = "__overflow__"
	activeSeriesTrackerType = "active-series"
	samplesTrackerType      = "samples"
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

	inactiveTimeout time.Duration

	sampleTrackers       *managerTrackers[*SampleTracker, *sampleTracker]
	activeSeriesTrackers *managerTrackers[*ActiveSeriesTracker, *activeSeriesTracker]
}

type managerTrackers[CT compositeTracker[IT], IT individualTracker] struct {
	sync.RWMutex
	composite  map[string]CT
	individual map[string]map[string]IT

	newComposite   func(trackers []IT, configHash uint64) CT
	newIndividual  func(userID, trackerName string, labels costattributionmodel.Labels, maxCardinality int, cooldown time.Duration, logger log.Logger) (IT, error)
	creationErrors *prometheus.CounterVec

	cardinalityDesc *descriptor
	overflowDesc    *descriptor
}

type compositeTracker[IT individualTracker] interface {
	getTrackers() []IT
	getConfigHash() uint64
}

type individualTracker interface {
	trackerName() string
	config() (labels costattributionmodel.Labels, maxCardinality int, cooldown time.Duration)
	cardinality() (cardinality int, overflown bool)
	collectCostAttribution(out chan<- prometheus.Metric)
	purge(now, deadline time.Time) int
}

func NewManager(cleanupInterval, inactiveTimeout time.Duration, logger log.Logger, limits *validation.Overrides, reg, costAttributionReg prometheus.Registerer) (*Manager, error) {
	allTrackerCreationErrors := promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_cost_attribution_tracker_creation_errors_total",
		Help: "The total number of errors creating cost attribution trackers for each user.",
	}, []string{"user", "tracker_type", "tracker_name"})

	m := &Manager{
		sampleTrackers: &managerTrackers[*SampleTracker, *sampleTracker]{
			composite:  make(map[string]*SampleTracker),
			individual: make(map[string]map[string]*sampleTracker),

			newComposite:   newSampleTrackerComposite,
			newIndividual:  newSampleTracker,
			creationErrors: allTrackerCreationErrors.MustCurryWith(prometheus.Labels{"tracker_type": samplesTrackerType}),
		},
		activeSeriesTrackers: &managerTrackers[*ActiveSeriesTracker, *activeSeriesTracker]{
			composite:  make(map[string]*ActiveSeriesTracker),
			individual: make(map[string]map[string]*activeSeriesTracker),

			newComposite:   newActiveSeriesTrackerComposite,
			newIndividual:  newActiveSeriesTracker,
			creationErrors: allTrackerCreationErrors.MustCurryWith(prometheus.Labels{"tracker_type": activeSeriesTrackerType}),
		},

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
	if m.sampleTrackers.cardinalityDesc, err = newDescriptor("cortex_cost_attribution_sample_tracker_cardinality",
		"The cardinality of a cost attribution sample tracker for each user.",
		[]string{"user", trackerLabel},
		nil); err != nil {
		return err
	}
	if m.sampleTrackers.overflowDesc, err = newDescriptor("cortex_cost_attribution_sample_tracker_overflown",
		"This metric is exported with value 1 when a sample tracker for a user is overflown. It's not exported otherwise.",
		[]string{"user", trackerLabel},
		nil); err != nil {
		return err
	}
	if m.activeSeriesTrackers.cardinalityDesc, err = newDescriptor("cortex_cost_attribution_active_series_tracker_cardinality",
		"The cardinality of a cost attribution active series tracker for each user.",
		[]string{"user", trackerLabel},
		nil); err != nil {
		return err
	}
	if m.activeSeriesTrackers.overflowDesc, err = newDescriptor("cortex_cost_attribution_active_series_tracker_overflown",
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
// merging the default tracker config with the additional trackers map.
func effectiveTrackerConfigs(limits *validation.Overrides, userID string) map[string]resolvedTrackerConfig {
	cfg := limits.CostAttributionConfig(userID)
	defaultMaxCardinality := cfg.MaxCardinality
	defaultCooldown := cfg.Cooldown

	result := make(map[string]resolvedTrackerConfig, len(cfg.AdditionalTrackers)+1)

	if len(cfg.Labels) > 0 {
		result[costattributionmodel.DefaultTrackerName] = resolvedTrackerConfig{
			labels:           cfg.Labels,
			maxCardinality:   defaultMaxCardinality,
			cooldownDuration: defaultCooldown,
		}
	}

	for name, tcfg := range cfg.AdditionalTrackers {
		if len(tcfg.Labels) == 0 {
			continue
		}
		result[name] = resolvedTrackerConfig{
			labels:           tcfg.Labels,
			maxCardinality:   defaultMaxCardinality,
			cooldownDuration: defaultCooldown,
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

func (m *Manager) ActiveSeriesTracker(userID string) *ActiveSeriesTracker {
	if m == nil {
		return nil
	}
	if at, ok := m.activeSeriesTrackers.get(userID, m.limits, m.logger, false); ok {
		return at
	}
	return nil
}

func (m *Manager) SampleTracker(userID string) *SampleTracker {
	if m == nil {
		return nil
	}
	if st, ok := m.sampleTrackers.get(userID, m.limits, m.logger, false); ok {
		return st
	}
	return nil
}

// get() will retrieve a tracker, rebuilding it if config has changed.
func (mt *managerTrackers[CT, IT]) get(userID string, limits *validation.Overrides, logger log.Logger, cleanupIfNoConfig bool) (CT, bool) {
	configHash, has := limits.CostAttributionConfigHash(userID)
	if !has {
		if cleanupIfNoConfig {
			// When purging, also check if this user _had_ trackers before.
			mt.Lock()
			delete(mt.composite, userID)
			delete(mt.individual, userID)
			mt.Unlock()
		}
		var zero CT
		return zero, false
	}

	mt.RLock()
	if tracker, ok := mt.composite[userID]; ok && tracker.getConfigHash() == configHash {
		mt.RUnlock()
		return tracker, true
	}
	mt.RUnlock()

	return mt.rebuild(userID, limits, logger)
}

// rebuild will rebuild the tracker for the userID, checking first if maybe a different call has rebuilt it.
func (mt *managerTrackers[CT, IT]) rebuild(userID string, limits *validation.Overrides, logger log.Logger) (CT, bool) {
	mt.Lock()
	defer mt.Unlock()

	configHash, has := limits.CostAttributionConfigHash(userID)
	if !has {
		delete(mt.composite, userID)
		delete(mt.individual, userID)
		var zero CT
		return zero, false
	}

	if tracker, ok := mt.composite[userID]; ok && tracker.getConfigHash() == configHash {
		return tracker, true
	}

	configs := effectiveTrackerConfigs(limits, userID)

	individualUserTrackers, ok := mt.individual[userID]
	if !ok {
		individualUserTrackers = make(map[string]IT, len(configs))
		mt.individual[userID] = individualUserTrackers
	}

	// Remove the ones that are no longer in the config.
	for name := range individualUserTrackers {
		if _, ok := configs[name]; !ok {
			delete(individualUserTrackers, name)
		}
	}

	// Add the missing ones.
	for name, cfg := range configs {
		cfgLabels := sortLabels(cfg.labels)
		if existing, ok := individualUserTrackers[name]; ok {
			lbls, maxCardinality, cooldown := existing.config()
			if maxCardinality == cfg.maxCardinality && cooldown == cfg.cooldownDuration && slices.Equal(lbls, cfgLabels) {
				continue
			}
		}
		tracker, err := mt.newIndividual(userID, name, cfgLabels, cfg.maxCardinality, cfg.cooldownDuration, logger)
		if err != nil {
			mt.creationErrors.With(prometheus.Labels{"user": userID, "tracker_name": name}).Inc()
			level.Warn(logger).Log("msg", "error creating cost attribution tracker, skipping it", "user", userID, "tracker_name", name, "error", err)
			delete(individualUserTrackers, name)
			continue
		}
		individualUserTrackers[name] = tracker
	}

	trackers := slices.Collect(maps.Values(individualUserTrackers))
	slices.SortFunc(trackers, func(a, b IT) int { return strings.Compare(a.trackerName(), b.trackerName()) })
	composite := mt.newComposite(trackers, configHash)
	mt.composite[userID] = composite
	return composite, true
}

func (m *Manager) Collect(out chan<- prometheus.Metric) {
	m.activeSeriesTrackers.collect(out)
	m.sampleTrackers.collect(out)
}

func (mt *managerTrackers[CT, IT]) collect(out chan<- prometheus.Metric) {
	mt.RLock()
	trackersByUserID := maps.Clone(mt.composite)
	mt.RUnlock()

	for userID, composite := range trackersByUserID {
		for _, tracker := range composite.getTrackers() {
			cardinality, overflown := tracker.cardinality()
			out <- mt.cardinalityDesc.gauge(float64(cardinality), userID, tracker.trackerName())
			if overflown {
				out <- mt.overflowDesc.gauge(1, userID, tracker.trackerName())
			}
		}
	}
}

func (m *Manager) Describe(chan<- *prometheus.Desc) {
	// Describe is not implemented because the metrics include dynamic labels. The Manager functions as an unchecked exporter.
	// For more details, refer to the documentation: https://pkg.go.dev/github.com/prometheus/client_golang/prometheus#hdr-Custom_Collectors_and_constant_Metrics
}

func (m *Manager) collectCostAttribution(out chan<- prometheus.Metric) {
	m.activeSeriesTrackers.collectCostAttribution(out)
	m.sampleTrackers.collectCostAttribution(out)
}

func (mt *managerTrackers[CT, IT]) collectCostAttribution(out chan<- prometheus.Metric) {
	mt.RLock()
	trackersByUserID := maps.Clone(mt.composite)
	mt.RUnlock()

	for _, composite := range trackersByUserID {
		for _, tracker := range composite.getTrackers() {
			tracker.collectCostAttribution(out)
		}
	}
}

func (m *Manager) describeCostAttribution(chan<- *prometheus.Desc) {
	// Describe is not implemented because the metrics include dynamic labels. The Manager functions as an unchecked exporter.
	// For more details, refer to the documentation: https://pkg.go.dev/github.com/prometheus/client_golang/prometheus#hdr-Custom_Collectors_and_constant_Metrics
}

func (m *Manager) purgeInactiveAttributionsUntil(now time.Time) {
	deadline := now.Add(-m.inactiveTimeout)
	m.activeSeriesTrackers.purge(now, deadline, m.limits, m.logger)
	m.sampleTrackers.purge(now, deadline, m.limits, m.logger)
}

func (mt *managerTrackers[CT, IT]) purge(now, deadline time.Time, limits *validation.Overrides, logger log.Logger) {
	mt.RLock()
	trackersByUserID := maps.Clone(mt.composite)
	mt.RUnlock()

	for userID := range trackersByUserID {
		composite, ok := mt.get(userID, limits, logger, true)
		if !ok {
			// get() might have deleted it if the user doesn't have configs anymore.
			continue
		}

		userCardinality := 0
		for _, tracker := range composite.getTrackers() {
			userCardinality += tracker.purge(now, deadline)
		}

		// This user doesn't have data anymore, remove it.
		if userCardinality == 0 {
			mt.Lock()
			delete(mt.composite, userID)
			delete(mt.individual, userID)
			mt.Unlock()
		}
	}
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
