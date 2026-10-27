// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/grafana/mimir/pkg/mimirpb"
)

// The series' OTel resources, as OTLP sends them, which records carry in their resource tables with native resources
// on. A tenant's metrics with resources map their targets over the tenant's resources, so that a resource has series
// of many metrics, as a service's instance does, and each resource has a target_info series, as OTLP's converter
// writes it. A resource's series are written together, at the resource's time in each of their intervals, as an SDK
// exports a resource's metrics, and without stale markers, which no scraper writes for OTLP.

// ResourceProfile is the profile of the series' resources.
type ResourceProfile struct {
	// The share of a tenant's metrics whose series have resources, and how many series each resource has, over all of
	// the tenant's metrics.
	Share             float64 `json:"share"`
	SeriesPerResource float64 `json:"series_per_resource"`
	// A resource's descriptive attributes, besides service.name, service.namespace and service.instance.id, the
	// length of their values, and the share of them that are ints rather than strings. The names come from a pool of
	// AttributeNames, the first ones the most common.
	DescriptiveAttributes Percentiles `json:"descriptive_attributes"`
	ValueLength           Percentiles `json:"value_length"`
	IntShare              float64     `json:"int_share"`
	AttributeNames        int         `json:"attribute_names"`
	// Services per tenant, which resources are instances of.
	Services int `json:"services"`
	// The share of resources replaced every hour, with a new service.instance.id, as a pod is: their series are
	// replaced with them, instead of on their own. And the share of resources whose descriptive attributes change
	// every hour, which makes a new version of each.
	IdentityChurnPerHour    float64 `json:"identity_churn_per_hour"`
	DescriptiveChurnPerHour float64 `json:"descriptive_churn_per_hour"`
	// How many times an hour a churning resource's description changes: once without it.
	DescriptiveVersionsPerHour float64 `json:"descriptive_versions_per_hour,omitempty"`
	// Whether a service's resources share their descriptions, as replicas whose attributes are all alike do, instead of
	// each having its own.
	SharedDescriptions bool `json:"shared_descriptions,omitempty"`
	// How often each resource's target_info series is written, in seconds: as an SDK exports.
	TargetInfoIntervalSeconds int `json:"target_info_interval_seconds"`
}

// tenantResources are a tenant's resources.
type tenantResources struct {
	count int
	// How long an identity lives.
	identityLifetimeMs int64
	// The tenant's target_info metric, whose slots are the resources.
	targetInfo *metricModel
	// Each resource's offset into its identities' lifetimes, the offset into the period of its description's versions,
	// or -1 for one whose description doesn't change, and the second of an hour and the millisecond it's written at.
	identityPhaseMs, descriptionPhaseMs []int64
	// How long a churning description lasts.
	descriptionPeriodMs int64
	exportSecond        []int32
	exportMs            []int16
	// The resources written in each second of an interval, by the interval in seconds.
	bySecond map[int64][][]int32
}

const (
	saltResourceMetric    = 0x7265736f_6d657472
	saltDescriptiveInt    = 0x7265736f_696e7473
	saltDescriptiveLength = 0x7265736f_6c656e67
)

// newResources gives the tenant resources for its metrics that have them, and a target_info metric.
func (w *world) newResources(t *tenantModel) {
	p := w.profile.Resources
	if p == nil || p.Share <= 0 {
		return
	}
	series := 0
	var withResources []*metricModel
	for _, m := range t.metrics {
		if unitOf(m.hash64(math.MinInt, saltResourceMetric)) < p.Share {
			withResources = append(withResources, m)
			series += m.slots * m.seriesPerSlot
		}
	}
	if series == 0 {
		return
	}
	count := max(1, int(math.Round(float64(series)/math.Max(1, p.SeriesPerResource))))
	r := &tenantResources{
		count:               count,
		identityLifetimeMs:  lifetimeMs(p.IdentityChurnPerHour),
		descriptionPeriodMs: max(1, int64(3_600_000/math.Max(1, p.DescriptiveVersionsPerHour))),
		identityPhaseMs:     make([]int64, count),
		descriptionPhaseMs:  make([]int64, count),
		exportSecond:        make([]int32, count),
		exportMs:            make([]int16, count),
		bySecond:            map[int64][][]int32{},
	}
	for resource := range count {
		key := strconv.Itoa(resource)
		r.identityPhaseMs[resource] = int64(seededHash(w.seed, t.id, key, "identity") % uint64(r.identityLifetimeMs))
		// A shared description churns as its service's.
		description := w.descriptionKey(resource)
		r.descriptionPhaseMs[resource] = -1
		if unitOf(seededHash(w.seed, t.id, description, "churn")) < p.DescriptiveChurnPerHour {
			r.descriptionPhaseMs[resource] = int64(seededHash(w.seed, t.id, description, "description") % uint64(r.descriptionPeriodMs))
		}
		export := seededHash(w.seed, t.id, key, "export")
		r.exportSecond[resource], r.exportMs[resource] = int32(export%3600), int16((export>>32)%1000)
	}
	interval := int64(max(1, p.TargetInfoIntervalSeconds)) * 1000
	r.targetInfo = &metricModel{tenant: t, name: "target_info", kind: kindConstantGauge, seriesPerSlot: 1, slotsPerTarget: 1, slots: count, intervalMs: interval, scale: 10, targetInfo: true, resources: true}
	r.targetInfo.base = seededHash(w.seed, t.id, r.targetInfo.name)
	for _, m := range append(withResources, r.targetInfo) {
		if m != r.targetInfo {
			m.resources = true
			// Where the metric's targets start among the resources, so that each resource has series of some metrics.
			m.resourceOffset = int(m.base % uint64(count))
		}
		seconds := m.intervalMs / 1000
		if _, ok := r.bySecond[seconds]; ok {
			continue
		}
		buckets := make([][]int32, seconds)
		for resource := range count {
			second := int64(r.exportSecond[resource]) % seconds
			buckets[second] = append(buckets[second], int32(resource))
		}
		r.bySecond[seconds] = buckets
	}
	t.resources = r
}

// lifetimeMs is how long something replaced at perHour of its kind each hour lives: forever without churn.
func lifetimeMs(perHour float64) int64 {
	if perHour <= 0 {
		return math.MaxInt64 / 4
	}
	return int64(math.Max(1, 1/perHour) * 3_600_000)
}

// resourceOf is the resource of the slot of a metric with resources: its target's, over the tenant's resources.
func (m *metricModel) resourceOf(slot int) int {
	return (m.target(slot) + m.resourceOffset) % m.tenant.resources.count
}

// resourceSlotsAt calls f for the slots of the metric with resources written in the second [s, s+1000), at their
// resources' times, a resource's one after another.
func (m *metricModel) resourceSlotsAt(s int64, f func(slot int, t int64)) {
	r := m.tenant.resources
	seconds := m.intervalMs / 1000
	targets := (m.slots + m.slotsPerTarget - 1) / m.slotsPerTarget
	for _, resource := range r.bySecond[seconds][(s/1000)%seconds] {
		t := s - s%1000 + int64(r.exportMs[resource])
		for target := ((int(resource)-m.resourceOffset)%r.count + r.count) % r.count; target < targets; target += r.count {
			for slot := target * m.slotsPerTarget; slot < min((target+1)*m.slotsPerTarget, m.slots); slot++ {
				f(slot, t)
			}
		}
	}
}

// resourceGeneration is the resource's identity's generation at t, which its series' generations follow.
func (w *world) resourceGeneration(t *tenantModel, resource int, at int64) int64 {
	r := t.resources
	return (at + r.identityPhaseMs[resource]) / r.identityLifetimeMs
}

func (w *world) resourceGenerationStart(t *tenantModel, resource int, generation int64) int64 {
	r := t.resources
	return generation*r.identityLifetimeMs - r.identityPhaseMs[resource]
}

// descriptionVersion is the version of the resource's descriptive attributes at t: one for each period, an hour by
// default, for the profile's share of resources whose attributes churn, from an offset of the resource's, and 0 for the
// others.
func (w *world) descriptionVersion(t *tenantModel, resource int, at int64) int64 {
	phase := t.resources.descriptionPhaseMs[resource]
	if phase < 0 {
		return 0
	}
	return (at + phase) / t.resources.descriptionPeriodMs
}

// descriptionKey and descriptionIndex are what the resource's descriptive attributes are made from: the resource, or
// with shared descriptions, its service.
func (w *world) descriptionKey(resource int) string {
	if w.profile.Resources.SharedDescriptions {
		return "service-" + strconv.Itoa(w.descriptionIndex(resource))
	}
	return strconv.Itoa(resource)
}

func (w *world) descriptionIndex(resource int) int {
	if p := w.profile.Resources; p.SharedDescriptions {
		return resource % max(1, p.Services)
	}
	return resource
}

// resourceEntry is the resource's attributes at t, in a fixed order: the identifying ones, then the descriptive ones.
func (w *world) resourceEntry(t *tenantModel, resource int, at int64) mimirpb.ResourceEntry {
	p := w.profile.Resources
	key := strconv.Itoa(resource)
	generation, version := w.resourceGeneration(t, resource, at), w.descriptionVersion(t, resource, at)
	services := max(1, p.Services)
	attributes := []mimirpb.ResourceAttribute{
		{Key: "service.name", StringValue: "service-" + strconv.Itoa(resource%services)},
		{Key: "service.namespace", StringValue: t.id},
		{Key: "service.instance.id", StringValue: key + "-" + strconv.FormatInt(generation, 36)},
	}
	description := w.descriptionKey(resource)
	count := max(0, int(math.Round(p.DescriptiveAttributes.Quantile(unitOf(seededHash(w.seed, t.id, description, "count"))))))
	for i := range count {
		name := "attribute." + strconv.Itoa(i%max(1, p.AttributeNames))
		if i >= max(1, p.AttributeNames) {
			name += "." + strconv.Itoa(i)
		}
		h := seededHash(w.seed, t.id, description, name)
		if unitOf(h^saltDescriptiveInt) < p.IntShare {
			attributes = append(attributes, mimirpb.ResourceAttribute{Key: name, Type: mimirpb.RESOURCE_ATTRIBUTE_INT, IntValue: int64(h % 1_000_000)})
			continue
		}
		length := max(1, int(math.Round(p.ValueLength.Quantile(unitOf(h^saltDescriptiveLength)))))
		value := valueString(w.seed, name, w.descriptionIndex(resource), length)
		if i == 0 {
			// The first carries the description's version, so that it changes with it.
			value += "-" + strconv.FormatInt(version, 36)
		}
		attributes = append(attributes, mimirpb.ResourceAttribute{Key: name, StringValue: value})
	}
	return mimirpb.ResourceEntry{Attributes: attributes}
}

// targetInfoLabels are the labels of the resource's target_info series at t, as OTLP's converter writes them: job and
// instance from the service attributes, and every other attribute as a label.
func (w *world) targetInfoLabels(t *tenantModel, resource int, at int64) []mimirpb.LabelAdapter {
	entry := w.resourceEntry(t, resource, at)
	labels := []mimirpb.LabelAdapter{
		{Name: "__name__", Value: "target_info"},
		{Name: "job", Value: entry.Attributes[1].StringValue + "/" + entry.Attributes[0].StringValue},
		{Name: "instance", Value: entry.Attributes[2].StringValue},
	}
	for _, a := range entry.Attributes[3:] {
		value := a.StringValue
		if a.Type == mimirpb.RESOURCE_ATTRIBUTE_INT {
			value = strconv.FormatInt(a.IntValue, 10)
		}
		labels = append(labels, mimirpb.LabelAdapter{Name: strings.ReplaceAll(a.Key, ".", "_"), Value: value})
	}
	sort.Slice(labels, func(a, b int) bool { return labels[a].Name < labels[b].Name })
	return labels
}
