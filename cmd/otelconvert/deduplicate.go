package main

import (
	"hash/fnv"
	"slices"

	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	metricsv1 "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
)

func deduplicate(md *metricsv1.MetricsData) {
	resourceHashes := make(map[uint64][]int)
	for i, rm := range md.ResourceMetrics {
		h := hashAttributes(rm.Resource.Attributes)
		resourceHashes[h] = append(resourceHashes[h], i)
	}

	for _, resourceIndices := range resourceHashes {
		if len(resourceIndices) == 1 {
			continue
		}

		firstRM := md.ResourceMetrics[resourceIndices[0]]

		for _, resourceIndex := range resourceIndices[1:] {
			rm := md.ResourceMetrics[resourceIndex]

			if !resourceAttributesEqual(rm.Resource, firstRM.Resource) {
				continue
			}

			if rm.Resource.DroppedAttributesCount != firstRM.Resource.DroppedAttributesCount {
				continue
			}

			if rm.SchemaUrl != firstRM.SchemaUrl {
				continue
			}

			// Resources match so we can merge their scopes.
			firstRM.ScopeMetrics = append(firstRM.ScopeMetrics, rm.ScopeMetrics...)
			rm.ScopeMetrics = nil
		}
	}

	md.ResourceMetrics = slices.DeleteFunc(md.ResourceMetrics, func(rm *metricsv1.ResourceMetrics) bool {
		// While we're looping through anyway, deduplicate the scope metrics under the rm.
		deduplicateResourceMetrics(rm)
		// Remove any that have no scope metrics.
		return len(rm.ScopeMetrics) == 0
	})
}

func deduplicateResourceMetrics(rm *metricsv1.ResourceMetrics) {
	scopeHashes := make(map[uint64][]int)
	for i, sm := range rm.ScopeMetrics {
		h := hashAttributes(sm.Scope.Attributes)
		scopeHashes[h] = append(scopeHashes[h], i)
	}

	for _, scopeIndices := range scopeHashes {
		if len(scopeIndices) == 1 {
			continue
		}

		firstSM := rm.ScopeMetrics[scopeIndices[0]]

		for _, scopeIndex := range scopeIndices[1:] {
			sm := rm.ScopeMetrics[scopeIndex]

			if sm.Scope.Name != firstSM.Scope.Name {
				continue
			}

			if sm.Scope.Version != firstSM.Scope.Version {
				continue
			}

			if sm.Scope.DroppedAttributesCount != firstSM.Scope.DroppedAttributesCount {
				continue
			}

			// Scopes match so we can merge them.
			firstSM.Metrics = append(firstSM.Metrics, sm.Metrics...)
			sm.Metrics = nil
		}
	}

	// Drop scopes that have no metrics.
	rm.ScopeMetrics = slices.DeleteFunc(rm.ScopeMetrics, func(sm *metricsv1.ScopeMetrics) bool {
		return len(sm.Metrics) == 0
	})
}

func hashAttributes(attrs []*commonv1.KeyValue) uint64 {
	h := fnv.New64()

	for _, attr := range attrs {
		_, _ = h.Write([]byte(attr.GetKey()))
		_, _ = h.Write([]byte(attr.GetValue().GetStringValue()))
	}

	return h.Sum64()
}

func resourceAttributesEqual(a, b *resourcev1.Resource) bool {
	if len(a.Attributes) != len(b.Attributes) {
		return false
	}

	aAttrs := make(map[string]string, len(a.Attributes))
	for _, attr := range a.Attributes {
		aAttrs[attr.Key] = attr.GetValue().GetStringValue()
	}

	for _, bAttr := range b.Attributes {
		val, ok := aAttrs[bAttr.Key]
		if !ok || val != bAttr.Value.GetStringValue() {
			return false
		}
	}

	return true
}
