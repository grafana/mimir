// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"slices"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/validation"
)

// Discarded metadata reasons of the limits on resource shape.
const (
	reasonResourceTooManyAttributes            = "resource_too_many_attributes"
	reasonResourceAttributeValueTooLong        = "resource_attribute_value_too_long"
	reasonResourceServiceAttributeValueTooLong = "resource_service_attribute_value_too_long"
)

// isServiceAttribute reports whether the attribute is one of the service attributes that identify a resource, and
// that job and instance derive from.
func isServiceAttribute(key string) bool {
	return key == "service.name" || key == "service.namespace" || key == "service.instance.id"
}

type resourceLimitMetrics struct {
	// Resources that had descriptive attributes dropped to fit the limit on attributes.
	tooManyAttributes *prometheus.CounterVec
	// Descriptive attributes dropped for the length of their value.
	valueTooLong *prometheus.CounterVec
	// Resources dropped for the length of a service attribute's value.
	serviceValueTooLong *prometheus.CounterVec
}

func newResourceLimitMetrics(r prometheus.Registerer) *resourceLimitMetrics {
	return &resourceLimitMetrics{
		tooManyAttributes:   validation.DiscardedMetadataCounter(r, reasonResourceTooManyAttributes),
		valueTooLong:        validation.DiscardedMetadataCounter(r, reasonResourceAttributeValueTooLong),
		serviceValueTooLong: validation.DiscardedMetadataCounter(r, reasonResourceServiceAttributeValueTooLong),
	}
}

func (m *resourceLimitMetrics) deleteUserMetrics(userID string) {
	m.tooManyAttributes.DeleteLabelValues(userID)
	m.valueTooLong.DeleteLabelValues(userID)
	m.serviceValueTooLong.DeleteLabelValues(userID)
}

// applyResourceLimits makes the request's resources fit the limits on their shape, with 0 for no limit:
//   - A descriptive attribute with a value longer than maxValueLength is dropped. A resource with such a service
//     attribute is dropped, so that its series carry no resource.
//   - Past maxAttributes, descriptive attributes are dropped in key order. Service attributes never are.
func applyResourceLimits(m *resourceLimitMetrics, userID string, req *mimirpb.WriteRequest, maxAttributes, maxValueLength int) {
	var dropped []bool // By resource ref.
	for i := range req.ResourceTable {
		entry := &req.ResourceTable[i]
		if maxValueLength > 0 {
			kept := entry.Attributes[:0]
			drop := false
			for _, a := range entry.Attributes {
				if len(a.StringValue) <= maxValueLength && len(a.BytesValue) <= maxValueLength {
					kept = append(kept, a)
					continue
				}
				if isServiceAttribute(a.Key) {
					drop = true
					break
				}
				m.valueTooLong.WithLabelValues(userID).Inc()
			}
			if drop {
				if dropped == nil {
					dropped = make([]bool, len(req.ResourceTable)+1)
				}
				dropped[i+1] = true
				m.serviceValueTooLong.WithLabelValues(userID).Inc()
				continue
			}
			entry.Attributes = kept
		}
		if maxAttributes > 0 && len(entry.Attributes) > maxAttributes {
			dropDescriptiveAttributes(entry, len(entry.Attributes)-maxAttributes)
			m.tooManyAttributes.WithLabelValues(userID).Inc()
		}
	}
	if dropped == nil {
		return
	}
	for _, ts := range req.Timeseries {
		if dropped[ts.ResourceRef] {
			ts.ResourceRef = 0
		}
	}
}

// dropDescriptiveAttributes drops up to n descriptive attributes, the first in key order, keeping the others' order.
func dropDescriptiveAttributes(entry *mimirpb.ResourceEntry, n int) {
	var descriptive []string
	for _, a := range entry.Attributes {
		if !isServiceAttribute(a.Key) {
			descriptive = append(descriptive, a.Key)
		}
	}
	slices.Sort(descriptive)
	descriptive = descriptive[:min(n, len(descriptive))]
	entry.Attributes = slices.DeleteFunc(entry.Attributes, func(a mimirpb.ResourceAttribute) bool {
		_, found := slices.BinarySearch(descriptive, a.Key)
		return found
	})
}
