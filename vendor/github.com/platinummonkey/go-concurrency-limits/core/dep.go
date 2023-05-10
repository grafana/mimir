package core

import (
	"strings"
)

const (
	// MetricLimit is the name of the metric for current limit
	MetricLimit = "limit"
	// MetricDropped is the name of the metric for dropped counts
	MetricDropped = "dropped"
	// MetricInFlight is the name of the metric for current in flight count
	MetricInFlight = "inflight"
	// MetricPartitionLimit is the name of the metric for a current partition's limit
	MetricPartitionLimit = "limit.partition"
	// MetricRTT is the name of the metric for the sample Round Trip Time distribution
	MetricRTT = "rtt"
	// MetricMinRTT is the name of the metric for the Minimum Round Trip Time
	MetricMinRTT = "min_rtt"
	// MetricWindowMinRTT is the name of the metric for the Window's Minimum Round Trip Time
	MetricWindowMinRTT = "window.min_rtt"
	// MetricWindowQueueSize represents the name of the metric for the Window's Queue Size
	MetricWindowQueueSize = "window.queue_size"
	// MetricQueueSize represents the name of the metric for the size of a lifo queue
	MetricQueueSize = "queue_size"
	// MetricQueueLimit represents the name of the metric for the max size of a lifo queue
	MetricQueueLimit = "queue_limit"
)

// PrefixMetricWithName will prefix a given name with the metric name in the form "<name>.<metric>"
func PrefixMetricWithName(metric, name string) string {
	if name == "" {
		name = "default"
	}

	if strings.HasSuffix(name, ".") {
		return name + metric
	}
	return name + "." + metric
}
