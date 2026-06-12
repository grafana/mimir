// SPDX-License-Identifier: AGPL-3.0-only

package compartments

import (
	"strconv"
	"strings"

	"github.com/grafana/mimir/pkg/mimirpb"
)

// Router assigns a tenant's metric to a read compartment, and the corresponding Kafka topic, based
// on a hash of the user ID and metric name.
type Router struct {
	topics []string
}

// NewRouter creates a Router.
func NewRouter(cfg ReadConfig) *Router {
	topics := make([]string, cfg.NumCompartments)
	for id := range topics {
		topics[id] = strings.ReplaceAll(cfg.KafkaTopicFormat, CompartmentIDPlaceholder, strconv.Itoa(id))
	}
	return &Router{topics: topics}
}

// CompartmentForMetric returns the read compartment ID for the given tenant's metric name. The
// metricName is only hashed and never retained, so it is safe to pass an unsafe (pooled) string.
func (r *Router) CompartmentForMetric(userID, metricName string) int {
	hash := mimirpb.ShardByMetricName(userID, metricName)
	return int(hash % uint32(len(r.topics)))
}

// TopicForMetric returns the Kafka topic for the given tenant's metric name.
func (r *Router) TopicForMetric(userID, metricName string) string {
	return r.topics[r.CompartmentForMetric(userID, metricName)]
}

// NumCompartments returns the number of read compartments.
func (r *Router) NumCompartments() int {
	return len(r.topics)
}

// TopicForCompartment returns the Kafka topic for the given read compartment ID.
func (r *Router) TopicForCompartment(compartmentID int) string {
	return r.topics[compartmentID]
}
