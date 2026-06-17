// SPDX-License-Identifier: AGPL-3.0-only

package compartments

import (
	"slices"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/mimir/pkg/mimirpb"
)

// Router assigns a tenant's metric to a read compartment, and the corresponding Kafka topic, based
// on a hash of the user ID and metric name.
type Router struct {
	topics []string

	// allReadCompartmentIDs holds every read compartment ID.
	allReadCompartmentIDs []int
}

// NewRouter creates a Router. kafkaTopicTemplate is the Kafka topic name containing the
// ReadCompartmentIDPlaceholder, which is replaced with each read compartment ID to derive
// that compartment's topic.
func NewRouter(numReadCompartments int, kafkaTopicTemplate string) *Router {
	var (
		topics                = make([]string, numReadCompartments)
		allReadCompartmentIDs = make([]int, numReadCompartments)
	)

	for id := range topics {
		topics[id] = ReplaceReadCompartment(kafkaTopicTemplate, id)
		allReadCompartmentIDs[id] = id
	}

	return &Router{topics: topics, allReadCompartmentIDs: allReadCompartmentIDs}
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

// CompartmentsForMatchers returns the read compartments that may hold series matching the given matchers
// (series shard across compartments by metric name). It returns all compartments when it can't restrict
// the query.
//
// It restricts the query using __name__ matchers: an exact equality matcher maps to a single compartment,
// and a regexp whose match set is enumerable (for example "a|b") maps to the compartments of those names.
//
// It assumes a static compartment count: a changing count would shift the metric→compartment mapping
// and a targeted query could miss data until the rings settle.
func (r *Router) CompartmentsForMatchers(userID string, matchers []*labels.Matcher) []int {
	var (
		exactName  string
		setMatches []string
	)

	// Find the most restrictive enumerable set of candidate metric names from the __name__ matchers. An
	// exact matcher fully determines the name, so it always wins over a regexp's match set.
	for _, m := range matchers {
		if m.Name != model.MetricNameLabel {
			continue
		}

		switch m.Type {
		case labels.MatchEqual:
			// The first exact name fully determines the compartment. A different second exact name would
			// match no series, so whichever compartment we pick is safe (the query returns nothing anyway).
			if exactName == "" {
				exactName = m.Value
			}
		case labels.MatchRegexp:
			// SetMatches returns the finite set of names a regexp matches, or nil when it can't be reduced
			// to one (open-ended like ".+"); in the latter case we can't restrict and fall through to all
			// compartments. Among enumerable regexps, keep the smallest set.
			if sm := m.SetMatches(); len(sm) > 0 && (setMatches == nil || len(sm) < len(setMatches)) {
				setMatches = sm
			}
		}
	}

	if exactName != "" {
		return []int{r.CompartmentForMetric(userID, exactName)}
	}
	if len(setMatches) == 0 {
		// The query can't be restricted to a subset of compartments: query all of them.
		return slices.Clone(r.allReadCompartmentIDs)
	}

	var compartments []int
	for _, name := range setMatches {
		compartments = appendUnique(compartments, r.CompartmentForMetric(userID, name))
	}
	return compartments
}

// NumCompartments returns the number of read compartments.
func (r *Router) NumCompartments() int {
	return len(r.topics)
}

// TopicForCompartment returns the Kafka topic for the given read compartment ID.
func (r *Router) TopicForCompartment(compartmentID int) string {
	return r.topics[compartmentID]
}

// appendUnique appends v to s only if absent, deduplicating because several matched metric names can
// hash to the same compartment.
func appendUnique[T comparable](s []T, v T) []T {
	for _, existing := range s {
		if existing == v {
			return s
		}
	}
	return append(s, v)
}
