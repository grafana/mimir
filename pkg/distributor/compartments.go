// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"github.com/grafana/mimir/pkg/compartments"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/extract"
)

// compartmentTokens holds the partition-ring tokens for all series and metadata items of a write
// request that belong to a single read compartment.
type compartmentTokens struct {
	compartmentID int      // Read compartment ID; selects the per-compartment partition ring.
	topic         string   // Kafka topic for this compartment.
	indexes       []int    // Indexes into the combined series+metadata of the WriteRequest.
	tokens        []uint32 // Parallel to indexes: partition-ring tokens.
}

// writeRequestIndexes maps token-level indexes (as returned by the per-partition grouping) back to
// the original WriteRequest indexes.
func (ct *compartmentTokens) writeRequestIndexes(tokenIndexes []int) []int {
	result := make([]int, len(tokenIndexes))
	for i, ti := range tokenIndexes {
		result[i] = ct.indexes[ti]
	}
	return result
}

// getCompartmentTokensForWriteRequest groups the request's series and metadata by read compartment,
// computing the partition-ring token of each item. It is only called when compartments are enabled
// (router is never nil).
//
// It returns the grouped tokens and the index at which metadata starts in the combined series+metadata
// index space (i.e. len(req.Timeseries)).
func getCompartmentTokensForWriteRequest(router *compartments.Router, userID string, req *mimirpb.WriteRequest) ([]compartmentTokens, int) {
	initialMetadataIndex := len(req.Timeseries)

	numCompartments := router.NumCompartments()
	byCompartment := make([]compartmentTokens, numCompartments)
	for c := range byCompartment {
		byCompartment[c].compartmentID = c
		byCompartment[c].topic = router.TopicForCompartment(c)
	}

	for i, ts := range req.Timeseries {
		// UnsafeMetricNameFromLabelAdapters returns a reference into the pooled request buffer. It is
		// safe here because CompartmentForMetric only hashes the string and never retains it. A missing
		// __name__ yields an empty metric name, which deterministically maps to a compartment.
		metricName, _ := extract.UnsafeMetricNameFromLabelAdapters(ts.Labels)
		ct := &byCompartment[router.CompartmentForMetric(userID, metricName)]
		ct.indexes = append(ct.indexes, i)
		ct.tokens = append(ct.tokens, tokenForLabels(userID, ts.Labels))
	}

	for i, m := range req.Metadata {
		ct := &byCompartment[router.CompartmentForMetric(userID, m.MetricFamilyName)]
		ct.indexes = append(ct.indexes, initialMetadataIndex+i)
		ct.tokens = append(ct.tokens, tokenForMetadata(userID, m.MetricFamilyName))
	}

	// Drop empty compartments, keeping the original compartment IDs on the remaining entries.
	filtered := byCompartment[:0]
	for i := range byCompartment {
		if len(byCompartment[i].indexes) > 0 {
			filtered = append(filtered, byCompartment[i])
		}
	}
	return filtered, initialMetadataIndex
}
