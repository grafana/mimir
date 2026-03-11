// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/ingest"
	"github.com/grafana/mimir/pkg/util/extract"
)

// compartmentTokens holds the partition-ring tokens for all series/metadata
// items that belong to a single compartment.
type compartmentTokens struct {
	topic   string   // Kafka topic for this compartment.
	indexes []int    // Indexes into the combined series+metadata of the WriteRequest.
	tokens  []uint32 // Parallel to indexes: partition-ring tokens.
}

// writeRequestIndexes maps token-level indexes (as returned by ring.DoBatchWithOptions callback)
// back to the original WriteRequest indexes.
func (ct *compartmentTokens) writeRequestIndexes(tokenIndexes []int) []int {
	result := make([]int, len(tokenIndexes))
	for i, ti := range tokenIndexes {
		result[i] = ct.indexes[ti]
	}
	return result
}

// getCompartmentTokensForWriteRequest groups series and metadata by compartment, computing
// partition-ring tokens for each item. When the router is nil (compartments disabled),
// it returns a single compartmentTokens entry with the default topic.
func getCompartmentTokensForWriteRequest(
	router *ingest.CompartmentRouter,
	defaultTopic string,
	userID string,
	req *mimirpb.WriteRequest,
) ([]compartmentTokens, int) {
	// When compartments are disabled, return a single entry with the default topic.
	if router == nil {
		tokens, initialMetadataIndex := getSeriesAndMetadataTokens(userID, req)

		indexes := make([]int, len(tokens))
		for i := range indexes {
			indexes[i] = i
		}

		return []compartmentTokens{{
			topic:   defaultTopic,
			indexes: indexes,
			tokens:  tokens,
		}}, initialMetadataIndex
	}

	initialMetadataIndex := len(req.Timeseries)

	// With compartments: compute compartment + token for each item, then group.
	numCompartments := router.NumCompartments()
	byCompartment := make([]compartmentTokens, numCompartments)
	for c := 0; c < numCompartments; c++ {
		byCompartment[c].topic = router.Topic(c)
	}

	for i, ts := range req.Timeseries {
		// UnsafeMetricNameFromLabelAdapters returns a reference into the pooled request buffer.
		// This is safe because CompartmentForMetric only hashes the string and does not retain it.
		// If __name__ is missing, metricName is empty and the series gets a deterministic compartment
		// based on userID alone.
		metricName, _ := extract.UnsafeMetricNameFromLabelAdapters(ts.Labels)
		compartmentIdx := router.CompartmentForMetric(userID, string(metricName))
		token := tokenForLabels(userID, ts.Labels)
		ct := &byCompartment[compartmentIdx]
		ct.indexes = append(ct.indexes, i)
		ct.tokens = append(ct.tokens, token)
	}

	for i, m := range req.Metadata {
		compartmentIdx := router.CompartmentForMetric(userID, m.MetricFamilyName)
		token := tokenForMetadata(userID, m.MetricFamilyName)
		ct := &byCompartment[compartmentIdx]
		ct.indexes = append(ct.indexes, initialMetadataIndex+i)
		ct.tokens = append(ct.tokens, token)
	}

	// Filter out empty compartments in place.
	n := 0
	for i := range byCompartment {
		if len(byCompartment[i].indexes) > 0 {
			byCompartment[n] = byCompartment[i]
			n++
		}
	}

	return byCompartment[:n], initialMetadataIndex
}
