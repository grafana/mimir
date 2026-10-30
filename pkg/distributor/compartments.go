// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"github.com/grafana/dskit/tenant"

	"github.com/grafana/mimir/pkg/compartments"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/usagetracker/usagetrackerclient"
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
func getCompartmentTokensForWriteRequest(router *compartments.TopicRouter, userID string, req *mimirpb.WriteRequest) ([]compartmentTokens, int) {
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

// groupSeriesHashesByReadCompartment groups seriesHashes by the read compartment of each series in
// timeseries. Each group is treated by the usage-tracker client as a separate user. The groups are
// ordered by ascending compartment ID and share one backing array for the hashes.
func groupSeriesHashesByReadCompartment(router *compartments.TopicRouter, userID string, metadata tenant.Metadata, timeseries []mimirpb.PreallocTimeseries, seriesHashes []uint64) []usagetrackerclient.UserSeries {
	numCompartments := router.NumCompartments()

	// Find the read compartment of each series. At first, offsets holds the number of series of each
	// compartment.
	offsets := make([]int, numCompartments)
	seriesCompartments := make([]int, len(timeseries))
	for i, ts := range timeseries {
		metricName, _ := extract.UnsafeMetricNameFromLabelAdapters(ts.Labels)
		compartment := router.CompartmentForMetric(userID, metricName)
		seriesCompartments[i] = compartment
		offsets[compartment]++
	}

	// Reserve one contiguous slice of the shared hashes array for each non-empty compartment, in
	// ascending compartment order. Then offsets[compartment] is the first free slot of that slice.
	hashes := make([]uint64, len(seriesHashes))
	seriesHashesByReadCompartment := make([]usagetrackerclient.UserSeries, 0, numCompartments)
	start := 0
	for compartment, count := range offsets {
		offsets[compartment] = start
		if count > 0 {
			end := start + count
			seriesHashesByReadCompartment = append(seriesHashesByReadCompartment, usagetrackerclient.UserSeries{
				UserID: metadata.WithTenant(compartments.TenantIDWithReadCompartment(userID, compartment)),
				Series: hashes[start:end:end],
			})
			start = end
		}
	}

	// Write each series hash into the next free slot of its compartment. Within a compartment the
	// hashes keep the order of the original request.
	for i, hash := range seriesHashes {
		compartment := seriesCompartments[i]
		hashes[offsets[compartment]] = hash
		offsets[compartment]++
	}

	return seriesHashesByReadCompartment
}
