// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ingester/ingester_v2.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ingester

import (
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/tsdb"

	"github.com/grafana/mimir/pkg/ingester/client"
)

var labelValuesCardinalityTargetSizeBytes = 1 * 1024 * 1024

func labelValuesCardinality(
	seriesCountTotal uint64,
	index tsdb.IndexReader,
	lbNames []string,
	matchers []*labels.Matcher,
	server client.Ingester_LabelValuesCardinalityServer,
) error {
	resp := client.LabelValuesCardinalityResponse{
		SeriesCountTotal: seriesCountTotal,
	}
	baseSize := resp.Size()
	respSize := baseSize

	for _, lbName := range lbNames {
		// Obtain all values for current label name.
		lbValues, err := index.LabelValues(lbName, matchers...)
		if err != nil {
			return err
		}
		// For each value count total number of series storing the result into cardinality response.
		for _, lbValue := range lbValues {
			lbValueMatchers := append(matchers, labels.MustNewMatcher(labels.MatchEqual, lbName, lbValue))

			// Get total series count applying label matchers.
			seriesCount, err := countLabelValueSeries(index, lbValueMatchers)
			if err != nil {
				return err
			}
			item := &client.LabelValueCardinality{
				LabelName:   lbName,
				LabelValue:  lbValue,
				SeriesCount: seriesCount,
			}
			resp.Items = append(resp.Items, item)

			// Flush the response when reached message threshold.
			respSize += item.Size()
			if respSize >= labelValuesCardinalityTargetSizeBytes {
				if err := client.SendLabelValuesCardinalityResponse(server, &resp); err != nil {
					return err
				}
				resp.Items = resp.Items[:0]
				respSize = baseSize
			}
		}
	}
	// Send remaining items.
	if len(resp.Items) > 0 {
		return client.SendLabelValuesCardinalityResponse(server, &resp)
	}
	return nil
}

var postingForMatchersFn = tsdb.PostingsForMatchers

func countLabelValueSeries(
	index tsdb.IndexReader,
	labelValueMatchers []*labels.Matcher,
) (uint64, error) {
	var count uint64

	p, err := postingForMatchersFn(index, labelValueMatchers...)
	if err != nil {
		return 0, err
	}
	for p.Next() {
		count++
	}
	if p.Err() != nil {
		return 0, p.Err()
	}
	return count, nil
}
