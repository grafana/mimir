// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ingester/ingester_v2.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ingester

import (
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/index"

	"github.com/grafana/mimir/pkg/ingester/client"
)

// labelValuesCardinalityTargetSizeBytes is the maximum allowed size in bytes for label cardinality response.
// We arbitrarily set it to 1mb to avoid reaching the actual gRPC default limit (4mb).
var labelValuesCardinalityTargetSizeBytes = 1 * 1024 * 1024

type labelValuesCardinalityIndexReader struct {
	tsdb.IndexReader
	PostingsForMatchers func(tsdb.IndexPostingsReader, ...*labels.Matcher) (index.Postings, error)
}

func labelValuesCardinality(
	seriesCountTotal uint64,
	idxReader labelValuesCardinalityIndexReader,
	lbNames []string,
	matchers []*labels.Matcher,
	server client.Ingester_LabelValuesCardinalityServer,
) error {
	resp := client.LabelValuesCardinalityResponse{
		SeriesCountTotal: seriesCountTotal,
	}
	baseSize := resp.Size()
	respSize := baseSize
	sentResp := false

	for _, lbName := range lbNames {
		// Obtain all values for current label name.
		lbValues, err := idxReader.LabelValues(lbName, matchers...)
		if err != nil {
			return err
		}
		// For each value count total number of series storing the result into cardinality response.
		for _, lbValue := range lbValues {
			lbValueMatchers := append(matchers, labels.MustNewMatcher(labels.MatchEqual, lbName, lbValue))

			// Get total series count applying label matchers.
			seriesCount, err := countLabelValueSeries(idxReader, lbValueMatchers)
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
				sentResp = true
			}
		}
	}
	// Send response in case nothing has been previously sent or there are pending items.
	if !sentResp || len(resp.Items) > 0 {
		return client.SendLabelValuesCardinalityResponse(server, &resp)
	}
	return nil
}

func countLabelValueSeries(
	idxReader labelValuesCardinalityIndexReader,
	labelValueMatchers []*labels.Matcher,
) (uint64, error) {
	var count uint64

	p, err := idxReader.PostingsForMatchers(idxReader.IndexReader, labelValueMatchers...)
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
