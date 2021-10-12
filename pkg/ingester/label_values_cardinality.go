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

type labelNameValueKey struct {
	labelName  string
	labelValue string
}

func labelValuesCardinalityV2(
	idxReader labelValuesCardinalityIndexReader,
	lbNames []string,
	matchers []*labels.Matcher,
	srv client.Ingester_LabelValuesCardinalityServer,
) error {
	var resp client.LabelValuesCardinalityResponse

	respItems := make(map[labelNameValueKey]*client.LabelValueCardinality)
	respSz := 0

	for _, lbName := range lbNames {
		// Fetch all postings matching label names and matchers.
		labelValueMatchers := append(matchers, labels.MustNewMatcher(labels.MatchNotEqual, lbName, ""))

		p, err := idxReader.PostingsForMatchers(idxReader.IndexReader, labelValueMatchers...)
		if err != nil {
			return err
		}
		// Iterate through all postings building up the responses for all encountered values
		// for the requested label names.
		for p.Next() {

			lbValue, err := idxReader.LabelValueFor(p.At(), lbName)
			if err != nil {
				return err
			}
			// Update existing response item
			k := labelNameValueKey{labelName: lbName, labelValue: lbValue}

			if rItem := respItems[k]; rItem != nil {
				rItem.SeriesCount++
				continue
			}
			// If not existing, create a new response item and keep track of it.
			rItem := &client.LabelValueCardinality{
				LabelName:   lbName,
				LabelValue:  lbValue,
				SeriesCount: 1,
			}
			respItems[k] = rItem

			// Flush response items when reached message threshold.
			respSz += rItem.Size()
			if respSz >= labelValuesCardinalityTargetSizeBytes {
				if err := client.SendLabelValuesCardinalityResponse(srv, &resp); err != nil {
					return err
				}
				resp.Items = resp.Items[:0]
				respSz = 0
			}
		}
	}

	// Send response in case nothing has been previously sent or there are pending items.
	if len(respItems) > 0 {
		if err := client.SendLabelValuesCardinalityResponse(srv, &resp); err != nil {
			return err
		}
	}
	return nil
}

func labelValuesCardinality(
	idxReader labelValuesCardinalityIndexReader,
	lbNames []string,
	matchers []*labels.Matcher,
	server client.Ingester_LabelValuesCardinalityServer,
) error {
	resp := client.LabelValuesCardinalityResponse{}
	baseSize := resp.Size()
	respSize := baseSize

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
			}
		}
	}
	// Send response in case nothing has been previously sent or there are pending items.
	if len(resp.Items) > 0 {
		return client.SendLabelValuesCardinalityResponse(server, &resp)
	}
	return nil
}

func countLabelValueSeries(
	idxReader labelValuesCardinalityIndexReader,
	labelValueMatchers []*labels.Matcher,
) (uint64, error) {

	p, err := idxReader.PostingsForMatchers(idxReader.IndexReader, labelValueMatchers...)
	if err != nil {
		return 0, err
	}
	return countPostings(p)
}

func countPostings(
	p index.Postings,
) (uint64, error) {
	var count uint64

	for p.Next() {
		count++
	}
	if p.Err() != nil {
		return 0, p.Err()
	}
	return count, nil
}

func labelValuesCardinalityV3(
	idxReader labelValuesCardinalityIndexReader,
	lbNames []string,
	matchers []*labels.Matcher,
	srv client.Ingester_LabelValuesCardinalityServer,
) error {
	resp := client.LabelValuesCardinalityResponse{}
	baseSize := resp.Size()
	respSize := baseSize

	pMatchersRaw, err := idxReader.PostingsForMatchers(idxReader.IndexReader, matchers...)
	if err != nil {
		return err
	}

	pMatchers := index.NewPostingsCloner(pMatchersRaw)

	for _, lbName := range lbNames {
		// Obtain all values for current label name.
		lbValues, err := idxReader.LabelValues(lbName, matchers...)
		if err != nil {
			return err
		}
		// For each value count total number of series storing the result into cardinality response.
		for _, lbValue := range lbValues {
			p, err := idxReader.PostingsForMatchers(idxReader, labels.MustNewMatcher(labels.MatchEqual, lbName, lbValue))
			if err != nil {
			}

			// Get total series count applying label matchers.
			seriesCount, err := countPostings(index.Intersect(p, pMatchers.Clone()))
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
				if err := client.SendLabelValuesCardinalityResponse(srv, &resp); err != nil {
					return err
				}
				resp.Items = resp.Items[:0]
				respSize = baseSize
			}
		}
	}
	// Send response in case nothing has been previously sent or there are pending items.
	if len(resp.Items) > 0 {
		return client.SendLabelValuesCardinalityResponse(srv, &resp)
	}
	return nil
}
