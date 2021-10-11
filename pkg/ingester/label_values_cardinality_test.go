// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ingester/ingester_v2.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ingester

import (
	"context"
	"sort"
	"testing"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/ingester/client"
)

func TestLabelValues_CardinalityReportSentInBatches(t *testing.T) {
	// set a 25 byte response size threshold
	labelValuesCardinalityTargetSizeBytes = 25

	existingLabels := map[string][]string{
		"label-a": {"a-0", "a-1", "a-2"},
		"label-b": {"b-0", "b-1", "b-2", "b-3"},
	}
	// server
	mockServer := &mockLabelValuesCardinalityServer{context: context.Background()}
	var server client.Ingester_LabelValuesCardinalityServer = mockServer

	// index reader
	idxReader := labelValuesCardinalityIndexReader{
		IndexReader: &mockIndex{existingLabels: existingLabels},
		PostingsForMatchers: func(reader tsdb.IndexPostingsReader, matcher ...*labels.Matcher) (index.Postings, error) {
			return &mockPostings{n: 100}, nil
		},
	}
	err := labelValuesCardinality(
		idxReader,
		[]string{"label-a", "label-b"},
		[]*labels.Matcher{},
		server,
	)
	require.NoError(t, err)

	require.Len(t, mockServer.SentResponses, 4)

	require.Equal(t, []*client.LabelValueCardinality{
		{LabelName: "label-a", LabelValue: "a-0", SeriesCount: 100},
		{LabelName: "label-a", LabelValue: "a-1", SeriesCount: 100},
	}, mockServer.SentResponses[0].Items)

	require.Equal(t, []*client.LabelValueCardinality{
		{LabelName: "label-a", LabelValue: "a-2", SeriesCount: 100},
		{LabelName: "label-b", LabelValue: "b-0", SeriesCount: 100},
	}, mockServer.SentResponses[1].Items)

	require.Equal(t, []*client.LabelValueCardinality{
		{LabelName: "label-b", LabelValue: "b-1", SeriesCount: 100},
		{LabelName: "label-b", LabelValue: "b-2", SeriesCount: 100},
	}, mockServer.SentResponses[2].Items)

	require.Equal(t, []*client.LabelValueCardinality{
		{LabelName: "label-b", LabelValue: "b-3", SeriesCount: 100},
	}, mockServer.SentResponses[3].Items)
}

func TestLabelValues_ExpectedAllValuesToBeReturnedInSingleMessage(t *testing.T) {
	labelValuesCardinalityTargetSizeBytes = 1000

	for _, tc := range []struct {
		description    string
		existingLabels map[string][]string
		expectedItems  []*client.LabelValueCardinality
	}{
		{
			"all values returned in a single message even if only one label",
			map[string][]string{
				"label-a": {"a-0"},
			},
			[]*client.LabelValueCardinality{
				{SeriesCount: 50, LabelName: "label-a", LabelValue: "a-0"},
			},
		},
		{
			"all values returned in a single message if label values count less then batch size",
			map[string][]string{
				"label-a": {"a-0", "a-1", "a-2"},
				"label-b": {"b-0", "b-1"},
			},
			[]*client.LabelValueCardinality{
				{SeriesCount: 50, LabelName: "label-a", LabelValue: "a-0"},
				{SeriesCount: 50, LabelName: "label-a", LabelValue: "a-1"},
				{SeriesCount: 50, LabelName: "label-a", LabelValue: "a-2"},
				{SeriesCount: 50, LabelName: "label-b", LabelValue: "b-0"},
				{SeriesCount: 50, LabelName: "label-b", LabelValue: "b-1"},
			},
		},
	} {
		t.Run(tc.description, func(t *testing.T) {
			// server
			mockServer := &mockLabelValuesCardinalityServer{context: context.Background()}
			var server client.Ingester_LabelValuesCardinalityServer = mockServer

			// index reader
			idxReader := labelValuesCardinalityIndexReader{
				IndexReader: &mockIndex{existingLabels: tc.existingLabels},
				PostingsForMatchers: func(reader tsdb.IndexPostingsReader, matcher ...*labels.Matcher) (index.Postings, error) {
					return &mockPostings{n: 50}, nil
				},
			}
			err := labelValuesCardinality(
				idxReader,
				[]string{"label-a", "label-b"},
				[]*labels.Matcher{},
				server,
			)
			require.NoError(t, err)
			if tc.expectedItems == nil {
				require.Empty(t, mockServer.SentResponses)
				return
			}
			require.Len(t, mockServer.SentResponses, 1)
			require.Equal(t, tc.expectedItems, mockServer.SentResponses[0].Items)
		})
	}
}

type mockPostings struct {
	index.Postings
	n int
}

func (m *mockPostings) Next() bool {
	if m.n == 0 {
		return false
	}
	m.n--
	return true
}
func (m *mockPostings) Err() error { return nil }

type mockIndex struct {
	tsdb.IndexReader
	existingLabels map[string][]string
}

func (i mockIndex) LabelNames(_ ...*labels.Matcher) ([]string, error) {
	var l []string
	for k := range i.existingLabels {
		l = append(l, k)
	}
	sort.Strings(l)
	return l, nil
}

func (i mockIndex) LabelValues(name string, _ ...*labels.Matcher) ([]string, error) {
	return i.existingLabels[name], nil
}

func (i mockIndex) Close() error { return nil }

type mockLabelValuesCardinalityServer struct {
	client.Ingester_LabelValuesCardinalityServer
	SentResponses []client.LabelValuesCardinalityResponse
	context       context.Context
}

func (m *mockLabelValuesCardinalityServer) Send(response *client.LabelValuesCardinalityResponse) error {
	items := make([]*client.LabelValueCardinality, len(response.Items))
	copy(items, response.Items)
	m.SentResponses = append(m.SentResponses, client.LabelValuesCardinalityResponse{
		Items: items,
	})
	return nil
}

func (m *mockLabelValuesCardinalityServer) Context() context.Context {
	return m.context
}
