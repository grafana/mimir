// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"sort"
	"testing"

	"github.com/grafana/mimir/pkg/ingester/client"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/require"
)

// expected 4 batches with 2 label value per batch except the last one. the last one must be a batch with 1 label value.
// Expected label<>value pairs:
// 0. label-a: [val-0, val-1]
// 1. label-a: [val-2] , label-b: [val-0]
// 2. label-b: [val-1, val-2]
// 3. label-b: [val-3]
func TestLabelNamesCardinalityReportSentInBatches(t *testing.T) {

	analyzer := CardinalityReporter{}
	existingLabels := map[string][]string{"label-a": {"val-0", "val-1", "val-2"},
		"label-b": {"val-0", "val-1", "val-2", "val-3"},
	}
	mockServer := MockLabelNamesCardinalityServer{context: context.Background()}
	var server client.Ingester_LabelNamesCardinalityServer = &mockServer
	require.NoError(t, analyzer.labelNamesCardinality(MockIndex{existingLabels: existingLabels}, []*labels.Matcher{}, 2, &server))

	require.Len(t, mockServer.SentResponses, 4)

	require.Equal(t, []*client.LabelNamesCardinality{
		{LabelName: "label-a", LabelValues: []string{"val-0", "val-1"}}},
		mockServer.SentResponses[0].Items)
	require.Equal(t, []*client.LabelNamesCardinality{
		{LabelName: "label-a", LabelValues: []string{"val-2"}},
		{LabelName: "label-b", LabelValues: []string{"val-0"}}},
		mockServer.SentResponses[1].Items)
	require.Equal(t, []*client.LabelNamesCardinality{
		{LabelName: "label-b", LabelValues: []string{"val-1", "val-2"}}},
		mockServer.SentResponses[2].Items)
	require.Equal(t, []*client.LabelNamesCardinality{
		{LabelName: "label-b", LabelValues: []string{"val-3"}}},
		mockServer.SentResponses[3].Items)
}

func TestExpectedAllValuesToBeReturnedInSingleMessage(t *testing.T) {
	for _, tc := range []struct {
		description     string
		existingLabels  map[string][]string
		expectedMessage []*client.LabelNamesCardinality
	}{
		{"all values returned in a single message even if only one label",
			map[string][]string{"label-a": {"val-0"}},
			[]*client.LabelNamesCardinality{{LabelName: "label-a", LabelValues: []string{"val-0"}}}},
		{"all values returned in a single message if label values count less then batch size",
			map[string][]string{"label-a": {"val-0", "val-1", "val-2"},
				"label-b": {"val-0", "val-1", "val-2", "val-3"}},
			[]*client.LabelNamesCardinality{
				{LabelName: "label-a", LabelValues: []string{"val-0", "val-1", "val-2"}},
				{LabelName: "label-b", LabelValues: []string{"val-0", "val-1", "val-2", "val-3"}}}},
	} {
		t.Run(tc.description, func(t *testing.T) {
			analyzer := CardinalityReporter{}
			mockServer := MockLabelNamesCardinalityServer{context: context.Background()}
			var server client.Ingester_LabelNamesCardinalityServer = &mockServer

			require.NoError(t, analyzer.labelNamesCardinality(MockIndex{existingLabels: tc.existingLabels}, []*labels.Matcher{}, 1000, &server))

			require.Len(t, mockServer.SentResponses, 1)
			require.Equal(t, tc.expectedMessage, mockServer.SentResponses[0].Items)
		})
	}
}

type MockIndex struct {
	tsdb.IndexReader
	existingLabels map[string][]string
}

func (i MockIndex) LabelNames(_ ...*labels.Matcher) ([]string, error) {
	var l []string
	for k := range i.existingLabels {
		l = append(l, k)
	}
	sort.Strings(l)
	return l, nil
}
func (i MockIndex) LabelValues(name string, _ ...*labels.Matcher) ([]string, error) {
	return i.existingLabels[name], nil
}

type MockLabelNamesCardinalityServer struct {
	client.Ingester_LabelNamesCardinalityServer
	SentResponses []client.LabelNamesCardinalityResponse
	context       context.Context
}

func (m *MockLabelNamesCardinalityServer) Send(response *client.LabelNamesCardinalityResponse) error {
	m.SentResponses = append(m.SentResponses, *response)
	return nil
}

func (m *MockLabelNamesCardinalityServer) Context() context.Context {
	return m.context
}
