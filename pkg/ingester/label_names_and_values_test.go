// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"sort"
	"strings"
	"testing"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/ingester/client"
)

// Scenario: each label name or label value is 8 bytes value. Except `label-c` label, its label name is 7 bytes in length.
//
// expected 7 messages:
// 0. {Items:[&LabelValues{LabelName:label-aa,Values:[a0000000 a1111111 a2222222],}]}
// This message size is 32 bytes. it must be sent because its size reached the threshold of 32 bytes.
// 1. {Items:[&LabelValues{LabelName:label-bb,Values:[b0000000 b1111111 b2222222],}]}
// This message size is 32 bytes. it must be sent because its size reached the threshold of 32 bytes.
// 2. {Items:[&LabelValues{LabelName:label-bb,Values:[b3333333],} &LabelValues{LabelName:label-c,Values:[c0000000],}]}
// This message size is 32 bytes. it must be sent because its size reached the threshold of 32 bytes.
// 3. {Items:[&LabelValues{LabelName:label-dd,Values:[d0000000],}]}
// This message size is 16 bytes. it must be sent even if it's not reached the threshold of 32 bytes, but adding the next label-name leads to passing threshold.
// 4. {Items:[&LabelValues{LabelName:strings.Repeat("label-ee", 10),Values:[e0000000],}]}
// This message size is 88 bytes, but anyway it must be sent.
// 5. {Items:[&LabelValues{LabelName:"label-ff",Values:[f0000000 f1111111 f2222222],}]}
// This message size is 32 bytes. it must be sent because its size reached the threshold of 32 bytes.
// 6. {Items:[&LabelValues{LabelName:"label-gg",Values:[g0000000],}]}
// This message size is 16 bytes. it must be sent even if it's not reached the threshold of 32 bytes, but it's the last message.
func TestLabelNamesAndValuesAreSentInBatches(t *testing.T) {

	existingLabels := map[string][]string{
		"label-aa":                     {"a0000000", "a1111111", "a2222222"},
		"label-bb":                     {"b0000000", "b1111111", "b2222222", "b3333333"},
		"label-c":                      {"c0000000"},
		"label-dd":                     {"d0000000"},
		strings.Repeat("label-ee", 10): {"e0000000"},
		"label-ff":                     {"f0000000", "f1111111", "f2222222"},
		"label-gg":                     {"g0000000"},
	}
	mockServer := MockLabelNamesAndValuesServer{context: context.Background()}
	var server client.Ingester_LabelNamesAndValuesServer = &mockServer
	require.NoError(t, labelNamesAndValues(MockIndex{existingLabels: existingLabels}, []*labels.Matcher{}, 32, server))

	require.Len(t, mockServer.SentResponses, 7)

	require.Equal(t, []*client.LabelValues{
		{LabelName: "label-aa", Values: []string{"a0000000", "a1111111", "a2222222"}}},
		mockServer.SentResponses[0].Items)
	require.Equal(t, []*client.LabelValues{
		{LabelName: "label-bb", Values: []string{"b0000000", "b1111111", "b2222222"}}},
		mockServer.SentResponses[1].Items)
	require.Equal(t, []*client.LabelValues{
		{LabelName: "label-bb", Values: []string{"b3333333"}}, {LabelName: "label-c", Values: []string{"c0000000"}}},
		mockServer.SentResponses[2].Items)
	require.Equal(t, []*client.LabelValues{
		{LabelName: "label-dd", Values: []string{"d0000000"}}},
		mockServer.SentResponses[3].Items)
	require.Equal(t, []*client.LabelValues{
		{LabelName: strings.Repeat("label-ee", 10), Values: []string{"e0000000"}}},
		mockServer.SentResponses[4].Items)
	require.Equal(t, []*client.LabelValues{
		{LabelName: "label-ff", Values: []string{"f0000000", "f1111111", "f2222222"}}},
		mockServer.SentResponses[5].Items)
	require.Equal(t, []*client.LabelValues{
		{LabelName: "label-gg", Values: []string{"g0000000"}}},
		mockServer.SentResponses[6].Items)
}

func TestExpectedAllLabelNamesAndValuesToBeReturnedInSingleMessage(t *testing.T) {
	for _, tc := range []struct {
		description     string
		existingLabels  map[string][]string
		expectedMessage []*client.LabelValues
	}{
		{
			"all values returned in a single message even if only one label",
			map[string][]string{"label-a": {"val-0"}},
			[]*client.LabelValues{
				{LabelName: "label-a", Values: []string{"val-0"}},
			},
		},
		{
			"all values returned in a single message if label values count less then batch size",
			map[string][]string{
				"label-a": {"val-0", "val-1", "val-2"},
				"label-b": {"val-0", "val-1", "val-2", "val-3"},
			},
			[]*client.LabelValues{
				{LabelName: "label-a", Values: []string{"val-0", "val-1", "val-2"}},
				{LabelName: "label-b", Values: []string{"val-0", "val-1", "val-2", "val-3"}},
			},
		},
	} {
		t.Run(tc.description, func(t *testing.T) {
			mockServer := MockLabelNamesAndValuesServer{context: context.Background()}
			var server client.Ingester_LabelNamesAndValuesServer = &mockServer

			require.NoError(t, labelNamesAndValues(MockIndex{existingLabels: tc.existingLabels}, []*labels.Matcher{}, 128, server))

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

type MockLabelNamesAndValuesServer struct {
	client.Ingester_LabelNamesAndValuesServer
	SentResponses []client.LabelNamesAndValuesResponse
	context       context.Context
}

func (m *MockLabelNamesAndValuesServer) Send(response *client.LabelNamesAndValuesResponse) error {
	items := make([]*client.LabelValues, len(response.Items))
	for i, it := range response.Items {
		values := make([]string, len(it.Values))
		copy(values, it.Values)
		items[i] = &client.LabelValues{LabelName: it.LabelName, Values: values}
	}
	m.SentResponses = append(m.SentResponses, client.LabelNamesAndValuesResponse{Items: items})
	return nil
}

func (m *MockLabelNamesAndValuesServer) Context() context.Context {
	return m.context
}
