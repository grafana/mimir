// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"bytes"
	"context"
	"encoding/json"
	"sort"
	"strings"
	"testing"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/index"
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
	mockServer := mockLabelNamesAndValuesServer{context: context.Background()}
	var server client.Ingester_LabelNamesAndValuesServer = &mockServer
	require.NoError(t, labelNamesAndValues(mockIndex{existingLabels: existingLabels}, []*labels.Matcher{}, 32, server))

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
			mockServer := mockLabelNamesAndValuesServer{context: context.Background()}
			var server client.Ingester_LabelNamesAndValuesServer = &mockServer

			require.NoError(t, labelNamesAndValues(mockIndex{existingLabels: tc.existingLabels}, []*labels.Matcher{}, 128, server))

			require.Len(t, mockServer.SentResponses, 1)
			require.Equal(t, tc.expectedMessage, mockServer.SentResponses[0].Items)
		})
	}
}

func TestLabelValues_CardinalityReportSentInBatches(t *testing.T) {
	existingLabels := map[string][]string{
		"lbl-a": {"a0000000", "a1111111", "a2222222"},
		"lbl-b": {"b0000000", "b1111111", "b2222222", "b3333333"},
		"lbl-c": {"c0000000"},
		"lbl-d": {"d0000000"},
		"lbl-e": {"e0000000"},
		"lbl-f": {"f0000000", "f1111111", "f2222222"},
		"lbl-g": {"g0000000"},
	}
	// server
	mockServer := &mockLabelValuesCardinalityServer{context: context.Background()}
	var server client.Ingester_LabelValuesCardinalityServer = mockServer

	// index reader
	idxReader := &mockIndex{existingLabels: existingLabels}
	postingsForMatchersFn := func(reader tsdb.IndexPostingsReader, matcher ...*labels.Matcher) (index.Postings, error) {
		return &mockPostings{n: 100}, nil
	}
	err := labelValuesCardinality(
		[]string{"lbl-a", "lbl-b", "lbl-c", "lbl-d", "lbl-e", "lbl-f", "lbl-g"},
		[]*labels.Matcher{},
		idxReader,
		postingsForMatchersFn,
		25,
		server,
	)
	require.NoError(t, err)

	require.Len(t, mockServer.SentResponses, 4)

	require.Equal(t, []*client.LabelValueSeriesCount{
		{
			LabelName: "lbl-a",
			LabelValueSeries: map[string]uint64{
				"a0000000": 100,
				"a1111111": 100,
				"a2222222": 100,
			},
		},
		{
			LabelName: "lbl-b",
			LabelValueSeries: map[string]uint64{
				"b0000000": 100,
			},
		},
	}, mockServer.SentResponses[0].Items)

	require.Equal(t, []*client.LabelValueSeriesCount{
		{
			LabelName: "lbl-b",
			LabelValueSeries: map[string]uint64{
				"b1111111": 100,
				"b2222222": 100,
				"b3333333": 100,
			},
		},
		{
			LabelName: "lbl-c",
			LabelValueSeries: map[string]uint64{
				"c0000000": 100,
			},
		},
	}, mockServer.SentResponses[1].Items)

	require.Equal(t, []*client.LabelValueSeriesCount{
		{
			LabelName: "lbl-d",
			LabelValueSeries: map[string]uint64{
				"d0000000": 100,
			},
		},
		{
			LabelName: "lbl-e",
			LabelValueSeries: map[string]uint64{
				"e0000000": 100,
			},
		},
		{
			LabelName: "lbl-f",
			LabelValueSeries: map[string]uint64{
				"f0000000": 100,
				"f1111111": 100,
			},
		},
	}, mockServer.SentResponses[2].Items)

	require.Equal(t, []*client.LabelValueSeriesCount{
		{
			LabelName: "lbl-f",
			LabelValueSeries: map[string]uint64{
				"f2222222": 100,
			},
		},
		{
			LabelName: "lbl-g",
			LabelValueSeries: map[string]uint64{
				"g0000000": 100,
			},
		},
	}, mockServer.SentResponses[3].Items)
}

func TestLabelValues_ExpectedAllValuesToBeReturnedInSingleMessage(t *testing.T) {
	testCases := map[string]struct {
		labels         []string
		matchers       []*labels.Matcher
		existingLabels map[string][]string
		expectedItems  []*client.LabelValueSeriesCount
	}{
		"empty response is returned when no labels are provided": {
			labels:         []string{"label-a", "label-b"},
			matchers:       []*labels.Matcher{},
			existingLabels: map[string][]string{},
			expectedItems:  nil,
		},
		"all values returned in a single message": {
			labels:   []string{"label-a", "label-b"},
			matchers: []*labels.Matcher{},
			existingLabels: map[string][]string{
				"label-a": {"a-0"},
			},
			expectedItems: []*client.LabelValueSeriesCount{
				{LabelName: "label-a", LabelValueSeries: map[string]uint64{"a-0": 50}},
			},
		},
		"all values returned in a single message if response size is less then batch size": {
			labels:   []string{"label-a", "label-b"},
			matchers: []*labels.Matcher{},
			existingLabels: map[string][]string{
				"label-a": {"a-0", "a-1", "a-2"},
				"label-b": {"b-0", "b-1"},
			},
			expectedItems: []*client.LabelValueSeriesCount{
				{
					LabelName:        "label-a",
					LabelValueSeries: map[string]uint64{"a-0": 50, "a-1": 50, "a-2": 50},
				},
				{
					LabelName:        "label-b",
					LabelValueSeries: map[string]uint64{"b-0": 50, "b-1": 50},
				},
			},
		},
	}
	for tName, tCfg := range testCases {
		t.Run(tName, func(t *testing.T) {
			// server
			mockServer := &mockLabelValuesCardinalityServer{context: context.Background()}
			var server client.Ingester_LabelValuesCardinalityServer = mockServer

			// index reader
			idxReader := &mockIndex{existingLabels: tCfg.existingLabels}
			postingsForMatchersFn := func(reader tsdb.IndexPostingsReader, matcher ...*labels.Matcher) (index.Postings, error) {
				return &mockPostings{n: 50}, nil
			}
			err := labelValuesCardinality(
				tCfg.labels,
				tCfg.matchers,
				idxReader,
				postingsForMatchersFn,
				1000,
				server,
			)
			require.NoError(t, err)
			if tCfg.expectedItems == nil {
				require.Empty(t, mockServer.SentResponses)
				return
			}
			require.Len(t, mockServer.SentResponses, 1)
			require.Equal(t, tCfg.expectedItems, mockServer.SentResponses[0].Items)
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

type mockLabelNamesAndValuesServer struct {
	client.Ingester_LabelNamesAndValuesServer
	SentResponses []client.LabelNamesAndValuesResponse
	context       context.Context
}

func (m *mockLabelNamesAndValuesServer) Send(response *client.LabelNamesAndValuesResponse) error {
	items := make([]*client.LabelValues, len(response.Items))
	for i, it := range response.Items {
		values := make([]string, len(it.Values))
		copy(values, it.Values)
		items[i] = &client.LabelValues{LabelName: it.LabelName, Values: values}
	}
	m.SentResponses = append(m.SentResponses, client.LabelNamesAndValuesResponse{Items: items})
	return nil
}

func (m *mockLabelNamesAndValuesServer) Context() context.Context {
	return m.context
}

type mockLabelValuesCardinalityServer struct {
	client.Ingester_LabelValuesCardinalityServer
	SentResponses []client.LabelValuesCardinalityResponse
	context       context.Context
}

func (m *mockLabelValuesCardinalityServer) Send(resp *client.LabelValuesCardinalityResponse) error {
	var sentResp client.LabelValuesCardinalityResponse
	b, err := json.Marshal(resp)
	if err != nil {
		return err
	}
	if err := json.NewDecoder(bytes.NewReader(b)).Decode(&sentResp); err != nil {
		return err
	}
	m.SentResponses = append(m.SentResponses, sentResp)
	return nil
}

func (m *mockLabelValuesCardinalityServer) Context() context.Context {
	return m.context
}
