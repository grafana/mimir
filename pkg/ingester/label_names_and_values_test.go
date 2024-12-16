// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/grafana/dskit/user"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
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
	var stream client.Ingester_LabelNamesAndValuesServer = &mockServer
	var valueFilter = func(string, string) (bool, error) {
		return true, nil
	}
	require.NoError(t, labelNamesAndValues(&mockIndex{existingLabels: existingLabels}, []*labels.Matcher{}, 32, stream, valueFilter))

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

func TestLabelNamesAndValues_FilteredValues(t *testing.T) {

	existingLabels := map[string][]string{
		"label-aa": {"a0000000", "a1111111", "a2222222"},
		"label-bb": {"b0000000", "b1111111", "b2222222", "b3333333"},
		"label-cc": {"c0000000"},
	}
	mockServer := mockLabelNamesAndValuesServer{context: context.Background()}
	var stream client.Ingester_LabelNamesAndValuesServer = &mockServer
	var valueFilter = func(_, value string) (bool, error) {
		return strings.Contains(value, "0"), nil
	}
	require.NoError(t, labelNamesAndValues(&mockIndex{existingLabels: existingLabels}, []*labels.Matcher{}, 32, stream, valueFilter))

	require.Len(t, mockServer.SentResponses, 2)

	require.Equal(t, []*client.LabelValues{
		{LabelName: "label-aa", Values: []string{"a0000000"}},
		{LabelName: "label-bb", Values: []string{"b0000000"}},
	}, mockServer.SentResponses[0].Items)
	require.Equal(t, []*client.LabelValues{
		{LabelName: "label-cc", Values: []string{"c0000000"}},
	}, mockServer.SentResponses[1].Items)
}

func TestIngester_LabelValuesCardinality_SentInBatches(t *testing.T) {
	const labelValueSize = 1024 // make labelValueSize bigger to make test run faster, smaller to make the results more readable.
	const maxBatchLabelValues = queryStreamBatchMessageSize / labelValueSize
	labelValueTemplate := fmt.Sprintf("%%0%dd", labelValueSize)
	idx := 0
	nextLabelValue := func() string { idx++; return fmt.Sprintf(labelValueTemplate, idx) }

	samples := []mimirpb.Sample{{TimestampMs: 1_000, Value: 1}}
	seriesForLabel := func(label string) mimirpb.PreallocTimeseries {
		return mimirpb.PreallocTimeseries{TimeSeries: &mimirpb.TimeSeries{
			Labels: mimirpb.FromLabelsToLabelAdapters(labels.FromStrings(
				labels.MetricName, "metric",
				label, nextLabelValue())),
			Samples: samples,
		}}
	}

	in := prepareHealthyIngester(t, nil)
	ctx := user.InjectOrgID(context.Background(), userID)

	writeReq := &mimirpb.WriteRequest{Source: mimirpb.API}

	// First batch will contain exactly queryStreamBatchMessageSize bytes if label-a values
	for i := 0; i < maxBatchLabelValues; i++ {
		writeReq.Timeseries = append(writeReq.Timeseries, seriesForLabel("label-a"))
	}

	// Second batch will contain the last value of the label-a, and (maxBatchLabelValues-1) values of the label-b
	writeReq.Timeseries = append(writeReq.Timeseries, seriesForLabel("label-a"))
	for i := 0; i < maxBatchLabelValues-1; i++ {
		writeReq.Timeseries = append(writeReq.Timeseries, seriesForLabel("label-b"))
	}

	// Third batch will contain the last value of the label-b
	writeReq.Timeseries = append(writeReq.Timeseries, seriesForLabel("label-b"))

	// Write the series
	_, err := in.Push(ctx, writeReq)
	require.NoError(t, err)

	// Query
	mockServer := &mockLabelValuesCardinalityServer{context: ctx}
	req := &client.LabelValuesCardinalityRequest{
		LabelNames: []string{"label-a", "label-b"},
		Matchers:   nil,
	}

	err = in.LabelValuesCardinality(req, mockServer)
	require.NoError(t, err)
	require.Len(t, mockServer.SentResponses, 3, "There should be three responses")

	require.Equal(t, 1, len(mockServer.SentResponses[0].Items), "First response should contain only one label")
	require.Equalf(t, maxBatchLabelValues, len(mockServer.SentResponses[0].Items[0].LabelValueSeries), "First label of first response should contain max amount of labels %d", maxBatchLabelValues)

	require.Equal(t, 2, len(mockServer.SentResponses[1].Items), "Second server response should contain two label names")
	require.Equal(t, 1, len(mockServer.SentResponses[1].Items[0].LabelValueSeries), "First label of second response should contain one value")
	require.Equalf(t, maxBatchLabelValues-1, len(mockServer.SentResponses[1].Items[1].LabelValueSeries), "Second label of second response should contain %d values", maxBatchLabelValues-1)

	require.Equal(t, 1, len(mockServer.SentResponses[2].Items), "Third response should contain only one label")
	require.Equal(t, 1, len(mockServer.SentResponses[2].Items[0].LabelValueSeries), "First label of third response should contain one label")
}

func TestIngester_LabelValuesCardinality_AllValuesToBeReturnedInSingleMessage(t *testing.T) {
	testCases := map[string]struct {
		labels         []string
		matchers       []*client.LabelMatcher
		existingLabels map[string][]string
		expectedItems  []*client.LabelValueSeriesCount
	}{
		"empty response is returned when no labels are provided": {
			labels:         []string{"label-a", "label-b"},
			matchers:       nil,
			existingLabels: map[string][]string{},
			expectedItems:  nil,
		},
		"all values returned in a single message": {
			labels:   []string{"label-a", "label-b"},
			matchers: nil,
			existingLabels: map[string][]string{
				"label-a": {"a-0"},
			},
			expectedItems: []*client.LabelValueSeriesCount{
				{LabelName: "label-a", LabelValueSeries: map[string]uint64{"a-0": 1}},
			},
		},
		"all values returned in a single message if response size is less then batch size": {
			labels:   []string{"label-a", "label-b"},
			matchers: nil,
			existingLabels: map[string][]string{
				"label-a": {"a-0", "a-1", "a-2"},
				"label-b": {"b-0", "b-1"},
			},
			expectedItems: []*client.LabelValueSeriesCount{
				{
					LabelName:        "label-a",
					LabelValueSeries: map[string]uint64{"a-0": 1, "a-1": 2, "a-2": 3},
				},
				{
					LabelName:        "label-b",
					LabelValueSeries: map[string]uint64{"b-0": 1, "b-1": 2},
				},
			},
		},
		"only matching series are returned when a matcher is provided": {
			labels:   []string{"label-a", "label-b"},
			matchers: []*client.LabelMatcher{{Type: client.REGEX_MATCH, Name: "label-a", Value: "a-(1|2)"}},
			existingLabels: map[string][]string{
				"label-a": {"a-0", "a-1", "a-2"},
				"label-b": {"b-0", "b-1"},
			},
			expectedItems: []*client.LabelValueSeriesCount{
				{
					LabelName:        "label-a",
					LabelValueSeries: map[string]uint64{"a-1": 2, "a-2": 3},
				},
			},
		},
	}
	for tName, tCfg := range testCases {
		t.Run(tName, func(t *testing.T) {
			in := prepareHealthyIngester(t, nil)
			ctx := user.InjectOrgID(context.Background(), userID)

			samples := []mimirpb.Sample{{TimestampMs: 1_000, Value: 1}}
			for lblName, lblValues := range tCfg.existingLabels {
				for idx, lblValue := range lblValues {
					count := idx + 1
					for c := 0; c < count; c++ {
						_, err := in.Push(ctx, writeRequestSingleSeries(
							labels.FromStrings(labels.MetricName, "foo", lblName, lblValue, "counter", strconv.Itoa(c)),
							samples))
						require.NoError(t, err)
					}
				}
			}

			mockServer := &mockLabelValuesCardinalityServer{context: ctx}
			req := &client.LabelValuesCardinalityRequest{
				LabelNames: tCfg.labels,
				Matchers:   tCfg.matchers,
			}
			err := in.LabelValuesCardinality(req, mockServer)
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

func TestLabelNamesAndValues_ContextCancellation(t *testing.T) {
	cctx, cancel := context.WithCancel(context.Background())

	// Server mock.
	mockServer := mockLabelNamesAndValuesServer{context: cctx}
	var stream client.Ingester_LabelNamesAndValuesServer = &mockServer

	// Index reader mock.
	existingLabels := make(map[string][]string)
	lbValues := make([]string, 0, 100)
	for j := 0; j < 100; j++ {
		lbValues = append(lbValues, fmt.Sprintf("val-%d", j))
	}
	existingLabels["__name__"] = lbValues

	idxOpDelay := time.Millisecond * 100

	idxReader := &mockIndex{
		existingLabels: existingLabels,
		opDelay:        idxOpDelay,
	}

	var valueFilter = func(string, string) (bool, error) {
		return true, nil
	}
	doneCh := make(chan error, 1)
	go func() {
		err := labelNamesAndValues(
			idxReader,
			[]*labels.Matcher{},
			1*1024*1024, // 1MB
			stream,
			valueFilter,
		)
		doneCh <- err // Signal request completion.
	}()

	cancel() // Cancel stream context.

	// Assert labelNamesAndValues completion.
	select {
	case err := <-doneCh:
		require.ErrorIsf(t, err, context.Canceled, "labelNamesAndValues unexpected error: %s", err)

	case <-time.After(time.Second):
		require.Fail(t, "labelNamesAndValues was not completed after context cancellation")
	}
}

type infinitePostings struct{}

func (ip infinitePostings) Next() bool                  { return true }
func (ip infinitePostings) Seek(storage.SeriesRef) bool { return true }
func (ip infinitePostings) At() storage.SeriesRef       { return 0 }
func (ip infinitePostings) Err() error                  { return nil }

func TestCountLabelValueSeries_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := countLabelValueSeries(ctx, "lblName", "lblVal", nil, nil, func(context.Context, tsdb.IndexPostingsReader, ...*labels.Matcher) (index.Postings, error) {
		return infinitePostings{}, nil
	})

	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled)
}

func BenchmarkIngester_LabelValuesCardinality(b *testing.B) {
	const (
		userID     = "test"
		numSeries  = 10e6
		metricName = "metric_name"
	)

	in := prepareHealthyIngester(b, nil)
	ctx := user.InjectOrgID(context.Background(), userID)

	samples := []mimirpb.Sample{{TimestampMs: 1_000, Value: 1}}
	writeReq := &mimirpb.WriteRequest{Source: mimirpb.API}
	for s := 0; s < numSeries; s++ {
		writeReq.Timeseries = append(writeReq.Timeseries, mimirpb.PreallocTimeseries{
			TimeSeries: &mimirpb.TimeSeries{
				Labels: mimirpb.FromLabelsToLabelAdapters(labels.FromStrings(
					labels.MetricName, metricName,
					// Take prime modulus on the labels, to ensure that each one is a new series.
					"mod_10", strconv.Itoa(s%(2*5)),
					"mod_77", strconv.Itoa(s%(7*11)),
					"mod_4199", strconv.Itoa(s%(13*17*19)))),
				Samples: samples,
			},
		})
	}
	_, err := in.Push(ctx, writeReq)
	require.NoError(b, err)

	for _, bc := range []struct {
		name       string
		labelNames []string
		matchers   []*client.LabelMatcher
	}{
		{
			name:       "no matchers, __name__ label with 1 value all series",
			labelNames: []string{labels.MetricName},
			matchers:   nil,
		},
		{
			name:       "no matchers, mod_10 label, 1M series each",
			labelNames: []string{"mod_10"},
			matchers:   nil,
		},
		{
			name:       "no matchers, mod_77 label, 130K series each",
			labelNames: []string{"mod_77"},
			matchers:   nil,
		},
		{
			name:       "no matchers, mod_4199, 2400 series each",
			labelNames: []string{"mod_4199"},
			matchers:   nil,
		},
		{
			name:       "__name__ matcher, mod_10 label, 1M series each",
			labelNames: []string{"mod_10"},
			matchers:   []*client.LabelMatcher{{Type: client.EQUAL, Name: labels.MetricName, Value: metricName}},
		},
		{
			name:       "__name__ matcher, mod_77 label, 130K series each",
			labelNames: []string{"mod_77"},
			matchers:   []*client.LabelMatcher{{Type: client.EQUAL, Name: labels.MetricName, Value: metricName}},
		},
		{
			name:       "__name__ matcher, mod_4199 label, 2400 series each",
			labelNames: []string{"mod_77"},
			matchers:   []*client.LabelMatcher{{Type: client.EQUAL, Name: labels.MetricName, Value: metricName}},
		},
		{
			name:       "__name__ and mod_10 matchers, mod_4199 label, 240 series each",
			labelNames: []string{labels.MetricName, "mod_100"},
			matchers: []*client.LabelMatcher{
				{Type: client.EQUAL, Name: labels.MetricName, Value: metricName},
				{Type: client.EQUAL, Name: "mod_10", Value: "0"},
			},
		},
	} {
		b.Run(bc.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				req := &client.LabelValuesCardinalityRequest{
					LabelNames: bc.labelNames,
					Matchers:   bc.matchers,
				}
				mockServer := &mockLabelValuesCardinalityServer{context: ctx}
				err := in.LabelValuesCardinality(req, mockServer)
				require.NoError(b, err)
			}
		})
	}
}

type mockIndex struct {
	mock.Mock
	tsdb.IndexReader
	existingLabels map[string][]string
	opDelay        time.Duration
}

func (i *mockIndex) LabelNames(context.Context, ...*labels.Matcher) ([]string, error) {
	if i.opDelay > 0 {
		time.Sleep(i.opDelay)
	}
	var l []string
	for k := range i.existingLabels {
		l = append(l, k)
	}
	slices.Sort(l)
	return l, nil
}

func (i *mockIndex) LabelValues(_ context.Context, name string, _ ...*labels.Matcher) ([]string, error) {
	if i.opDelay > 0 {
		time.Sleep(i.opDelay)
	}
	return i.existingLabels[name], nil
}

func (i *mockIndex) Close() error { return nil }

func (i *mockIndex) Series(ref storage.SeriesRef, builder *labels.ScratchBuilder, chks *[]chunks.Meta) error {
	args := i.Called(ref, builder, chks)
	return args.Error(0)
}

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
