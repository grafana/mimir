// SPDX-License-Identifier: AGPL-3.0-only

package cardinality

import (
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecodeLabelNamesRequest(t *testing.T) {
	var (
		params = url.Values{
			"selector":     []string{`{second!="2",first="1"}`},
			"count_method": []string{"active"},
			"limit":        []string{"100"},
		}

		expected = &LabelNamesRequest{
			Matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "first", "1"),
				labels.MustNewMatcher(labels.MatchNotEqual, "second", "2"),
			},
			CountMethod: ActiveMethod,
			Limit:       100,
		}
	)

	t.Run("DecodeLabelNamesRequest() with GET request", func(t *testing.T) {
		req, err := http.NewRequest("GET", "http://localhost?"+params.Encode(), nil)
		require.NoError(t, err)

		actual, err := DecodeLabelNamesRequest(req)
		require.NoError(t, err)

		assert.Equal(t, expected, actual)
	})

	t.Run("DecodeLabelNamesRequest() with POST request", func(t *testing.T) {
		req, err := http.NewRequest("POST", "http://localhost/", strings.NewReader(params.Encode()))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

		actual, err := DecodeLabelNamesRequest(req)
		require.NoError(t, err)

		assert.Equal(t, expected, actual)
	})

	t.Run("DecodeLabelNamesRequestFromValues()", func(t *testing.T) {
		actual, err := DecodeLabelNamesRequestFromValues(params)
		require.NoError(t, err)

		assert.Equal(t, expected, actual)
	})
}

func TestLabelNamesRequest_String(t *testing.T) {
	req := &LabelNamesRequest{
		Matchers: []*labels.Matcher{
			labels.MustNewMatcher(labels.MatchEqual, "first", "1"),
			labels.MustNewMatcher(labels.MatchNotEqual, "second", "2"),
		},
		CountMethod: ActiveMethod,
		Limit:       100,
	}

	assert.Equal(t, "first=\"1\"\x01second!=\"2\"\x00active\x00100", req.String())
}

func TestDecodeLabelValuesRequest(t *testing.T) {
	var (
		params = url.Values{
			"selector":      []string{`{second!="2",first="1"}`},
			"label_names[]": []string{"metric_2", "metric_1"},
			"count_method":  []string{"active"},
			"limit":         []string{"100"},
		}

		expected = &LabelValuesRequest{
			LabelNames: []model.LabelName{"metric_1", "metric_2"},
			Matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "first", "1"),
				labels.MustNewMatcher(labels.MatchNotEqual, "second", "2"),
			},
			CountMethod: ActiveMethod,
			Limit:       100,
		}
	)

	t.Run("DecodeLabelValuesRequest() GET request", func(t *testing.T) {
		req, err := http.NewRequest("GET", "http://localhost?"+params.Encode(), nil)
		require.NoError(t, err)

		actual, err := DecodeLabelValuesRequest(req)
		require.NoError(t, err)

		assert.Equal(t, expected, actual)
	})

	t.Run("DecodeLabelValuesRequest() POST request", func(t *testing.T) {
		req, err := http.NewRequest("POST", "http://localhost/", strings.NewReader(params.Encode()))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

		actual, err := DecodeLabelValuesRequest(req)
		require.NoError(t, err)

		assert.Equal(t, expected, actual)
	})

	t.Run("DecodeLabelValuesRequestFromValues() GET request", func(t *testing.T) {
		actual, err := DecodeLabelValuesRequestFromValues(params)
		require.NoError(t, err)

		assert.Equal(t, expected, actual)
	})
}

func TestLabelValuesRequest_String(t *testing.T) {
	req := &LabelValuesRequest{
		LabelNames: []model.LabelName{"foo", "bar"},
		Matchers: []*labels.Matcher{
			labels.MustNewMatcher(labels.MatchEqual, "first", "1"),
			labels.MustNewMatcher(labels.MatchNotEqual, "second", "2"),
		},
		CountMethod: ActiveMethod,
		Limit:       100,
	}

	assert.Equal(t, "foo\x01bar\x00first=\"1\"\x01second!=\"2\"\x00active\x00100", req.String())
}

func TestActiveSeriesRequest_String(t *testing.T) {
	req := &ActiveSeriesRequest{
		Matchers: []*labels.Matcher{
			labels.MustNewMatcher(labels.MatchEqual, "first", "1"),
			labels.MustNewMatcher(labels.MatchNotEqual, "second", "2"),
		},
	}

	assert.Equal(t, "first=\"1\"\x01second!=\"2\"", req.String())
}
