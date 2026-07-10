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

		actual, err := DecodeLabelNamesRequest(req, 0)
		require.NoError(t, err)

		assert.Equal(t, expected, actual)
	})

	t.Run("DecodeLabelNamesRequest() with POST request", func(t *testing.T) {
		req, err := http.NewRequest("POST", "http://localhost/", strings.NewReader(params.Encode()))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

		actual, err := DecodeLabelNamesRequest(req, 0)
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

		actual, err := DecodeLabelValuesRequest(req, 0)
		require.NoError(t, err)

		assert.Equal(t, expected, actual)
	})

	t.Run("DecodeLabelValuesRequest() POST request", func(t *testing.T) {
		req, err := http.NewRequest("POST", "http://localhost/", strings.NewReader(params.Encode()))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

		actual, err := DecodeLabelValuesRequest(req, 0)
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

func TestDecodeLabelPresenceRequest(t *testing.T) {
	var (
		params = url.Values{
			"selector": []string{`{second!="2",first="1"}`},
			"label[]":  []string{"cluster", "namespace"},
			"limit":    []string{"50"},
		}

		expected = &LabelPresenceRequest{
			Matchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, "first", "1"),
				labels.MustNewMatcher(labels.MatchNotEqual, "second", "2"),
			},
			Labels: []string{"cluster", "namespace"},
			Limit:  50,
		}
	)

	t.Run("with GET request", func(t *testing.T) {
		req, err := http.NewRequest("GET", "http://localhost?"+params.Encode(), nil)
		require.NoError(t, err)

		actual, err := DecodeLabelPresenceRequest(req, 0)
		require.NoError(t, err)

		assert.Equal(t, expected, actual)
	})

	t.Run("with POST request", func(t *testing.T) {
		req, err := http.NewRequest("POST", "http://localhost/", strings.NewReader(params.Encode()))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

		actual, err := DecodeLabelPresenceRequest(req, 0)
		require.NoError(t, err)

		assert.Equal(t, expected, actual)
	})
}

func TestDecodeLabelPresenceRequest_Errors(t *testing.T) {
	t.Run("missing selector", func(t *testing.T) {
		_, err := DecodeLabelPresenceRequestFromValues(url.Values{
			"label[]": []string{"cluster"},
		})
		require.EqualError(t, err, "missing 'selector' parameter")
	})

	t.Run("missing label[]", func(t *testing.T) {
		_, err := DecodeLabelPresenceRequestFromValues(url.Values{
			"selector": []string{`{first="1"}`},
		})
		require.EqualError(t, err, "'label[]' param is required")
	})

	t.Run("invalid label name", func(t *testing.T) {
		_, err := DecodeLabelPresenceRequestFromValues(url.Values{
			"selector": []string{`{first="1"}`},
			"label[]":  []string{""},
		})
		require.EqualError(t, err, "invalid 'label[]' param ''")
	})
}

func TestLabelPresenceRequest_String(t *testing.T) {
	req := &LabelPresenceRequest{
		Matchers: []*labels.Matcher{
			labels.MustNewMatcher(labels.MatchEqual, "first", "1"),
			labels.MustNewMatcher(labels.MatchNotEqual, "second", "2"),
		},
		Labels: []string{"cluster", "namespace"},
		Limit:  50,
	}

	assert.Equal(t, "first=\"1\"\x01second!=\"2\"\x00cluster\x01namespace\x0050", req.String())
}
