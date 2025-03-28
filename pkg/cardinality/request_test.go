// SPDX-License-Identifier: AGPL-3.0-only

package cardinality

import (
	"io"
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
	t.Parallel()
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

	t.Run("errors if limit parameter is greater than 500", func(t *testing.T) {
		t.Parallel()
		p := url.Values{
			"selector": []string{`{fruit="mango"}`},
			"limit":    []string{"501"},
		}
		req, err := http.NewRequest("GET", "http://localhost?"+p.Encode(), nil)
		require.NoError(t, err)

		actual, err := DecodeLabelNamesRequest(req)
		assert.EqualError(t, err, "'limit' param cannot be greater than '500'")
		assert.Nil(t, actual)
	})

	t.Run("DecodeLabelNamesRequest() with GET request", func(t *testing.T) {
		t.Parallel()
		req, err := http.NewRequest("GET", "http://localhost?"+params.Encode(), nil)
		require.NoError(t, err)

		actual, err := DecodeLabelNamesRequest(req)
		require.NoError(t, err)

		assert.Equal(t, expected, actual)
	})

	t.Run("DecodeLabelNamesRequest() with POST request", func(t *testing.T) {
		t.Parallel()
		req, err := http.NewRequest("POST", "http://localhost/", strings.NewReader(params.Encode()))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

		actual, err := DecodeLabelNamesRequest(req)
		require.NoError(t, err)

		assert.Equal(t, expected, actual)
	})

	t.Run("DecodeLabelNamesRequestFromValues()", func(t *testing.T) {
		t.Parallel()
		actual, err := DecodeLabelNamesRequestFromValues(params)
		require.NoError(t, err)

		assert.Equal(t, expected, actual)
	})
}

func TestLabelNamesRequest_String(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
	tests := map[string]struct {
		give    *http.Request
		want    *LabelValuesRequest
		wantErr string
	}{
		"errors if limit is less than 0": {
			give: newRequest(
				t,
				http.MethodGet,
				localhostWithValues(url.Values{
					"label_names[]": []string{"metric_1"},
					"limit":         []string{"-1"},
				}),
				http.NoBody,
				nil,
			),
			want:    nil,
			wantErr: "'limit' param cannot be less than '0'",
		},
		"allows a limit greater than 500": {
			give: newRequest(
				t,
				http.MethodGet,
				localhostWithValues(url.Values{
					"label_names[]": []string{"metric_1"},
					"limit":         []string{"777"},
				}),
				http.NoBody,
				nil,
			),
			want: &LabelValuesRequest{
				LabelNames:  []model.LabelName{"metric_1"},
				Matchers:    nil,
				CountMethod: InMemoryMethod,
				Limit:       777,
			},
			wantErr: "",
		},
		"errors if limit is out of range": {
			give: newRequest(
				t,
				http.MethodGet,
				localhostWithValues(url.Values{
					"label_names[]": []string{"metric_1"},
					"limit":         []string{"9999999999999999999999999999"},
				}),
				http.NoBody,
				nil,
			),
			want:    nil,
			wantErr: `strconv.Atoi: parsing "9999999999999999999999999999": value out of range`,
		},
		"errors if limit is invalid": {
			give: newRequest(
				t,
				http.MethodGet,
				localhostWithValues(url.Values{
					"label_names[]": []string{"metric_1"},
					"limit":         []string{"this is not an integer"},
				}),
				http.NoBody,
				nil,
			),
			want:    nil,
			wantErr: `strconv.Atoi: parsing "this is not an integer": invalid syntax`,
		},
		"errors if multiple limit parameters are specified": {
			give: newRequest(
				t,
				http.MethodGet,
				localhostWithValues(url.Values{
					"label_names[]": []string{"metric_1"},
					"limit":         []string{"1", "2"},
				}),
				http.NoBody,
				nil,
			),
			want:    nil,
			wantErr: `multiple 'limit' params are not allowed`,
		},
		"limit defaults to 20": {
			give: newRequest(
				t,
				http.MethodGet,
				localhostWithValues(url.Values{
					"label_names[]": []string{"metric_1"},
					"count_method":  []string{"active"},
				}),
				http.NoBody,
				nil,
			),
			want: &LabelValuesRequest{
				LabelNames:  []model.LabelName{"metric_1"},
				Matchers:    nil,
				CountMethod: ActiveMethod,
				Limit:       20,
			},
			wantErr: "",
		},
		"count_method defaults to in_memory": {
			give: newRequest(
				t,
				http.MethodGet,
				localhostWithValues(url.Values{
					"label_names[]": []string{"metric_1"},
				}),
				http.NoBody,
				nil,
			),
			want: &LabelValuesRequest{
				LabelNames:  []model.LabelName{"metric_1"},
				Matchers:    nil,
				CountMethod: InMemoryMethod,
				Limit:       20,
			},
			wantErr: "",
		},
		"errors if count_method is invalid": {
			give: newRequest(
				t,
				http.MethodGet,
				localhostWithValues(url.Values{
					"label_names[]": []string{"metric_1"},
					"count_method":  []string{"pineapple"},
				}),
				http.NoBody,
				nil,
			),
			want:    nil,
			wantErr: "invalid 'count_method' param 'pineapple'. valid options are: [active,inmemory]",
		},
		"errors if label_names[] are missing": {
			give: newRequest(
				t,
				http.MethodGet,
				localhostWithValues(url.Values{}),
				http.NoBody,
				nil,
			),
			want:    nil,
			wantErr: "'label_names[]' param is required",
		},
		"errors if label_names[] are invalid": {
			give: newRequest(
				t,
				http.MethodGet,
				localhostWithValues(url.Values{
					"label_names[]": []string{""},
				}),
				http.NoBody,
				nil,
			),
			want:    nil,
			wantErr: "invalid 'label_names' param ''",
		},
		"errors if multiple selector parameters are specified": {
			give: newRequest(
				t,
				http.MethodGet,
				localhostWithValues(url.Values{
					"label_names[]": []string{"metric_1"},
					"selector":      []string{`{fruit="mango"}`, `{fruit="banana"}`},
				}),
				http.NoBody,
				nil,
			),
			want:    nil,
			wantErr: "multiple 'selector' params are not allowed",
		},
		"errors if selector parameter is invalid": {
			give: newRequest(
				t,
				http.MethodGet,
				localhostWithValues(url.Values{
					"label_names[]": []string{"metric_1"},
					"selector":      []string{`!!!!`},
				}),
				http.NoBody,
				nil,
			),
			want:    nil,
			wantErr: "failed to parse selector: 1:1: parse error: unexpected character after '!': '!'",
		},
		"valid DecodeLabelValuesRequest() GET request": {
			give: newRequest(
				t,
				http.MethodGet,
				localhostWithValues(url.Values{
					"selector":      []string{`{second!="2",first="1"}`},
					"label_names[]": []string{"metric_2", "metric_1"},
					"count_method":  []string{"active"},
					"limit":         []string{"100"},
				}),
				http.NoBody,
				nil,
			),
			want: &LabelValuesRequest{
				LabelNames: []model.LabelName{"metric_1", "metric_2"},
				Matchers: []*labels.Matcher{
					labels.MustNewMatcher(labels.MatchEqual, "first", "1"),
					labels.MustNewMatcher(labels.MatchNotEqual, "second", "2"),
				},
				CountMethod: ActiveMethod,
				Limit:       100,
			},
			wantErr: "",
		},
		"valid DecodeLabelValuesRequest() POST request": {
			give: newRequest(
				t,
				http.MethodPost,
				"http://localhost/",
				strings.NewReader(url.Values{
					"selector":      []string{`{second!="2",first="1"}`},
					"label_names[]": []string{"metric_2", "metric_1"},
					"count_method":  []string{"inmemory"},
					"limit":         []string{"100"},
				}.Encode()),
				http.Header{"Content-Type": []string{"application/x-www-form-urlencoded"}},
			),
			want: &LabelValuesRequest{
				LabelNames: []model.LabelName{"metric_1", "metric_2"},
				Matchers: []*labels.Matcher{
					labels.MustNewMatcher(labels.MatchEqual, "first", "1"),
					labels.MustNewMatcher(labels.MatchNotEqual, "second", "2"),
				},
				CountMethod: InMemoryMethod,
				Limit:       100,
			},
			wantErr: "",
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			actual, err := DecodeLabelValuesRequest(tt.give)
			assert.Equal(t, tt.want, actual)
			if tt.wantErr == "" {
				assert.NoError(t, err)
			} else {
				assert.EqualError(t, err, tt.wantErr)
			}
		})
	}
}

func TestDecodeLabelValuesRequestFromValues(t *testing.T) {
	t.Parallel()
	params := url.Values{
		"selector":      []string{`{second!="2",first="1"}`},
		"label_names[]": []string{"metric_2", "metric_1"},
		"count_method":  []string{"active"},
		"limit":         []string{"100"},
	}

	expected := &LabelValuesRequest{
		LabelNames: []model.LabelName{"metric_1", "metric_2"},
		Matchers: []*labels.Matcher{
			labels.MustNewMatcher(labels.MatchEqual, "first", "1"),
			labels.MustNewMatcher(labels.MatchNotEqual, "second", "2"),
		},
		CountMethod: ActiveMethod,
		Limit:       100,
	}

	actual, err := DecodeLabelValuesRequestFromValues(params)
	require.NoError(t, err)

	assert.Equal(t, expected, actual)
}

func TestLabelValuesRequest_String(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
	req := &ActiveSeriesRequest{
		Matchers: []*labels.Matcher{
			labels.MustNewMatcher(labels.MatchEqual, "first", "1"),
			labels.MustNewMatcher(labels.MatchNotEqual, "second", "2"),
		},
	}

	assert.Equal(t, "first=\"1\"\x01second!=\"2\"", req.String())
}

func newRequest(t *testing.T, method, url string, body io.Reader, h http.Header) *http.Request {
	t.Helper()

	r, err := http.NewRequest(method, url, body)
	require.NoError(t, err)
	r.Header = h

	return r
}

func localhostWithValues(v url.Values) string {
	return "http://localhost?" + v.Encode()
}
