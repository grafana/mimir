// SPDX-License-Identifier: AGPL-3.0-only

package transport

import (
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseSeekerBodyForm(t *testing.T) {
	t.Run("no body", func(t *testing.T) {
		expectedForm := url.Values{
			"bar": []string{"baz"},
		}

		req := httptest.NewRequest("GET", "/foo?bar=baz", nil)
		require.NoError(t, ParseSeekerBodyForm(req))
		require.Equal(t, expectedForm, req.Form)
	})

	t.Run("with body parsed twice for same form", func(t *testing.T) {
		expectedForm := url.Values{
			"bar": []string{"baz"},
		}

		req := httptest.NewRequest("POST", "/foo", strings.NewReader(expectedForm.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

		require.NoError(t, ParseSeekerBodyForm(req))
		require.Equal(t, expectedForm, req.Form)

		// Reset the form value
		req.Form, req.PostForm = nil, nil

		require.NoError(t, ParseSeekerBodyForm(req))
		require.Equal(t, expectedForm, req.Form)
	})

	t.Run("with body parsed twice and form changing", func(t *testing.T) {
		encodedForm := url.Values{
			"bar": []string{"baz"},
		}

		req := httptest.NewRequest("POST", "/foo", strings.NewReader(encodedForm.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

		require.NoError(t, ParseSeekerBodyForm(req))
		require.Equal(t, encodedForm, req.Form)

		// Add a query param
		query := req.URL.Query()
		query.Add("query", "value")
		req.URL.RawQuery = query.Encode()

		// Reset the form value
		req.Form, req.PostForm = nil, nil

		expectedForm := url.Values{
			"bar":   []string{"baz"},
			"query": []string{"value"},
		}
		require.NoError(t, ParseSeekerBodyForm(req))
		require.Equal(t, expectedForm, req.Form)
	})
}
