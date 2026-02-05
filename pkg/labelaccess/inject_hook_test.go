// SPDX-License-Identifier: AGPL-3.0-only

package labelaccess

import (
	"net/http/httptest"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
)

type LabelPoliciesMock struct {
	LabelPolicySet LabelPolicySet
}

func (m *LabelPoliciesMock) GetLabelPolicySet() (LabelPolicySet, error) {
	return m.LabelPolicySet, nil
}

func TestInjectMatchers(t *testing.T) {
	t.Run("user-defined X-Prom-Label-Policy, no label policies", func(t *testing.T) {
		r := httptest.NewRequest("GET", "http://test.com", nil)
		r.Header.Add(HTTPHeaderKey, "user-defined")
		err := InjectMatchers(r, &LabelPoliciesMock{})
		require.NoError(t, err)
		value := r.Header.Get(HTTPHeaderKey)
		require.Equal(t, "user-defined", value)
	})

	t.Run("user-defined X-Prom-Label-Policy, with label policies", func(t *testing.T) {
		r := httptest.NewRequest("GET", "http://test.com", nil)

		labelPoliciesMock := &LabelPoliciesMock{
			LabelPolicySet: LabelPolicySet{
				"1": []*LabelPolicy{{
					Selector: []*labels.Matcher{{
						Type:  labels.MatchEqual,
						Name:  HTTPHeaderKey,
						Value: "policy-defined",
					}},
				}},
			},
		}

		r.Header.Add(HTTPHeaderKey, "user-defined")
		err := InjectMatchers(r, labelPoliciesMock)
		require.NoError(t, err)
		value := r.Header.Get(HTTPHeaderKey)
		require.Contains(t, value, "policy-defined")
		require.NotContains(t, value, "user-defined")
	})

	t.Run("no user-defined X-Prom-Label-Policy, no label policies", func(t *testing.T) {
		r := httptest.NewRequest("GET", "http://test.com", nil)

		labelPoliciesMock := &LabelPoliciesMock{}
		err := InjectMatchers(r, labelPoliciesMock)
		require.NoError(t, err)
		value := r.Header.Get(HTTPHeaderKey)
		require.Equal(t, "", value)
	})

	t.Run("no user-defined X-Prom-Label-Policy, with label policies", func(t *testing.T) {
		r := httptest.NewRequest("GET", "http://test.com", nil)

		labelPoliciesMock := &LabelPoliciesMock{
			LabelPolicySet: LabelPolicySet{
				"1": []*LabelPolicy{{
					Selector: []*labels.Matcher{{
						Type:  labels.MatchEqual,
						Name:  HTTPHeaderKey,
						Value: "policy-defined",
					}},
				}},
			},
		}

		err := InjectMatchers(r, labelPoliciesMock)
		require.NoError(t, err)
		value := r.Header.Get(HTTPHeaderKey)
		require.Contains(t, value, "policy-defined")
		require.NotContains(t, value, "user-defined")
	})
}
