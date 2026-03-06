// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/grafana/backend-enterprise/pkg/labelaccess/propagation_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: Grafana Labs

package labelaccess

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
)

func TestPolicyHTTPHeaderValue(t *testing.T) {
	tests := []struct {
		name         string
		wantErr      bool
		instanceName string
		policy       *LabelPolicy
	}{
		{
			name:         "default",
			wantErr:      false,
			instanceName: "test-instance",
			policy: &LabelPolicy{
				Selector: []*labels.Matcher{
					{
						Type:  labels.MatchEqual,
						Value: "test_value",
						Name:  "test_name",
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			headerVal, err := policyToHeaderValue(tt.instanceName, tt.policy)
			require.NoError(t, err)

			instanceName, policy, err := policyFromHeaderValue(headerVal)
			require.NoError(t, err)
			require.Equal(t, tt.instanceName, instanceName)
			require.Equal(t, len(tt.policy.Selector), len(policy.Selector))
		})
	}

	t.Run("invalid label policies results in error", func(t *testing.T) {
		_, _, err := policyFromHeaderValue("test-instance:xxx/yyy")
		require.Error(t, err)
	})

	t.Run("empty label name in policies results in error", func(t *testing.T) {
		policy := &LabelPolicy{
			Selector: []*labels.Matcher{
				{
					Type:  labels.MatchNotRegexp,
					Value: "test_value",
					// Empty string is not a valid label name
					Name: "",
				},
			},
		}

		headerVal, err := policyToHeaderValue("test-instance", policy)
		require.NoError(t, err)

		_, _, err = policyFromHeaderValue(headerVal)
		require.Error(t, err)
	})
}

func TestPropagation(t *testing.T) {
	tests := []struct {
		name              string
		instancePolicyMap LabelPolicySet
	}{
		{
			name: "simple",
			instancePolicyMap: LabelPolicySet{
				"test-instance": {
					{
						Selector: []*labels.Matcher{
							{
								Type:  labels.MatchEqual,
								Value: "test_value",
								Name:  "test_name",
							},
						},
					},
				},
			},
		},
		{
			name: "multiple",
			instancePolicyMap: LabelPolicySet{
				"test-instance": {
					{
						Selector: []*labels.Matcher{
							{
								Type:  labels.MatchEqual,
								Name:  "test_name",
								Value: "test_value",
							},
							{
								Type:  labels.MatchNotEqual,
								Name:  "test_name_alt",
								Value: "test_value",
							},
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			requirePolicyIsExpected := func(t *testing.T, instancePolicyMap LabelPolicySet) {
				require.Equal(t, len(tt.instancePolicyMap), len(instancePolicyMap))

				for instanceName, policies := range instancePolicyMap {
					expectedPolicies, instanceExpected := tt.instancePolicyMap[instanceName]
					require.True(t, instanceExpected)
					require.Equal(t, len(expectedPolicies), len(policies))
					for i, policy := range policies {
						require.Equal(t, len(expectedPolicies[i].Selector), len(policy.Selector))
						for n, matcher := range policy.Selector {
							require.Equal(t, expectedPolicies[i].Selector[n].Type, matcher.Type)
							require.Equal(t, expectedPolicies[i].Selector[n].Name, matcher.Name)
							require.Equal(t, expectedPolicies[i].Selector[n].Value, matcher.Value)
						}
					}
				}
			}

			t.Run("HTTP", func(t *testing.T) {
				r := &http.Request{
					Header: http.Header{},
				}
				err := InjectLabelMatchersHTTP(r, tt.instancePolicyMap)
				require.NoError(t, err)

				instancePolicyMap, err := ExtractLabelMatchersHTTP(r)
				require.NoError(t, err)
				requirePolicyIsExpected(t, instancePolicyMap)
			})

			t.Run("slice", func(t *testing.T) {
				v, err := InjectLabelMatchersSlice(tt.instancePolicyMap)
				require.NoError(t, err)

				instancePolicyMap, err := ExtractLabelMatchersSlice(v)
				require.NoError(t, err)
				requirePolicyIsExpected(t, instancePolicyMap)
			})
		})
	}
}

func TestPropagationContext(t *testing.T) {
	tests := []struct {
		name              string
		instancePolicyMap LabelPolicySet
	}{
		{
			name: "simple",
			instancePolicyMap: LabelPolicySet{
				"test-instance": {
					{
						Selector: []*labels.Matcher{
							{
								Type:  labels.MatchEqual,
								Value: "test_value",
								Name:  "test_name",
							},
						},
					},
				},
			},
		},
		{
			name: "multiple",
			instancePolicyMap: LabelPolicySet{
				"test-instance": {
					{
						Selector: []*labels.Matcher{
							{
								Type:  labels.MatchEqual,
								Name:  "test_name",
								Value: "test_value",
							},
							{
								Type:  labels.MatchNotEqual,
								Name:  "test_name_alt",
								Value: "test_value",
							},
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			ctx = InjectLabelMatchersContext(ctx, tt.instancePolicyMap)
			instancePolicyMap, err := ExtractLabelMatchersContext(ctx)
			require.NoError(t, err)
			require.Equal(t, len(tt.instancePolicyMap), len(instancePolicyMap))

			for instanceName, policies := range instancePolicyMap {
				expectedPolicies, instanceExpected := tt.instancePolicyMap[instanceName]
				require.True(t, instanceExpected)
				require.Equal(t, len(expectedPolicies), len(policies))
				for i, policy := range policies {
					require.Equal(t, len(expectedPolicies[i].Selector), len(policy.Selector))
					for n, matcher := range policy.Selector {
						require.Equal(t, expectedPolicies[i].Selector[n].Type, matcher.Type)
						require.Equal(t, expectedPolicies[i].Selector[n].Name, matcher.Name)
						require.Equal(t, expectedPolicies[i].Selector[n].Value, matcher.Value)
					}
				}
			}
		})
	}
}

func TestExtractLabelMatchers(t *testing.T) {
	fooBarPolicy := &LabelPolicy{
		Selector: []*labels.Matcher{
			{
				Type:  labels.MatchEqual,
				Name:  "foo",
				Value: "bar",
			},
		},
	}
	fooCarPolicy := &LabelPolicy{
		Selector: []*labels.Matcher{
			{
				Type:  labels.MatchEqual,
				Name:  "foo",
				Value: "car",
			},
		},
	}

	tests := []struct {
		name         string
		headerValues []string
		want         LabelPolicySet
		wantErr      error
	}{
		{
			name: "passthrough",
			want: LabelPolicySet{},
		},
		{
			name:         "single-header/single-policy",
			headerValues: []string{"user-1:%7Bfoo=%22bar%22%7D"},
			want: LabelPolicySet{
				"user-1": {
					fooBarPolicy,
				},
			},
		},
		{
			name:         "single-header/empty-selector",
			headerValues: []string{"user-1:%7B%7D"},
			want: LabelPolicySet{
				"user-1": {
					{
						Selector: []*labels.Matcher{},
					},
				},
			},
		},
		{
			name:         "single-header/multiple-policies",
			headerValues: []string{"user-1:%7Bfoo=%22bar%22%7D,user-2:%7Bfoo=%22bar%22%7D,user-1:%7Bfoo=%22car%22%7D"},
			want: LabelPolicySet{
				"user-1": {
					fooBarPolicy,
					fooCarPolicy,
				},
				"user-2": {
					fooBarPolicy,
				},
			},
		},
		{
			name: "multiple-header/multiple-policies",
			headerValues: []string{
				"user-1:%7Bfoo=%22bar%22%7D,user-2:%7Bfoo=%22bar%22%7D,user-1:%7Bfoo=%22car%22%7D",
				"user-2:%7Bfoo=%22car%22%7D",
			},
			want: LabelPolicySet{
				"user-1": {
					fooBarPolicy,
					fooCarPolicy,
				},
				"user-2": {
					fooBarPolicy,
					fooCarPolicy,
				},
			},
		},
		{
			name: "invalid-header/extra-comma",
			headerValues: []string{
				"user-1:%7Bfoo=%22bar%22%7D,user-2:%7Bfoo=%22bar%22%7D,user-1:%7Bfoo=%22car%22%7D,",
			},
			wantErr: errInvalidHeaderParse,
		},
		{
			name: "invalid-header/empty",
			headerValues: []string{
				"user-1:%7Bfoo=%22bar%22%7D,user-2:%7Bfoo=%22bar%22%7D,user-1:%7Bfoo=%22car%22%7D,",
				"",
			},
			wantErr: errInvalidHeaderParse,
		},
		{
			name: "invalid-header/no-tenant",
			headerValues: []string{
				"%7Bfoo=%22bar%22%7D",
			},
			wantErr: errInvalidHeaderParse,
		},
		{
			name: "invalid-header/bad-escape",
			headerValues: []string{
				"user-1:%%7Bfoo=%22bar%22%7D",
			},
			wantErr: errInvalidHeaderEscape,
		},
		{
			name: "invalid-header/bad-selector",
			headerValues: []string{
				`user-1:{foo=="bar"}`,
			},
			wantErr: errInvalidSelector,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Run("from HTTP headers", func(t *testing.T) {
				r, err := http.NewRequest("GET", "test", nil)
				require.NoError(t, err)
				for _, headerValue := range tt.headerValues {
					r.Header.Add(HTTPHeaderKey, headerValue)
				}
				got, err := ExtractLabelMatchersHTTP(r)
				if tt.wantErr == nil {
					require.NoError(t, err)
				} else {
					require.ErrorIs(t, err, tt.wantErr)
					require.Equal(t, tt.want, got)
				}
			})

			t.Run("from slice", func(t *testing.T) {
				got, err := ExtractLabelMatchersSlice(tt.headerValues)
				if tt.wantErr == nil {
					require.NoError(t, err)
				} else {
					require.ErrorIs(t, err, tt.wantErr)
					require.Equal(t, tt.want, got)
				}
			})
		})
	}
}

func TestHashCollisionResistance(t *testing.T) {
	const instanceCount = 50000
	var lps LabelPolicySet
	fooBarPolicy := &LabelPolicy{
		Selector: []*labels.Matcher{
			{
				Type:  labels.MatchEqual,
				Name:  "foo",
				Value: "bar",
			},
		},
	}
	hashes := make(map[string]struct{})

	for i := 0; i < instanceCount; i++ {
		lps = LabelPolicySet{
			fmt.Sprintf("billing%d", i): {
				fooBarPolicy,
			},
		}
		hashes[lps.Hash()] = struct{}{}
	}

	require.Equal(t, len(hashes), instanceCount)
}
