// SPDX-License-Identifier: AGPL-3.0-only

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
		// Pass an invalid policy.
		_, _, err := policyFromHeaderValue("test-instance:xxx/yyy")
		require.Error(t, err)
	})

	t.Run("empty label name in policies results in error", func(t *testing.T) {
		policy := &LabelPolicy{
			Selector: []*labels.Matcher{
				{
					Type:  labels.MatchNotRegexp,
					Value: "test_value",
					// Empty string is not a valid label name.
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
								Value: "test_value",
								Name:  "test_name",
							},
							{
								Type:  labels.MatchNotEqual,
								Value: "other_value",
								Name:  "other_name",
							},
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test HTTP propagation.
			req, err := http.NewRequest("GET", "http://test.com", nil)
			require.NoError(t, err)

			err = InjectLabelMatchersHTTP(req, tt.instancePolicyMap)
			require.NoError(t, err)

			gotPolicyMap, err := ExtractLabelMatchersHTTP(req)
			require.NoError(t, err)
			require.Equal(t, tt.instancePolicyMap, gotPolicyMap)

			// Test slice propagation.
			values, err := InjectLabelMatchersSlice(tt.instancePolicyMap)
			require.NoError(t, err)

			gotPolicyMap, err = ExtractLabelMatchersSlice(values)
			require.NoError(t, err)
			require.Equal(t, tt.instancePolicyMap, gotPolicyMap)
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
								Value: "test_value",
								Name:  "test_name",
							},
							{
								Type:  labels.MatchNotEqual,
								Value: "other_value",
								Name:  "other_name",
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

			gotPolicyMap, err := ExtractLabelMatchersContext(ctx)
			require.NoError(t, err)
			require.Equal(t, tt.instancePolicyMap, gotPolicyMap)
		})
	}
}

func TestExtractLabelMatchers(t *testing.T) {
	for _, tc := range []struct {
		name                   string
		request                func() *http.Request
		expectedPolicies       LabelPolicySet
		expectedErrorSubstring string
	}{
		{
			name: "passthrough",
			request: func() *http.Request {
				req, _ := http.NewRequest("GET", "http://test.com", nil)
				return req
			},
			expectedPolicies:       LabelPolicySet{},
			expectedErrorSubstring: "",
		},
		{
			name: "single header with single policy",
			request: func() *http.Request {
				req, _ := http.NewRequest("GET", "http://test.com", nil)
				req.Header.Add(HTTPHeaderKey, "tenant1:%7Benv%3D%22prod%22%7D")
				return req
			},
			expectedPolicies: LabelPolicySet{
				"tenant1": {
					{
						Selector: []*labels.Matcher{
							labels.MustNewMatcher(labels.MatchEqual, "env", "prod"),
						},
					},
				},
			},
			expectedErrorSubstring: "",
		},
		{
			name: "single header with empty selector",
			request: func() *http.Request {
				req, _ := http.NewRequest("GET", "http://test.com", nil)
				req.Header.Add(HTTPHeaderKey, "tenant1:%7B%7D")
				return req
			},
			expectedPolicies: LabelPolicySet{
				"tenant1": {
					{
						Selector: []*labels.Matcher{},
					},
				},
			},
			expectedErrorSubstring: "",
		},
		{
			name: "single header with multiple policies",
			request: func() *http.Request {
				req, _ := http.NewRequest("GET", "http://test.com", nil)
				req.Header.Add(HTTPHeaderKey, "tenant1:%7Benv%3D%22prod%22%7D,tenant1:%7Bregion%3D%22us%22%7D")
				return req
			},
			expectedPolicies: LabelPolicySet{
				"tenant1": {
					{
						Selector: []*labels.Matcher{
							labels.MustNewMatcher(labels.MatchEqual, "env", "prod"),
						},
					},
					{
						Selector: []*labels.Matcher{
							labels.MustNewMatcher(labels.MatchEqual, "region", "us"),
						},
					},
				},
			},
			expectedErrorSubstring: "",
		},
		{
			name: "multiple headers with multiple policies",
			request: func() *http.Request {
				req, _ := http.NewRequest("GET", "http://test.com", nil)
				req.Header.Add(HTTPHeaderKey, "tenant1:%7Benv%3D%22prod%22%7D")
				req.Header.Add(HTTPHeaderKey, "tenant1:%7Bregion%3D%22us%22%7D")
				return req
			},
			expectedPolicies: LabelPolicySet{
				"tenant1": {
					{
						Selector: []*labels.Matcher{
							labels.MustNewMatcher(labels.MatchEqual, "env", "prod"),
						},
					},
					{
						Selector: []*labels.Matcher{
							labels.MustNewMatcher(labels.MatchEqual, "region", "us"),
						},
					},
				},
			},
			expectedErrorSubstring: "",
		},
		{
			name: "invalid header with extra comma",
			request: func() *http.Request {
				req, _ := http.NewRequest("GET", "http://test.com", nil)
				req.Header.Add(HTTPHeaderKey, "tenant1:%7Benv%3D%22prod%22%7D,")
				return req
			},
			expectedPolicies:       nil,
			expectedErrorSubstring: "invalid label policy header value, unable to parse tenant and selector",
		},
		{
			name: "invalid empty header",
			request: func() *http.Request {
				req, _ := http.NewRequest("GET", "http://test.com", nil)
				req.Header.Add(HTTPHeaderKey, "")
				return req
			},
			expectedPolicies:       nil,
			expectedErrorSubstring: "invalid label policy header value, unable to parse tenant and selector",
		},
		{
			name: "invalid header with no tenant",
			request: func() *http.Request {
				req, _ := http.NewRequest("GET", "http://test.com", nil)
				req.Header.Add(HTTPHeaderKey, "%7Benv%3D%22prod%22%7D")
				return req
			},
			expectedPolicies:       nil,
			expectedErrorSubstring: "invalid label policy header value, unable to parse tenant and selector",
		},
		{
			name: "invalid header with bad escape",
			request: func() *http.Request {
				req, _ := http.NewRequest("GET", "http://test.com", nil)
				req.Header.Add(HTTPHeaderKey, "tenant1:%ZZ")
				return req
			},
			expectedPolicies:       nil,
			expectedErrorSubstring: "invalid label policy header value unable to url escape selector string",
		},
		{
			name: "invalid header with bad selector",
			request: func() *http.Request {
				req, _ := http.NewRequest("GET", "http://test.com", nil)
				req.Header.Add(HTTPHeaderKey, "tenant1:{invalid==}")
				return req
			},
			expectedPolicies:       nil,
			expectedErrorSubstring: "invalid label policy header value unable to parse selector string",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			policies, err := ExtractLabelMatchersHTTP(tc.request())
			if tc.expectedErrorSubstring != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.expectedErrorSubstring)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expectedPolicies, policies)
			}
		})
	}
}

func TestHashCollisionResistance(t *testing.T) {
	// Generate many unique policy sets and ensure no hash collisions.
	hashes := make(map[string]LabelPolicySet)

	for i := 0; i < 50000; i++ {
		policySet := LabelPolicySet{
			fmt.Sprintf("tenant-%d", i): {
				{
					Selector: []*labels.Matcher{
						labels.MustNewMatcher(labels.MatchEqual, "env", fmt.Sprintf("env-%d", i)),
					},
				},
			},
		}

		hash := policySet.Hash()
		if existing, found := hashes[hash]; found {
			t.Fatalf("Hash collision detected:\n  PolicySet1: %v\n  PolicySet2: %v\n  Hash: %s", existing, policySet, hash)
		}
		hashes[hash] = policySet
	}
}
