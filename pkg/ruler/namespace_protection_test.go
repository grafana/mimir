// SPDX-License-Identifier: AGPL-3.0-only

package ruler

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/util/validation"
)

func Test_IsNamespaceProtected(t *testing.T) {
	tests := map[string]struct {
		userID    string
		namespace string
		protected bool
		limits    RulesLimits
	}{
		"Namespace is protected": {
			userID:    "user1",
			namespace: "namespace1",
			protected: true,
			limits: validation.MockOverrides(func(_ *validation.Limits, tenantLimits map[string]*validation.Limits) {
				tenantLimits["user1"] = validation.MockDefaultLimits()
				tenantLimits["user1"].RulerProtectedNamespaces = []string{"namespace1"}
			}),
		},
		"Namespace is not protected": {
			userID:    "user1",
			namespace: "namespace3",
			protected: false,
			limits: validation.MockOverrides(func(_ *validation.Limits, tenantLimits map[string]*validation.Limits) {
				tenantLimits["user1"] = validation.MockDefaultLimits()
				tenantLimits["user1"].RulerProtectedNamespaces = []string{"namespace1", "namespace2"}
			}),
		},
		"User has no protected namespaces": {
			userID:    "user2",
			namespace: "namespace1",
			protected: false,
			limits:    validation.MockDefaultOverrides(),
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			r := &Ruler{limits: tt.limits}
			got := r.IsNamespaceProtected(tt.userID, tt.namespace)
			require.Equalf(t, tt.protected, got, "IsNamespaceProtected() = %v, want %v", got, tt.protected)
		})
	}
}

func Test_ProtectedNamespacesHeaderFromString(t *testing.T) {
	header := ProtectedNamespacesHeaderFromString("namespace1")
	require.Equal(t, http.Header{ProtectedNamespacesHeader: []string{"namespace1"}}, header)
}

func Test_ProtectedNamespacesHeaderFromSet(t *testing.T) {
	tc := map[string]struct {
		namespacesSet map[string]struct{}
		expected      http.Header
	}{
		"Single namespace": {
			namespacesSet: map[string]struct{}{
				"namespace1": {},
			},
			expected: http.Header{ProtectedNamespacesHeader: []string{"namespace1"}},
		},
		"Multiple namespaces": {
			namespacesSet: map[string]struct{}{
				"namespace1": {},
				"namespace2": {},
			},
			expected: http.Header{ProtectedNamespacesHeader: []string{"namespace1", "namespace2"}},
		},
		"Empty namespaces": {
			namespacesSet: map[string]struct{}{},
			expected:      nil,
		},
	}

	for name, tt := range tc {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tt.expected, ProtectedNamespacesHeaderFromSet(tt.namespacesSet))
		})
	}
}

func Test_AllowProtectionOverride(t *testing.T) {
	tc := map[string]struct {
		reqHeaders http.Header
		namespace  string
		expected   error
	}{
		"Valid override": {
			reqHeaders: http.Header{
				OverrideProtectionHeader: {"namespace1"},
			},
			namespace: "namespace1",
			expected:  nil,
		},
		"No override header": {
			reqHeaders: http.Header{},
			namespace:  "namespace1",
			expected:   ErrNoProtectionOverrideHeader,
		},
		"Namespace not in override": {
			reqHeaders: http.Header{
				OverrideProtectionHeader: {"namespace2"},
			},
			namespace: "namespace1",
			expected:  ErrNamespaceTargetNotMatch,
		},
		"Empty override header": {
			reqHeaders: http.Header{
				OverrideProtectionHeader: {""},
			},
			namespace: "namespace1",
			expected:  ErrNoProtectionOverrideHeader,
		},
	}

	for name, tt := range tc {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tt.expected, AllowProtectionOverride(tt.reqHeaders, tt.namespace))
		})
	}
}
