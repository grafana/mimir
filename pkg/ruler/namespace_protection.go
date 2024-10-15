// SPDX-License-Identifier: AGPL-3.0-only

package ruler

import (
	"errors"
	"net/http"
	"slices"
	"strings"
)

const (
	OverrideProtectionHeader  = "X-Mimir-Ruler-Override-Namespace-Protection"
	ProtectedNamespacesHeader = "X-Mimir-Ruler-Protected-Namespaces"
)

var (
	ErrNoProtectionOverrideHeader = errors.New("no protection override header")
	ErrNamespaceTargetNotMatch    = errors.New("namespace does not match target")
)

// IsNamespaceProtected returns true if the namespace is protected for the given user.
func (r *Ruler) IsNamespaceProtected(userID string, namespace string) bool {
	namespaces := r.limits.RulerProtectedNamespaces(userID)
	for _, ns := range namespaces {
		if namespace == ns {
			return true
		}
	}

	return false
}

// ProtectedNamespacesHeaderFromString returns a http.Header with the given namespace as the value and ProtectedNamespacesHeader as the key.
func ProtectedNamespacesHeaderFromString(namespace string) http.Header {
	return http.Header{
		ProtectedNamespacesHeader: []string{namespace},
	}
}

// ProtectedNamespacesHeaderFromSet returns a http.Header with the given namespaces command-separated as the value and ProtectedNamespacesHeader as the key.
func ProtectedNamespacesHeaderFromSet(namespacesSet map[string]struct{}) http.Header {
	if len(namespacesSet) == 0 {
		return nil
	}

	ns := make([]string, 0, len(namespacesSet))
	for k := range namespacesSet {
		ns = append(ns, k)
	}

	slices.Sort(ns)

	return http.Header{
		ProtectedNamespacesHeader: ns,
	}
}

// AllowProtectionOverride checks if the request headers contain the protection override header and if the given namespace is in the list of overrides.
func AllowProtectionOverride(reqHeaders http.Header, namespace string) error {
	value := reqHeaders.Get(OverrideProtectionHeader)
	if value == "" {
		return ErrNoProtectionOverrideHeader
	}

	overrideNamespaces := strings.Split(value, ",")

	if len(overrideNamespaces) == 0 {
		return ErrNamespaceTargetNotMatch
	}

	for _, overrideNamespace := range overrideNamespaces {
		if overrideNamespace == namespace {
			return nil
		}
	}

	return ErrNamespaceTargetNotMatch
}
