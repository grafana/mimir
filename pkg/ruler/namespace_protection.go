package ruler

import (
	"context"
	"errors"
	"strings"
)

const (
	OverrideProtectionHeader  = "X-Override-Protection"
	ProtectedNamespacesHeader = "X-Protected-Namespaces"
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

// ProtectedNamespaceHeaderFromString returns a HTTPHeader with the given namespace as the value and ProtectedNamespacesHeader as the key.
func ProtectedNamespaceHeaderFromString(namespace string) HTTPHeader {
	return HTTPHeader{
		Key:   ProtectedNamespacesHeader,
		Value: namespace,
	}
}

// ProtectedNamespacesHeaderFromSet returns a HTTPHeader with the given namespaces command-separated as the value and ProtectedNamespacesHeader as the key.
func ProtectedNamespacesHeaderFromSet(namespacesSet map[string]struct{}) HTTPHeader {
	if len(namespacesSet) == 0 {
		return HTTPHeader{}
	}

	ns := make([]string, 0, len(namespacesSet))

	for k, _ := range namespacesSet {
		ns = append(ns, k)
	}

	return HTTPHeader{
		Key:   ProtectedNamespacesHeader,
		Value: strings.Join(ns, ","),
	}
}

func AllowProtectionOverride(ctx context.Context, namespace string) error {
	value, ok := ctx.Value(OverrideProtectionHeader).(string)
	if !ok {
		return ErrNoProtectionOverrideHeader
	}

	// TODO: Perhaps support a wildcard for all namespaces?
	// TODO: Perhaps many namespaces is an overkill?
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
