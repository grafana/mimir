// SPDX-License-Identifier: AGPL-3.0-only

package ruler

import (
	"net/url"
	"path/filepath"
	"sync"
)

// GroupURLRegistry maintains a mapping of per-rule-group remote write URLs.
// It is populated by DefaultMultiTenantManager.syncRulesToManager and consumed
// by the GroupEvaluationContextFunc in DefaultTenantManagerFactory.
type GroupURLRegistry struct {
	mu   sync.RWMutex
	urls map[string]map[string][]string // userID → "namespace/groupName" → []URL
}

// NewGroupURLRegistry creates an empty GroupURLRegistry.
func NewGroupURLRegistry() *GroupURLRegistry {
	return &GroupURLRegistry{urls: make(map[string]map[string][]string)}
}

// Set replaces the entire set of per-group URLs for a tenant.
// groupURLs maps "namespace/groupName" → []URL.
func (r *GroupURLRegistry) Set(userID string, groupURLs map[string][]string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(groupURLs) == 0 {
		delete(r.urls, userID)
		return
	}
	r.urls[userID] = groupURLs
}

// Remove removes all per-group URL entries for a tenant.
func (r *GroupURLRegistry) Remove(userID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.urls, userID)
}

// Lookup returns the remote write URLs for the named group within userID's namespace.
// Returns nil if no per-group URLs are configured.
func (r *GroupURLRegistry) Lookup(userID, namespace, groupName string) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if tenantURLs, ok := r.urls[userID]; ok {
		return tenantURLs[namespace+"/"+groupName]
	}
	return nil
}

// namespaceFromGroupFile extracts the namespace (decoded) from the on-disk rule file
// path produced by the mapper: <rulePath>/<userID>/<url.PathEscape(namespace)>.
func namespaceFromGroupFile(filePath string) string {
	encoded := filepath.Base(filePath)
	ns, err := url.PathUnescape(encoded)
	if err != nil {
		return encoded
	}
	return ns
}
