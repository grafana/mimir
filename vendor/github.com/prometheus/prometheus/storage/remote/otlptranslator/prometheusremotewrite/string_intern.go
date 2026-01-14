// Copyright 2024 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package prometheusremotewrite

import "sync"

// stringInterner interns common strings to reduce allocations.
// Thread-safe for concurrent use.
type stringInterner struct {
	mu      sync.RWMutex
	strings map[string]string
	maxSize int
}

// Common label names that should always be interned.
// These are pre-populated to avoid allocation on first use.
var commonLabelNames = []string{
	// Prometheus reserved labels
	"__name__",
	"job",
	"instance",
	"le",
	"quantile",
	// OTel scope labels
	"otel_scope_name",
	"otel_scope_version",
	// Common OTel resource attributes
	"service_name",
	"service_namespace",
	"service_instance_id",
	// Common metric attributes
	"host_name",
	"host_id",
	"container_id",
	"container_name",
	"k8s_pod_name",
	"k8s_namespace_name",
	"k8s_deployment_name",
	"k8s_node_name",
	// Common HTTP/RPC attributes
	"http_method",
	"http_status_code",
	"http_route",
	"rpc_method",
	"rpc_service",
	// Common database attributes
	"db_system",
	"db_name",
	"db_operation",
}

var defaultLabelInterner = newStringInterner(10000)

func init() {
	// Pre-populate common label names
	for _, s := range commonLabelNames {
		defaultLabelInterner.strings[s] = s
	}
}

func newStringInterner(maxSize int) *stringInterner {
	return &stringInterner{
		strings: make(map[string]string, len(commonLabelNames)+256),
		maxSize: maxSize,
	}
}

// Intern returns a canonical version of s, reducing memory if s was seen before.
// For strings not seen before, the original string is stored and returned.
func (si *stringInterner) Intern(s string) string {
	// Fast path: check if already interned (read lock only)
	si.mu.RLock()
	if interned, ok := si.strings[s]; ok {
		si.mu.RUnlock()
		return interned
	}
	si.mu.RUnlock()

	// Slow path: acquire write lock and add to map
	si.mu.Lock()
	defer si.mu.Unlock()

	// Double-check after acquiring write lock
	if interned, ok := si.strings[s]; ok {
		return interned
	}

	// Don't grow beyond max size to prevent unbounded memory growth
	if len(si.strings) >= si.maxSize {
		return s
	}

	si.strings[s] = s
	return s
}

// InternLabelName interns a label name using the global interner.
// Label names are highly repetitive across series, making them ideal for interning.
func InternLabelName(name string) string {
	return defaultLabelInterner.Intern(name)
}
