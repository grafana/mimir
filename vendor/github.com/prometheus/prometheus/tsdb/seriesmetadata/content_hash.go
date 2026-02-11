// Copyright The Prometheus Authors
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

package seriesmetadata

import (
	"slices"

	"github.com/cespare/xxhash/v2"
)

// hashResourceContent computes a deterministic xxhash for a ResourceVersion's content.
// The hash covers identifying attrs, descriptive attrs, and all entities.
// It does NOT include MinTime/MaxTime since those are per-mapping, not per-content.
// Attribute keys are sorted before hashing to ensure determinism regardless of map
// iteration order.
func hashResourceContent(rv *ResourceVersion) uint64 {
	h := xxhash.New()

	hashAttrs(h, rv.Identifying)
	h.Write([]byte{1}) // section separator
	hashAttrs(h, rv.Descriptive)
	h.Write([]byte{1})

	// Entities must be sorted by Type (enforced by NewResourceVersion and parseResourceContent).
	for _, e := range rv.Entities {
		h.WriteString(e.Type)
		h.Write([]byte{0})
		hashAttrs(h, e.ID)
		h.Write([]byte{1})
		hashAttrs(h, e.Description)
		h.Write([]byte{1})
	}

	return h.Sum64()
}

// hashScopeContent computes a deterministic xxhash for a ScopeVersion's content.
// The hash covers name, version, schema URL, and attributes.
// It does NOT include MinTime/MaxTime.
func hashScopeContent(sv *ScopeVersion) uint64 {
	h := xxhash.New()

	h.WriteString(sv.Name)
	h.Write([]byte{0})
	h.WriteString(sv.Version)
	h.Write([]byte{0})
	h.WriteString(sv.SchemaURL)
	h.Write([]byte{0})
	hashAttrs(h, sv.Attrs)

	return h.Sum64()
}

// hashAttrs writes a deterministic representation of a string map into a hash.
// Keys are sorted before hashing for determinism.
func hashAttrs(h *xxhash.Digest, attrs map[string]string) {
	keys := make([]string, 0, len(attrs))
	for k := range attrs {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	for _, k := range keys {
		h.WriteString(k)
		h.Write([]byte{0})
		h.WriteString(attrs[k])
		h.Write([]byte{0})
	}
}
