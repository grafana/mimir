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
	"maps"

	"github.com/cespare/xxhash/v2"
)

// KindID uniquely identifies a metadata kind.
type KindID string

const (
	KindResource KindID = "resource"
)

// kindMetaAccessor is implemented by memSeries to provide kind-generic access
// to per-series metadata. Defined here (in seriesmetadata) so that commit
// implementations can use it without importing tsdb.
type kindMetaAccessor interface {
	GetKindMeta(id KindID) (any, bool)
	SetKindMeta(id KindID, v any)
}

// resourceOps implements KindOps for *ResourceVersion.
type resourceOps struct{}

func (resourceOps) Equal(a, b *ResourceVersion) bool         { return ResourceVersionsEqual(a, b) }
func (resourceOps) Copy(v *ResourceVersion) *ResourceVersion { return copyResourceVersion(v) }

func (resourceOps) ContentHash(v *ResourceVersion) uint64 { return hashResourceContent(v) }
func (resourceOps) ThinCopy(canonical, v *ResourceVersion) *ResourceVersion {
	return &ResourceVersion{
		Identifying: canonical.Identifying,
		Descriptive: canonical.Descriptive,
		MinTime:     v.MinTime,
		MaxTime:     v.MaxTime,
	}
}

func (resourceOps) IsInterned(canonical, v *ResourceVersion) bool {
	return mapSameUnderlying(canonical.Identifying, v.Identifying)
}

// ResourceOps is the shared KindOps instance for resources.
var ResourceOps KindOps[*ResourceVersion] = resourceOps{}

// hashResourceContent computes a deterministic xxhash for a ResourceVersion's content.
// The hash covers identifying attrs and descriptive attrs.
// It does NOT include MinTime/MaxTime since those are per-mapping, not per-content.
func hashResourceContent(rv *ResourceVersion) uint64 {
	hash, _ := hashResourceContentReusable(rv, nil)
	return hash
}

func hashResourceContentReusable(rv *ResourceVersion, keysBuf []string) (uint64, []string) {
	return ResourceContentHash(rv.Identifying, rv.Descriptive, keysBuf)
}

// ResourceContentHash is the content hash that the store records for a resource with these attributes, as
// ResourceHasContentHash checks it. keysBuf is a buffer it reuses and returns; with one of enough capacity, it doesn't
// allocate.
func ResourceContentHash(identifying, descriptive map[string]string, keysBuf []string) (uint64, []string) {
	var h xxhash.Digest

	keysBuf = hashAttrs(&h, identifying, keysBuf)
	_, _ = h.Write([]byte{1}) // section separator
	keysBuf = hashAttrs(&h, descriptive, keysBuf)

	return h.Sum64(), keysBuf
}

// ResourceCommitData carries resource WAL record data without importing tsdb/record.
type ResourceCommitData struct {
	Identifying map[string]string
	Descriptive map[string]string
	MinTime     int64
	MaxTime     int64
	Owned       bool
}

// CommitResourceDirect is the hot-path commit for resources.
func CommitResourceDirect(accessor kindMetaAccessor, rcd ResourceCommitData) {
	rv := &ResourceVersion{
		Identifying: maps.Clone(rcd.Identifying),
		Descriptive: maps.Clone(rcd.Descriptive),
		MinTime:     rcd.MinTime,
		MaxTime:     rcd.MaxTime,
	}

	existing, _ := accessor.GetKindMeta(KindResource)
	if existing == nil {
		accessor.SetKindMeta(KindResource, &Versioned[*ResourceVersion]{
			Versions: []*ResourceVersion{rv},
		})
	} else {
		vr := existing.(*Versioned[*ResourceVersion])
		if len(vr.Versions) > 0 && ResourceOps.Equal(vr.Versions[len(vr.Versions)-1], rv) {
			vr.Versions[len(vr.Versions)-1].UpdateTimeRange(rv.MinTime, rv.MaxTime)
		} else {
			vr.Versions = append(vr.Versions, rv)
		}
	}
}

func hashResourceCommitDataReusable(rcd ResourceCommitData, keysBuf []string) (uint64, []string) {
	return ResourceContentHash(rcd.Identifying, rcd.Descriptive, keysBuf)
}

// CommitResourceToStoreReusableWithRef builds a ResourceVersion from ResourceCommitData and
// commits it directly to the MemStore, reusing a key buffer and tracking the series ref.
func CommitResourceToStoreReusableWithRef(store *MemStore[*ResourceVersion], labelsHash uint64, rcd ResourceCommitData, seriesRef uint64, keysBuf []string) (contentChanged bool, old, cur *VersionedResource, updatedKeysBuf []string) {
	contentHash, keysBuf := hashResourceCommitDataReusable(rcd, keysBuf)
	contentChanged, old, cur = store.InsertVersionWithRef(labelsHash, contentHash, rcd.MinTime, rcd.MaxTime, seriesRef, func() *ResourceVersion {
		return buildResourceVersion(rcd)
	}, rcd.Owned)
	return contentChanged, old, cur, keysBuf
}

func buildResourceVersion(rcd ResourceCommitData) *ResourceVersion {
	cloneMap := maps.Clone[map[string]string]
	if rcd.Owned {
		cloneMap = func(m map[string]string) map[string]string { return m }
	}

	return &ResourceVersion{
		Identifying: cloneMap(rcd.Identifying),
		Descriptive: cloneMap(rcd.Descriptive),
		MinTime:     rcd.MinTime,
		MaxTime:     rcd.MaxTime,
	}
}

// CollectResourceDirect is the hot-path equivalent of CollectFromSeries.
func CollectResourceDirect(accessor kindMetaAccessor) (*VersionedResource, bool) {
	v, ok := accessor.GetKindMeta(KindResource)
	if !ok || v == nil {
		return nil, false
	}
	return v.(*Versioned[*ResourceVersion]), true
}
