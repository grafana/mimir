// SPDX-License-Identifier: AGPL-3.0-only

package backfill

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/thanos-io/objstore"
)

// Phases of a backfill. Each phase is recorded by a marker of that backfill.
const (
	PhaseBackfill = "backfill" // a backfill exists and its blocks are being uploaded
	PhaseValidate = "validate" // uploads are finished and the blocks are validated
	PhaseCompact  = "compact"  // the validated blocks are compacted
	PhaseCopy     = "copy"     // the compacted blocks are copied to blocks storage
	PhaseCleanup  = "cleanup"  // the backfill is finished or aborted, and its data and markers are deleted
)

// Each backfill keeps its phase markers and data under <tenant>/<backfill ID>/, relative to BucketPrefix
const phasesPathPrefix = "phases"

// Marker is the content of a phase marker.
type Marker struct {
	// ClearedAt is when the scheduler accepted dropping the work of the backfill. It is only set on cleanup markers.
	ClearedAt time.Time `json:"cleared_at,omitzero"`
}

// Prefix returns the prefix holding everything of a backfill, relative to BucketPrefix.
func Prefix(tenant string, backfillID uuid.UUID) string {
	return path.Join(tenant, backfillID.String())
}

// PhaseMarkerPath returns the path of a backfill's marker for a phase, relative to BucketPrefix.
func PhaseMarkerPath(tenant string, backfillID uuid.UUID, phase string) string {
	return path.Join(Prefix(tenant, backfillID), phasesPathPrefix, phase)
}

// DataPrefix returns the prefix holding the blocks of a backfill, relative to BucketPrefix.
func DataPrefix(tenant string, backfillID uuid.UUID) string {
	return path.Join(Prefix(tenant, backfillID), blocksPathPrefix)
}

// ListBackfills returns the IDs of a tenant's backfills that have any phase marker or data.
func ListBackfills(ctx context.Context, bkt objstore.BucketReader, tenant string) ([]uuid.UUID, error) {
	var ids []uuid.UUID
	prefix := tenant + objstore.DirDelim
	err := bkt.Iter(ctx, prefix, func(name string) error {
		dir, ok := strings.CutSuffix(strings.TrimPrefix(name, prefix), objstore.DirDelim)
		if !ok {
			return nil
		}
		if id, err := uuid.Parse(dir); err == nil {
			ids = append(ids, id)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list backfills: %w", err)
	}
	return ids, nil
}

// ReadMarker reads a backfill's marker for a phase. It reports false if there is no marker.
func ReadMarker(ctx context.Context, bkt objstore.BucketReader, tenant string, backfillID uuid.UUID, phase string) (Marker, bool, error) {
	r, err := bkt.Get(ctx, PhaseMarkerPath(tenant, backfillID, phase))
	if err != nil {
		if bkt.IsObjNotFoundErr(err) {
			return Marker{}, false, nil
		}
		return Marker{}, false, fmt.Errorf("failed to read %s marker: %w", phase, err)
	}
	defer r.Close()

	var m Marker
	if err := json.NewDecoder(r).Decode(&m); err != nil {
		return Marker{}, false, fmt.Errorf("failed to decode %s marker: %w", phase, err)
	}
	return m, true, nil
}

// WriteMarker writes a backfill's marker for a phase.
func WriteMarker(ctx context.Context, bkt objstore.Bucket, tenant string, backfillID uuid.UUID, phase string, m Marker) error {
	data, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("failed to encode %s marker: %w", phase, err)
	}
	if err := bkt.Upload(ctx, PhaseMarkerPath(tenant, backfillID, phase), bytes.NewReader(data)); err != nil {
		return fmt.Errorf("failed to write %s marker: %w", phase, err)
	}
	return nil
}
