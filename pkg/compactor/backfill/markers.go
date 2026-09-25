// SPDX-License-Identifier: AGPL-3.0-only

package backfill

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/thanos-io/objstore"
)

const (
	PhaseBackfill = "backfill"
	PhaseValidate = "validate"
	PhaseCompact  = "compact"
	PhaseCopy     = "copy"
	PhaseCleanup  = "cleanup"

	// PhasesPrefix holds one marker per tenant for each phase of its backfill, named phases/<phase>/<tenant>
	PhasesPrefix = "phases/"
	dataPrefix   = "data/"
)

func IsPhase(name string) bool {
	switch name {
	case PhaseBackfill, PhaseValidate, PhaseCompact, PhaseCopy, PhaseCleanup:
		return true
	}
	return false
}

func PhaseMarkerPath(phase, tenant string) string {
	return PhasesPrefix + phase + objstore.DirDelim + tenant
}

// DataPrefix is the prefix of the blocks of a tenant's backfill
func DataPrefix(backfillID, tenant string) string {
	return dataPrefix + backfillID + objstore.DirDelim + tenant
}

// Marker is the content of a phase marker
type Marker struct {
	BackfillID string `json:"backfill_id"`
}

// ReadMarker reads a tenant's marker for a phase, returning false if it does not exist
func ReadMarker(ctx context.Context, bkt objstore.BucketReader, phase, tenant string) (Marker, bool, error) {
	name := PhaseMarkerPath(phase, tenant)
	r, err := bkt.Get(ctx, name)
	if err != nil {
		if bkt.IsObjNotFoundErr(err) {
			return Marker{}, false, nil
		}
		return Marker{}, false, fmt.Errorf("failed to read %s: %w", name, err)
	}
	defer r.Close()

	content, err := io.ReadAll(r)
	if err != nil {
		return Marker{}, false, fmt.Errorf("failed to read %s: %w", name, err)
	}
	var m Marker
	if err := json.Unmarshal(content, &m); err != nil {
		return Marker{}, false, fmt.Errorf("failed to decode %s: %w", name, err)
	}
	return m, true, nil
}

func WriteMarker(ctx context.Context, bkt objstore.Bucket, phase, tenant string, m Marker) error {
	content, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return bkt.Upload(ctx, PhaseMarkerPath(phase, tenant), bytes.NewReader(content))
}
