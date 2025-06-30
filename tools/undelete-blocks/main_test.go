// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"maps"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/util/objtools"
)

func TestBlocksFromJSON(t *testing.T) {
	ids := make([]ulid.ULID, 2)
	for i := uint64(0); i < 2; i++ {
		id, err := ulid.New(i, nil)
		require.NoError(t, err)
		ids[i] = id
	}
	m := map[string][]ulid.ULID{
		"tenant1": ids,
		"tenant2": ids,
	}
	filteredMap := maps.Clone(m)
	delete(filteredMap, "tenant2")

	validContent, err := json.Marshal(m)
	require.NoError(t, err)

	testCases := map[string]struct {
		content     string
		filter      tenantFilter
		expectMap   map[string][]ulid.ULID
		expectError bool
	}{

		"valid": {
			content:   string(validContent),
			filter:    nopTenantFilter,
			expectMap: m,
		},
		"valid, filtered": {
			content:   string(validContent),
			filter:    func(s string) bool { return s != "tenant2" },
			expectMap: filteredMap,
		},
		"invalid": {
			content:     "}{",
			filter:      nopTenantFilter,
			expectError: true,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			m, err := getBlocksFromJSON(strings.NewReader(tc.content), tc.filter)
			if tc.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.expectMap, m)
		})
	}
}

func TestGetBlocksFromLines(t *testing.T) {
	ids := make([]ulid.ULID, 2)
	for i := uint64(0); i < 2; i++ {
		id, err := ulid.New(i, nil)
		require.NoError(t, err)
		ids[i] = id
	}
	validContent := strings.Join(
		[]string{
			"tenant1/" + ids[0].String(),
			"tenant1/" + ids[1].String(),
			"tenant2/" + ids[0].String() + "/",
			"tenant2/" + ids[1].String() + "/",
		}, "\n")

	testCases := map[string]struct {
		content     string
		expectMap   map[string][]ulid.ULID
		filter      tenantFilter
		expectError bool
	}{
		"valid": {
			content: validContent,
			filter:  nopTenantFilter,
			expectMap: map[string][]ulid.ULID{
				"tenant1": ids,
				"tenant2": ids,
			},
		},
		"valid, filtered": {
			content: validContent,
			filter:  func(s string) bool { return s != "tenant2" },
			expectMap: map[string][]ulid.ULID{
				"tenant1": ids,
			},
		},
		"invalid": {
			content:     "tenant/a",
			filter:      nopTenantFilter,
			expectError: true,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			m, err := getBlocksFromLines(strings.NewReader(tc.content), tc.filter)
			if tc.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.expectMap, m)
		})
	}
}

func nopTenantFilter(string) bool {
	return true
}

func TestVersionToRestore(t *testing.T) {
	matchingSize := int64(1)
	mismatchSize := int64(2)

	currentVersion := version{
		size: matchingSize,
		info: objtools.VersionInfo{
			VersionID: "1",
			IsCurrent: true,
		},
	}
	deleteMarkerVersion := version{
		info: objtools.VersionInfo{
			IsDeleteMarker: true,
			VersionID:      "2",
		},
	}
	olderNoncurrentVersion := version{
		size:         matchingSize,
		lastModified: time.Unix(1, 1),
		info: objtools.VersionInfo{
			VersionID: "3",
			IsCurrent: false,
		},
	}
	noncurrentVersion := version{
		size:         matchingSize,
		lastModified: time.Unix(2, 2),
		info: objtools.VersionInfo{
			VersionID: "4",
			IsCurrent: false,
		},
	}
	require.True(t, noncurrentVersion.lastModified.After(olderNoncurrentVersion.lastModified))

	testCases := map[string]struct {
		versions      []version
		targetSize    *int64
		expectVersion *version
		expectOk      bool
	}{
		"no versions": {
			versions:      nil,
			targetSize:    nil,
			expectVersion: nil,
			expectOk:      false,
		},
		"current version": {
			versions:      []version{currentVersion},
			targetSize:    nil,
			expectVersion: nil,
			expectOk:      true,
		},
		"size mismatch": {
			versions:      []version{currentVersion},
			targetSize:    &mismatchSize,
			expectVersion: nil,
			expectOk:      false,
		},
		"delete marker": {
			versions:      []version{deleteMarkerVersion},
			targetSize:    nil,
			expectVersion: nil,
			expectOk:      false,
		},
		"noncurrent version": {
			versions:      []version{deleteMarkerVersion, noncurrentVersion},
			targetSize:    &matchingSize,
			expectVersion: &noncurrentVersion,
			expectOk:      true,
		},
		"choose more recent noncurrent": {
			versions:      []version{olderNoncurrentVersion, noncurrentVersion, olderNoncurrentVersion},
			targetSize:    &matchingSize,
			expectVersion: &noncurrentVersion,
			expectOk:      true,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			version, ok := versionToRestore(tc.versions, tc.targetSize)
			require.Equal(t, tc.expectVersion, version)
			require.Equal(t, tc.expectOk, ok)
		})
	}
}

func TestHandleDeleteMarkers(t *testing.T) {
	tenantID := "tenant"
	blockID, err := ulid.New(0, nil)
	require.NoError(t, err)

	localMarkerPath := tenantID + objtools.Delim + blockID.String() + objtools.Delim + block.DeletionMarkFilename

	currentLocal := []version{
		{
			info: objtools.VersionInfo{
				VersionID: "1",
				IsCurrent: true,
			},
		},
	}
	recoverableLocal := []version{
		{
			info: objtools.VersionInfo{
				VersionID: "1",
				IsCurrent: false,
			},
		},
	}
	unrecoverableLocal := []version{
		{
			info: objtools.VersionInfo{
				VersionID:      "1",
				IsDeleteMarker: true,
			},
		},
	}

	testCases := map[string]struct {
		localMarkerVersions      []version
		globalDeleteMarkerExists bool
		expectDelete             bool
	}{
		"no local no global": {
			localMarkerVersions:      nil,
			globalDeleteMarkerExists: false,
			expectDelete:             false,
		},
		"no local with global": {
			localMarkerVersions:      nil,
			globalDeleteMarkerExists: true,
			expectDelete:             true,
		},
		"current local no global": {
			localMarkerVersions:      currentLocal,
			globalDeleteMarkerExists: false,
			expectDelete:             true,
		},
		"current local with global": {
			localMarkerVersions:      currentLocal,
			globalDeleteMarkerExists: true,
			expectDelete:             true,
		},
		// no recovery is actually performed for delete markers, but these cases help test the check for if a delete marker exists
		"recoverable local no global": {
			localMarkerVersions:      recoverableLocal,
			globalDeleteMarkerExists: false,
			expectDelete:             false,
		},
		"recoverable local with global": {
			localMarkerVersions:      recoverableLocal,
			globalDeleteMarkerExists: true,
			expectDelete:             true,
		},
		"unrecoverable local no global": {
			localMarkerVersions:      unrecoverableLocal,
			globalDeleteMarkerExists: false,
			expectDelete:             false,
		},
		"unrecoverable local with global": {
			localMarkerVersions:      unrecoverableLocal,
			globalDeleteMarkerExists: true,
			expectDelete:             true,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			globalMarkerState := globalMarkerState{deleteMarkerExists: tc.globalDeleteMarkerExists}
			versions := make(map[string][]version, 1)
			if len(tc.localMarkerVersions) > 0 {
				versions[localMarkerPath] = tc.localMarkerVersions
			}
			deleted, err := handleDeleteMarkers(context.Background(), nil, tenantID, blockID, globalMarkerState, versions, true, nopSlog())
			require.NoError(t, err)
			require.Equal(t, tc.expectDelete, deleted)
		})
	}
}

func TestHandleNoCompactMarker(t *testing.T) {
	tenantID := "tenant"
	blockID, err := ulid.New(0, nil)
	require.NoError(t, err)

	localMarkerPath := tenantID + objtools.Delim + blockID.String() + objtools.Delim + block.NoCompactMarkFilename

	currentLocal := []version{
		{
			info: objtools.VersionInfo{
				VersionID: "1",
				IsCurrent: true,
			},
		},
	}
	recoverableLocal := []version{
		{
			info: objtools.VersionInfo{
				VersionID: "1",
				IsCurrent: false,
			},
		},
	}
	unrecoverableLocal := []version{
		{
			info: objtools.VersionInfo{
				VersionID:      "1",
				IsDeleteMarker: true,
			},
		},
	}

	testCases := map[string]struct {
		localMarkerVersions         []version
		globalNoCompactMarkerExists bool
		expectErr                   bool
		expectWritten               bool
	}{
		"no local no global": {
			localMarkerVersions:         nil,
			globalNoCompactMarkerExists: false,
			expectErr:                   false,
			expectWritten:               false,
		},
		"no local with global": {
			localMarkerVersions:         nil,
			globalNoCompactMarkerExists: true,
			expectErr:                   false,
			expectWritten:               false,
		},
		"current local no global": {
			localMarkerVersions:         currentLocal,
			globalNoCompactMarkerExists: false,
			expectErr:                   false,
			expectWritten:               true,
		},
		"current local with global": {
			localMarkerVersions:         currentLocal,
			globalNoCompactMarkerExists: true,
			expectErr:                   false,
			expectWritten:               false,
		},
		"recoverable local no global": {
			localMarkerVersions:         recoverableLocal,
			globalNoCompactMarkerExists: false,
			expectErr:                   false,
			expectWritten:               true,
		},
		"recoverable local with global": {
			localMarkerVersions:         recoverableLocal,
			globalNoCompactMarkerExists: true,
			expectErr:                   false,
			expectWritten:               true,
		},
		"unrecoverable local no global": {
			localMarkerVersions:         unrecoverableLocal,
			globalNoCompactMarkerExists: false,
			expectErr:                   true,
			expectWritten:               false,
		},
		"unrecoverable local with global": {
			localMarkerVersions:         unrecoverableLocal,
			globalNoCompactMarkerExists: true,
			expectErr:                   true,
			expectWritten:               false,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			globalMarkerState := globalMarkerState{noCompactMarkerExists: tc.globalNoCompactMarkerExists}
			versions := make(map[string][]version, 1)
			if len(tc.localMarkerVersions) > 0 {
				versions[localMarkerPath] = tc.localMarkerVersions
			}
			written, err := handleNoCompactMarker(context.Background(), nil, tenantID, blockID, globalMarkerState, versions, true, nopSlog())
			if tc.expectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.expectWritten, written)
		})
	}
}

func v(t int, isDeleteMarker bool) version {
	return version{
		lastModified: time.Unix(int64(t), 0),
		info: objtools.VersionInfo{
			VersionID:      strconv.FormatInt(int64(t), 10),
			IsDeleteMarker: isDeleteMarker,
		},
	}
}

func TestShadowingDeleteMarkers(t *testing.T) {
	targetTime := 5
	target := v(targetTime, false)

	testCases := map[string]struct {
		versions    []version
		expectStart int
		expectEnd   int
	}{
		"empty": {
			versions: nil,
		},
		"single delete": {
			versions:    []version{target, v(targetTime+1, true)},
			expectStart: 1,
			expectEnd:   2,
		},
		"surrounding delete": {
			versions:    []version{v(targetTime-1, true), target, v(targetTime+1, true)},
			expectStart: 2,
			expectEnd:   3,
		},
		"multiple delete": {
			versions:    []version{target, v(targetTime+1, true), v(targetTime+2, true)},
			expectStart: 1,
			expectEnd:   3,
		},
		"single delete unordered": {
			versions:    []version{v(targetTime+1, true), target},
			expectStart: 0,
			expectEnd:   1,
		},
		"non-delete marker shadow": {
			versions: []version{target, v(targetTime+1, false)},
		},
		"mixed shadow": {
			versions: []version{target, v(targetTime+1, true), v(targetTime+2, false)},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			actual := deleteMarkersShadowingTarget(tc.versions, target)
			if tc.expectStart != tc.expectEnd {
				require.Equal(t, tc.versions[tc.expectStart:tc.expectEnd], actual)
			} else {
				require.Empty(t, actual)
			}
		})
	}
}

func TestBuildRestore(t *testing.T) {
	targetTime := 5
	target := v(targetTime, false)
	versions := []version{target, v(targetTime+1, true)}

	for _, allowVersionDelete := range []bool{false, true} {
		restoreInfo := buildRestore(
			versions,
			target,
			allowVersionDelete,
		)
		require.Equal(t, restoreInfo.target, target)
		if allowVersionDelete {
			require.Equal(t, versions[1:2], restoreInfo.shadowingDeleteMarkers)
		} else {
			require.Empty(t, restoreInfo.shadowingDeleteMarkers)
		}
	}
}

func TestRestoreDryRun(t *testing.T) {
	r := restoreInfo{
		shadowingDeleteMarkers: []version{v(0, true)},
		target:                 v(0, false),
	}

	// Passing a nil bucket as a trap. A dry run should not touch it
	require.NoError(t, restore(context.Background(), nil, r, nopSlog(), true))
	r.shadowingDeleteMarkers = nil
	require.NoError(t, restore(context.Background(), nil, r, nopSlog(), true))
}

func nopSlog() *slog.Logger {
	// slog.DiscardHandler can be used in go 1.24: https://github.com/golang/go/issues/62005
	return slog.New(slog.NewJSONHandler(io.Discard, nil))
}
