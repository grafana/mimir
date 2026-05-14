// SPDX-License-Identifier: AGPL-3.0-only

package blockvalidation

import (
	crypto_rand "crypto/rand"
	"fmt"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
)

// newULID returns a fresh ULID with cryptographic entropy so tests that compare
// two successive ULIDs always see distinct values.
func newULID() ulid.ULID {
	return ulid.MustNew(ulid.Now(), crypto_rand.Reader)
}

func newValidMeta() *block.Meta {
	id := newULID()
	return &block.Meta{
		BlockMeta: tsdb.BlockMeta{
			ULID:    id,
			MinTime: 1_000,
			MaxTime: 2_000,
			Version: block.TSDBVersion1,
		},
		Thanos: block.ThanosMeta{
			Labels: map[string]string{},
			Files: []block.File{
				{RelPath: "index", SizeBytes: 100},
				{RelPath: "chunks/000001", SizeBytes: 200},
				{RelPath: block.MetaFilename}, // meta entry is exempt from size constraints
			},
		},
	}
}

func TestCheckMeta_NilMeta(t *testing.T) {
	err := CheckMeta(nil, CheckMetaOptions{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing block metadata")
}

func TestCheckMeta_ValidBlock(t *testing.T) {
	require.NoError(t, CheckMeta(newValidMeta(), CheckMetaOptions{}))
}

func TestCheckMeta_Downsampled(t *testing.T) {
	m := newValidMeta()
	m.Thanos.Downsample.Resolution = 300_000
	err := CheckMeta(m, CheckMetaOptions{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "downsampled")
}

func TestCheckMeta_ExternalLabels(t *testing.T) {
	tests := map[string]struct {
		labels    map[string]string
		expectErr string
	}{
		"valid compactor shard label": {
			labels: map[string]string{block.CompactorShardIDExternalLabel: "1_of_4"},
		},
		"empty compactor shard label is accepted": {
			labels: map[string]string{block.CompactorShardIDExternalLabel: ""},
		},
		"invalid compactor shard label": {
			labels:    map[string]string{block.CompactorShardIDExternalLabel: "nope"},
			expectErr: "invalid " + block.CompactorShardIDExternalLabel,
		},
		"deprecated labels are accepted": {
			labels: map[string]string{
				block.DeprecatedTenantIDExternalLabel:   "anything",
				block.DeprecatedIngesterIDExternalLabel: "anything",
				block.DeprecatedShardIDExternalLabel:    "anything",
			},
		},
		"unknown label is rejected": {
			labels:    map[string]string{"custom_label": "value"},
			expectErr: "unsupported external label: custom_label",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			m := newValidMeta()
			m.Thanos.Labels = tc.labels
			err := CheckMeta(m, CheckMetaOptions{})
			if tc.expectErr == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.expectErr)
		})
	}
}

func TestCheckMeta_Files(t *testing.T) {
	tests := map[string]struct {
		files     []block.File
		expectErr string
	}{
		"valid file paths": {
			files: []block.File{
				{RelPath: "index", SizeBytes: 10},
				{RelPath: "chunks/000001", SizeBytes: 10},
				{RelPath: "chunks/000999", SizeBytes: 10},
			},
		},
		"invalid file path": {
			files: []block.File{
				{RelPath: "index", SizeBytes: 10},
				{RelPath: "chunks/abc", SizeBytes: 10},
			},
			expectErr: "file with invalid path: chunks/abc",
		},
		"zero file size": {
			files: []block.File{
				{RelPath: "index", SizeBytes: 0},
			},
			expectErr: "file with invalid size: index",
		},
		"negative file size": {
			files: []block.File{
				{RelPath: "index", SizeBytes: -1},
			},
			expectErr: "file with invalid size: index",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			m := newValidMeta()
			m.Thanos.Files = tc.files
			err := CheckMeta(m, CheckMetaOptions{})
			if tc.expectErr == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.expectErr)
		})
	}
}

func TestCheckMeta_MaxBlockSize(t *testing.T) {
	m := newValidMeta()
	// total declared size in newValidMeta() is 300 bytes (index 100 + chunks 200).

	require.NoError(t, CheckMeta(m, CheckMetaOptions{MaxBlockSizeBytes: 0}))
	require.NoError(t, CheckMeta(m, CheckMetaOptions{MaxBlockSizeBytes: 300}))

	err := CheckMeta(m, CheckMetaOptions{MaxBlockSizeBytes: 299})
	require.Error(t, err)
	assert.Equal(t, fmt.Sprintf(MaxBlockSizeBytesFormat, int64(299)), err.Error())
}

// TestCheckMaxBlockSize exercises the size helper directly so we can
// cover negative-size and overflow paths that are otherwise short-circuited
// by CheckMeta's earlier file checks. The compactor previously owned an
// equivalent TestMultitenantCompactor_ValidateMaximumBlockSize test which
// became redundant once the helper moved here.
func TestCheckMaxBlockSize(t *testing.T) {
	const maxInt64 = int64(1<<63 - 1)

	tests := map[string]struct {
		maxBlockSizeBytes int64
		files             []block.File
		expectErr         bool
	}{
		"no limit": {
			maxBlockSizeBytes: 0,
			files:             []block.File{{SizeBytes: maxInt64}},
			expectErr:         false,
		},
		"under limit": {
			maxBlockSizeBytes: 4,
			files:             []block.File{{SizeBytes: 1}, {SizeBytes: 2}},
			expectErr:         false,
		},
		"exact limit": {
			maxBlockSizeBytes: 3,
			files:             []block.File{{SizeBytes: 1}, {SizeBytes: 2}},
			expectErr:         false,
		},
		"under limit with zero-size file": {
			maxBlockSizeBytes: 2,
			files:             []block.File{{SizeBytes: 1}, {SizeBytes: 0}},
			expectErr:         false,
		},
		"over limit": {
			maxBlockSizeBytes: 1,
			files:             []block.File{{SizeBytes: 1}, {SizeBytes: 1}},
			expectErr:         true,
		},
		"negative file size": {
			maxBlockSizeBytes: 2,
			files:             []block.File{{SizeBytes: 2}, {SizeBytes: -1}},
			expectErr:         true,
		},
		"overflow": {
			maxBlockSizeBytes: maxInt64,
			files:             []block.File{{SizeBytes: maxInt64}, {SizeBytes: maxInt64}, {SizeBytes: maxInt64}},
			expectErr:         true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			err := CheckMaxBlockSize(tc.files, tc.maxBlockSizeBytes)
			if tc.expectErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestCheckMeta_Version(t *testing.T) {
	m := newValidMeta()
	m.Version = 99
	err := CheckMeta(m, CheckMetaOptions{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "version must be")
}

func TestCheckMeta_TimeRange(t *testing.T) {
	tests := map[string]struct {
		minTime, maxTime int64
		expectErr        string
	}{
		"negative minTime": {minTime: -1, maxTime: 10, expectErr: "invalid minTime/maxTime"},
		"negative maxTime": {minTime: 0, maxTime: -1, expectErr: "invalid minTime/maxTime"},
		"max before min":   {minTime: 10, maxTime: 5, expectErr: "invalid minTime/maxTime"},
		"min in future":    {minTime: time.Now().Add(time.Hour).UnixMilli(), maxTime: time.Now().Add(2 * time.Hour).UnixMilli(), expectErr: "greater than the present"},
		"max in future":    {minTime: 0, maxTime: time.Now().Add(time.Hour).UnixMilli(), expectErr: "greater than the present"},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			m := newValidMeta()
			m.MinTime = tc.minTime
			m.MaxTime = tc.maxTime
			err := CheckMeta(m, CheckMetaOptions{})
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.expectErr)
		})
	}
}

func TestSanitizeForUpload_AssignsULID(t *testing.T) {
	m := newValidMeta()
	original := m.ULID
	newID := newULID()
	require.NotEqual(t, original, newID)

	SanitizeForUpload(log.NewNopLogger(), m, newID)

	assert.Equal(t, newID, m.ULID)
}

func TestSanitizeForUpload_StripsDeprecatedAndEmptyLabels(t *testing.T) {
	m := newValidMeta()
	m.Thanos.Labels = map[string]string{
		block.CompactorShardIDExternalLabel:     "",
		block.DeprecatedTenantIDExternalLabel:   "x",
		block.DeprecatedIngesterIDExternalLabel: "y",
		block.DeprecatedShardIDExternalLabel:    "z",
	}

	SanitizeForUpload(log.NewNopLogger(), m, newULID())

	assert.Empty(t, m.Thanos.Labels)
}

func TestSanitizeForUpload_PreservesValidShardLabel(t *testing.T) {
	m := newValidMeta()
	m.Thanos.Labels = map[string]string{block.CompactorShardIDExternalLabel: "1_of_4"}

	SanitizeForUpload(log.NewNopLogger(), m, newULID())

	assert.Equal(t, "1_of_4", m.Thanos.Labels[block.CompactorShardIDExternalLabel])
}

func TestSanitizeForUpload_ResetsCompactionAndSource(t *testing.T) {
	m := newValidMeta()
	m.Compaction.Parents = []tsdb.BlockDesc{{ULID: newULID()}}
	m.Compaction.Sources = []ulid.ULID{newULID()}
	m.Thanos.Source = "not-upload"

	newID := newULID()
	SanitizeForUpload(log.NewNopLogger(), m, newID)

	assert.Nil(t, m.Compaction.Parents)
	require.Len(t, m.Compaction.Sources, 1)
	assert.Equal(t, newID, m.Compaction.Sources[0])
	assert.EqualValues(t, "upload", m.Thanos.Source)
}

func TestSanitizeForUpload_NilMeta(t *testing.T) {
	require.NotPanics(t, func() {
		SanitizeForUpload(log.NewNopLogger(), nil, newULID())
	})
}
