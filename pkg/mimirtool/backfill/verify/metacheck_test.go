// SPDX-License-Identifier: AGPL-3.0-only

package verify

import (
	"context"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
)

func newMetaForMetaCheck() block.Meta {
	id := ulid.MustNew(ulid.Now(), nil)
	return block.Meta{
		BlockMeta: tsdb.BlockMeta{
			ULID:    id,
			MinTime: 1_000,
			MaxTime: 2_000,
			Version: block.TSDBVersion1,
		},
		Thanos: block.ThanosMeta{
			Files: []block.File{
				{RelPath: "index", SizeBytes: 1},
				{RelPath: "chunks/000001", SizeBytes: 1},
			},
		},
	}
}

func TestMetaCheckVerifier_ValidBlock(t *testing.T) {
	v := NewMetaCheckVerifier(log.NewNopLogger())
	require.NoError(t, v.Verify(context.Background(), "", newMetaForMetaCheck()))
}

func TestMetaCheckVerifier_Name(t *testing.T) {
	require.Equal(t, "meta-check", NewMetaCheckVerifier(log.NewNopLogger()).Name())
}

func TestMetaCheckVerifier_RejectsDownsampled(t *testing.T) {
	m := newMetaForMetaCheck()
	m.Thanos.Downsample.Resolution = 300_000

	err := NewMetaCheckVerifier(log.NewNopLogger()).Verify(context.Background(), "", m)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "downsampled")
}

func TestMetaCheckVerifier_RejectsUnknownLabel(t *testing.T) {
	m := newMetaForMetaCheck()
	m.Thanos.Labels = map[string]string{"custom_label": "value"}

	err := NewMetaCheckVerifier(log.NewNopLogger()).Verify(context.Background(), "", m)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported external label: custom_label")
}

func TestMetaCheckVerifier_RejectsBadFilePath(t *testing.T) {
	m := newMetaForMetaCheck()
	m.Thanos.Files = []block.File{{RelPath: "weird/path", SizeBytes: 1}}

	err := NewMetaCheckVerifier(log.NewNopLogger()).Verify(context.Background(), "", m)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "file with invalid path")
}

func TestMetaCheckVerifier_RejectsFutureTime(t *testing.T) {
	m := newMetaForMetaCheck()
	m.MaxTime = time.Now().Add(time.Hour).UnixMilli()

	err := NewMetaCheckVerifier(log.NewNopLogger()).Verify(context.Background(), "", m)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "greater than the present")
}
