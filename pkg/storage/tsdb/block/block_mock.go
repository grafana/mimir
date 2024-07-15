// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/storage/tsdb/testutil/block_mock.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package block

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
)

func MockStorageBlock(t testing.TB, bucket objstore.Bucket, userID string, minT, maxT int64) tsdb.BlockMeta {
	m := MockStorageBlockWithExtLabels(t, bucket, userID, minT, maxT, nil)
	return m.BlockMeta
}

func MockStorageBlockWithExtLabels(t testing.TB, bucket objstore.Bucket, userID string, minT, maxT int64, externalLabels map[string]string) Meta {
	// Generate a block ID whose timestamp matches the maxT (for simplicity we assume it
	// has been compacted and shipped in zero time, even if not realistic).
	id := ulid.MustNew(uint64(maxT), rand.Reader)

	meta := Meta{
		BlockMeta: tsdb.BlockMeta{
			Version: 1,
			ULID:    id,
			MinTime: minT,
			MaxTime: maxT,
			Compaction: tsdb.BlockMetaCompaction{
				Level:   1,
				Sources: []ulid.ULID{id},
			},
		},
		Thanos: ThanosMeta{
			Labels: externalLabels,
			Source: SourceType("test"),
		},
	}

	metaContent, err := json.Marshal(meta)
	require.NoError(t, err, "failed to marshal mocked block meta")

	metaContentReader := strings.NewReader(string(metaContent))
	metaPath := fmt.Sprintf("%s/%s/meta.json", userID, id.String())
	require.NoError(t, bucket.Upload(context.Background(), metaPath, metaContentReader))

	// Upload an empty index, just to make sure the meta.json is not the only object in the block location.
	indexPath := fmt.Sprintf("%s/%s/index", userID, id.String())
	require.NoError(t, bucket.Upload(context.Background(), indexPath, strings.NewReader("")))

	return meta
}

func MockStorageDeletionMark(t testing.TB, bucket objstore.Bucket, userID string, meta tsdb.BlockMeta) *DeletionMark {
	mark := DeletionMark{
		ID:           meta.ULID,
		DeletionTime: time.Now().Add(-time.Minute).Unix(),
		Version:      DeletionMarkVersion1,
	}

	markContent, err := json.Marshal(mark)
	require.NoError(t, err, "failed to marshal mocked deletion mark")

	markContentReader := strings.NewReader(string(markContent))
	markPath := fmt.Sprintf("%s/%s/%s", userID, meta.ULID.String(), DeletionMarkFilename)
	require.NoError(t, bucket.Upload(context.Background(), markPath, markContentReader))

	return &mark
}

func MockNoCompactMark(t testing.TB, bucket objstore.Bucket, userID string, meta tsdb.BlockMeta) *NoCompactMark {
	mark := NoCompactMark{
		ID:            meta.ULID,
		NoCompactTime: time.Now().Unix(),
		Version:       DeletionMarkVersion1,
		Details:       "details",
		Reason:        ManualNoCompactReason,
	}

	markContent, err := json.Marshal(mark)
	require.NoError(t, err, "failed to marshal mocked no-compact mark")

	markContentReader := strings.NewReader(string(markContent))
	markPath := fmt.Sprintf("%s/%s/%s", userID, meta.ULID.String(), NoCompactMarkFilename)
	require.NoError(t, bucket.Upload(context.Background(), markPath, markContentReader))

	return &mark
}
