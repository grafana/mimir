// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"path"
	"strings"
	"testing"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/concurrency"
	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/bucket/filesystem"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/storage/tsdb/metadata"
)

func TestConvertTenantBlocks(t *testing.T) {
	dir := t.TempDir()
	bkt, err := filesystem.NewBucketClient(filesystem.Config{Directory: dir})
	require.NoError(t, err)

	ctx := context.Background()

	blockWithNoLabelsButManyOtherFields := ulid.MustNew(1, nil)
	blockWithWrongTenant := ulid.MustNew(2, nil)
	blockWithManyMimirLabels := ulid.MustNew(3, nil)
	blockWithNoChangesRequired := ulid.MustNew(4, nil)

	const tenant = "target_tenant"

	inputMetas := map[ulid.ULID]metadata.Meta{
		blockWithNoLabelsButManyOtherFields: {
			BlockMeta: tsdb.BlockMeta{
				ULID: blockWithNoLabelsButManyOtherFields,

				MinTime: 100,
				MaxTime: 200,
				Version: 1,

				Compaction: tsdb.BlockMetaCompaction{
					Level:   5,
					Sources: []ulid.ULID{blockWithNoLabelsButManyOtherFields},
				},
			},

			Thanos: metadata.Thanos{
				Version: 10,
				Downsample: metadata.ThanosDownsample{
					Resolution: 15,
				},
				Source: "ingester",
			},
		},

		blockWithWrongTenant: {
			BlockMeta: tsdb.BlockMeta{
				ULID: blockWithWrongTenant,
			},

			Thanos: metadata.Thanos{
				Labels: map[string]string{
					"test":       "label",
					"__org_id__": "wrong tenant",
				},
			},
		},

		blockWithManyMimirLabels: {
			BlockMeta: tsdb.BlockMeta{
				ULID: blockWithManyMimirLabels,
			},

			Thanos: metadata.Thanos{
				Labels: map[string]string{
					"__org_id__":                             "fake",
					mimir_tsdb.CompactorShardIDExternalLabel: "1_of_10",
					"__ingester_id__":                        "ingester-1",
				},
			},
		},

		blockWithNoChangesRequired: {
			BlockMeta: tsdb.BlockMeta{
				ULID: blockWithNoChangesRequired,
			},

			Thanos: metadata.Thanos{
				Labels: map[string]string{
					"__org_id__": tenant,
				},
			},
		},
	}

	for b, m := range inputMetas {
		require.NoError(t, uploadMetadata(ctx, bkt, m, path.Join(b.String(), metadata.MetaFilename)))
	}

	logs := &concurrency.SyncBuffer{}
	logger := log.NewLogfmtLogger(logs)

	// Run conversion
	assert.NoError(t, convertTenantBlocks(ctx, bkt, tenant, false, logger))

	expected := map[ulid.ULID]metadata.Meta{
		blockWithNoLabelsButManyOtherFields: {
			BlockMeta: tsdb.BlockMeta{
				ULID: blockWithNoLabelsButManyOtherFields,

				MinTime: 100,
				MaxTime: 200,
				Version: 1,

				Compaction: tsdb.BlockMetaCompaction{
					Level:   5,
					Sources: []ulid.ULID{blockWithNoLabelsButManyOtherFields},
				},
			},

			Thanos: metadata.Thanos{
				Version: 10,
				Downsample: metadata.ThanosDownsample{
					Resolution: 15,
				},
				Source: "ingester",
			},
		},

		blockWithWrongTenant: {
			BlockMeta: tsdb.BlockMeta{
				ULID: blockWithWrongTenant,
			},
		},

		blockWithManyMimirLabels: {
			BlockMeta: tsdb.BlockMeta{
				ULID: blockWithManyMimirLabels,
			},

			Thanos: metadata.Thanos{
				Labels: map[string]string{
					mimir_tsdb.CompactorShardIDExternalLabel: "1_of_10",
				},
			},
		},

		blockWithNoChangesRequired: {
			BlockMeta: tsdb.BlockMeta{
				ULID: blockWithNoChangesRequired,
			},
		},
	}

	for b, m := range expected {
		meta, err := block.DownloadMeta(ctx, logger, bkt, b)
		require.NoError(t, err, b.String())

		// Normalize empty map to nil to simplify tests.
		if len(meta.Thanos.Labels) == 0 {
			meta.Thanos.Labels = nil
		}
		require.Equal(t, m, meta, b.String())
	}

	assert.Equal(t, []string{
		`level=info tenant=target_tenant msg="no changes required" block=00000000010000000000000000`,
		`level=warn tenant=target_tenant msg="removing unknown label" block=00000000020000000000000000 label=__org_id__ value="wrong tenant"`,
		`level=warn tenant=target_tenant msg="removing unknown label" block=00000000020000000000000000 label=test value=label`,
		`level=info tenant=target_tenant msg="changes required, uploading meta.json file" block=00000000020000000000000000`,
		`level=info tenant=target_tenant msg="meta.json file uploaded successfully" block=00000000020000000000000000`,
		`level=warn tenant=target_tenant msg="removing unknown label" block=00000000030000000000000000 label=__ingester_id__ value=ingester-1`,
		`level=warn tenant=target_tenant msg="removing unknown label" block=00000000030000000000000000 label=__org_id__ value=fake`,
		`level=info tenant=target_tenant msg="changes required, uploading meta.json file" block=00000000030000000000000000`,
		`level=info tenant=target_tenant msg="meta.json file uploaded successfully" block=00000000030000000000000000`,
		`level=warn tenant=target_tenant msg="removing unknown label" block=00000000040000000000000000 label=__org_id__ value=target_tenant`,
		`level=info tenant=target_tenant msg="changes required, uploading meta.json file" block=00000000040000000000000000`,
		`level=info tenant=target_tenant msg="meta.json file uploaded successfully" block=00000000040000000000000000`,
	}, strings.Split(strings.TrimSpace(logs.String()), "\n"))
}

func TestConvertTenantBlocksDryMode(t *testing.T) {
	dir := t.TempDir()
	bkt, err := filesystem.NewBucketClient(filesystem.Config{Directory: dir})
	require.NoError(t, err)

	ctx := context.Background()

	blockWithNoLabelsButManyOtherFields := ulid.MustNew(1, nil)
	blockWithWrongTenant := ulid.MustNew(2, nil)
	blockWithManyMimirLabels := ulid.MustNew(3, nil)
	blockWithNoChangesRequired := ulid.MustNew(4, nil)

	const tenant = "target_tenant"

	inputMetas := map[ulid.ULID]metadata.Meta{
		blockWithNoLabelsButManyOtherFields: {
			BlockMeta: tsdb.BlockMeta{
				ULID: blockWithNoLabelsButManyOtherFields,

				MinTime: 100,
				MaxTime: 200,
				Version: 1,

				Compaction: tsdb.BlockMetaCompaction{
					Level:   5,
					Sources: []ulid.ULID{blockWithNoLabelsButManyOtherFields},
				},
			},

			Thanos: metadata.Thanos{
				Version: 10,
				Downsample: metadata.ThanosDownsample{
					Resolution: 15,
				},
				Source: "ingester",
			},
		},

		blockWithWrongTenant: {
			BlockMeta: tsdb.BlockMeta{
				ULID: blockWithWrongTenant,
			},

			Thanos: metadata.Thanos{
				Labels: map[string]string{
					"test":       "label",
					"__org_id__": "wrong tenant",
				},
			},
		},

		blockWithManyMimirLabels: {
			BlockMeta: tsdb.BlockMeta{
				ULID: blockWithManyMimirLabels,
			},

			Thanos: metadata.Thanos{
				Labels: map[string]string{
					"__org_id__":                             "fake",
					mimir_tsdb.CompactorShardIDExternalLabel: "1_of_10",
					"__ingester_id__":                        "ingester-1",
				},
			},
		},

		blockWithNoChangesRequired: {
			BlockMeta: tsdb.BlockMeta{
				ULID: blockWithNoChangesRequired,
			},

			Thanos: metadata.Thanos{
				Labels: map[string]string{
					"__org_id__": tenant,
				},
			},
		},
	}

	for b, m := range inputMetas {
		require.NoError(t, uploadMetadata(ctx, bkt, m, path.Join(b.String(), metadata.MetaFilename)))
	}

	logs := &concurrency.SyncBuffer{}
	logger := log.NewLogfmtLogger(logs)

	// Run conversion
	assert.NoError(t, convertTenantBlocks(ctx, bkt, tenant, true, logger))

	for b, m := range inputMetas {
		meta, err := block.DownloadMeta(ctx, logger, bkt, b)
		require.NoError(t, err)
		require.Equal(t, m, meta)
	}

	assert.Equal(t, []string{
		`level=info tenant=target_tenant msg="no changes required" block=00000000010000000000000000`,
		`level=warn tenant=target_tenant msg="removing unknown label" block=00000000020000000000000000 label=__org_id__ value="wrong tenant"`,
		`level=warn tenant=target_tenant msg="removing unknown label" block=00000000020000000000000000 label=test value=label`,
		`level=warn tenant=target_tenant msg="changes required, not uploading back due to dry run" block=00000000020000000000000000`,
		`level=warn tenant=target_tenant msg="removing unknown label" block=00000000030000000000000000 label=__ingester_id__ value=ingester-1`,
		`level=warn tenant=target_tenant msg="removing unknown label" block=00000000030000000000000000 label=__org_id__ value=fake`,
		`level=warn tenant=target_tenant msg="changes required, not uploading back due to dry run" block=00000000030000000000000000`,
		`level=warn tenant=target_tenant msg="removing unknown label" block=00000000040000000000000000 label=__org_id__ value=target_tenant`,
		`level=warn tenant=target_tenant msg="changes required, not uploading back due to dry run" block=00000000040000000000000000`,
	}, strings.Split(strings.TrimSpace(logs.String()), "\n"))
}
