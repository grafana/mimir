// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/gorilla/mux"
	"github.com/grafana/dskit/flagext"
	"github.com/oklog/ulid/v2"
	prom_tsdb "github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
	"github.com/grafana/mimir/pkg/storage/tsdb/testutil"
	"github.com/grafana/mimir/pkg/util/validation"
)

const blocksPageTestTenant = "user-1"

type blocksPageTestBlock struct {
	minTime         time.Time
	maxTime         time.Time
	compactionLevel int
	numSeries       uint64
	numSamples      uint64
	numChunks       uint64
	sizeBytes       int64
	labels          map[string]string
	deleted         bool
	noCompact       bool
}

type blocksPageTestResponse struct {
	Metas []struct {
		ULID        string `json:"ulid"`
		DeletedTime *int64 `json:"deletedTime"`
		SplitID     *int   `json:"splitId"`
	} `json:"metas"`
	Source        string `json:"source"`
	TotalBlocks   int    `json:"total_blocks"`
	MatchedBlocks int    `json:"matched_blocks"`
	BlocksRead    int    `json:"blocks_read"`
	Page          int    `json:"page"`
	PageSize      int    `json:"page_size"`
	TotalPages    int    `json:"total_pages"`
}

func (r blocksPageTestResponse) blockIDs() []string {
	ids := make([]string, 0, len(r.Metas))
	for _, m := range r.Metas {
		ids = append(ids, m.ULID)
	}
	return ids
}

func TestStoreGateway_BlocksHandler(t *testing.T) {
	baseTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	blockTime := func(hours int) time.Time { return baseTime.Add(time.Duration(hours) * time.Hour) }
	rfc3339 := func(t time.Time) string { return url.QueryEscape(t.Format(time.RFC3339)) }

	gateway, ids := prepareStoreGatewayForBlocksPage(t, []blocksPageTestBlock{
		{
			minTime: blockTime(0), maxTime: blockTime(2),
			compactionLevel: 1, numSeries: 10, numSamples: 1000, numChunks: 100, sizeBytes: 4096,
		},
		{
			minTime: blockTime(2), maxTime: blockTime(4),
			compactionLevel: 2, numSeries: 20, numSamples: 2000, numChunks: 200, sizeBytes: 2048,
			labels: map[string]string{block.CompactorShardIDExternalLabel: "1_of_4"},
		},
		{
			minTime: blockTime(4), maxTime: blockTime(6),
			compactionLevel: 3, numSeries: 30, numSamples: 3000, numChunks: 300, sizeBytes: 1024,
			labels:  map[string]string{block.OutOfOrderExternalLabel: block.OutOfOrderExternalLabelValue},
			deleted: true,
		},
		{
			minTime: blockTime(6), maxTime: blockTime(8),
			compactionLevel: 1, numSeries: 40, numSamples: 4000, numChunks: 400, sizeBytes: 8192,
			noCompact: true,
		},
	})
	first, second, deleted, noCompact := ids[0], ids[1], ids[2], ids[3]

	t.Run("filters", func(t *testing.T) {
		tests := map[string]struct {
			query            string
			expectedBlockIDs []string
		}{
			"no filter hides blocks marked for deletion": {
				query:            "",
				expectedBlockIDs: []string{first, second, noCompact},
			},
			"show_deleted includes blocks marked for deletion": {
				query:            "show_deleted=on",
				expectedBlockIDs: []string{first, second, deleted, noCompact},
			},
			"min_compaction_level": {
				query:            "show_deleted=on&min_compaction_level=2",
				expectedBlockIDs: []string{second, deleted},
			},
			"max_compaction_level": {
				query:            "show_deleted=on&max_compaction_level=1",
				expectedBlockIDs: []string{first, noCompact},
			},
			"block_id is not case sensitive": {
				query:            "block_id=" + strings.ToLower(second),
				expectedBlockIDs: []string{second},
			},
			"block_id does not match a prefix": {
				query:            "block_id=" + second[:10],
				expectedBlockIDs: []string{},
			},
			"min_time keeps blocks starting at or after it": {
				query:            "min_time=" + rfc3339(blockTime(2)),
				expectedBlockIDs: []string{second, noCompact},
			},
			"max_time keeps blocks ending at or before it": {
				query:            "max_time=" + rfc3339(blockTime(4)),
				expectedBlockIDs: []string{first, second},
			},
			"created_after keeps blocks whose ULID time is at or after it": {
				query:            "created_after=" + rfc3339(blockTime(4)),
				expectedBlockIDs: []string{second, noCompact},
			},
			"created_before keeps blocks whose ULID time is at or before it": {
				query:            "created_before=" + rfc3339(blockTime(4)),
				expectedBlockIDs: []string{first, second},
			},
			"filters are combined": {
				query:            "show_deleted=on&min_compaction_level=2&max_time=" + rfc3339(blockTime(4)),
				expectedBlockIDs: []string{second},
			},
		}

		for name, testData := range tests {
			t.Run(name, func(t *testing.T) {
				res := requestBlocksPageJSON(t, gateway, testData.query)
				assert.Equal(t, testData.expectedBlockIDs, res.blockIDs())
			})
		}
	})

	t.Run("pagination", func(t *testing.T) {
		tests := map[string]struct {
			query              string
			expectedBlockIDs   []string
			expectedPage       int
			expectedTotalPages int
		}{
			"first page": {
				query:              "page_size=2",
				expectedBlockIDs:   []string{first, second},
				expectedPage:       1,
				expectedTotalPages: 2,
			},
			"last page": {
				query:              "page_size=2&page=2",
				expectedBlockIDs:   []string{noCompact},
				expectedPage:       2,
				expectedTotalPages: 2,
			},
			"a page after the last one is clamped": {
				query:              "page_size=2&page=99",
				expectedBlockIDs:   []string{noCompact},
				expectedPage:       2,
				expectedTotalPages: 2,
			},
			"page size zero disables pagination": {
				query:              "page_size=0&page=3",
				expectedBlockIDs:   []string{first, second, noCompact},
				expectedPage:       1,
				expectedTotalPages: 1,
			},
			"pagination applies after filtering": {
				query:              "min_compaction_level=2&page_size=2",
				expectedBlockIDs:   []string{second},
				expectedPage:       1,
				expectedTotalPages: 1,
			},
		}

		for name, testData := range tests {
			t.Run(name, func(t *testing.T) {
				res := requestBlocksPageJSON(t, gateway, testData.query)
				assert.Equal(t, testData.expectedBlockIDs, res.blockIDs())
				assert.Equal(t, testData.expectedPage, res.Page)
				assert.Equal(t, testData.expectedTotalPages, res.TotalPages)
			})
		}
	})

	t.Run("split_count adds the split ID", func(t *testing.T) {
		res := requestBlocksPageJSON(t, gateway, "split_count=4")
		require.Len(t, res.Metas, 3)
		for _, m := range res.Metas {
			require.NotNil(t, m.SplitID)
			assert.Less(t, *m.SplitID, 4)
		}
	})

	t.Run("a JSON request is not paginated and reads every block, as it did before the bucket index", func(t *testing.T) {
		res := requestBlocksPageJSON(t, gateway, "")
		assert.Equal(t, blocksSourceBucketScan, res.Source)
		assert.Equal(t, 0, res.PageSize)
		assert.Equal(t, 3, res.BlocksRead)
		assert.Len(t, res.Metas, 3)
	})

	t.Run("the number of block meta files read", func(t *testing.T) {
		res := requestBlocksPageJSON(t, gateway, "scan_bucket=off")
		assert.Equal(t, blocksSourceBucketIndex, res.Source)
		assert.Equal(t, 0, res.BlocksRead)
		assert.Equal(t, []string{first, second, noCompact}, res.blockIDs())

		res = requestBlocksPageJSON(t, gateway, "scan_bucket=off&show_details=on&page_size=2")
		assert.Equal(t, 2, res.BlocksRead)

		res = requestBlocksPageJSON(t, gateway, "scan_bucket=off&show_sources=on")
		assert.Equal(t, 3, res.BlocksRead)

		res = requestBlocksPageJSON(t, gateway, "scan_bucket=on&created_after="+rfc3339(blockTime(6)))
		assert.Equal(t, []string{noCompact}, res.blockIDs())
		assert.Equal(t, 1, res.BlocksRead)
	})

	t.Run("the detail read is capped so it can never read every block", func(t *testing.T) {
		res := requestBlocksPageJSON(t, gateway, "scan_bucket=off&show_details=on&page_size=0")
		assert.Equal(t, blocksPageMaxDetailBlocks, res.PageSize)
	})

	t.Run("the blocks hidden by the deletion marks are counted", func(t *testing.T) {
		res := requestBlocksPageJSON(t, gateway, "scan_bucket=off")
		assert.Equal(t, 4, res.TotalBlocks)
		assert.Equal(t, 3, res.MatchedBlocks)

		assert.Contains(t, requestBlocksPage(t, gateway, "", "").Body.String(), "4 blocks before the filters, including 1 marked for deletion.")
	})

	t.Run("filters give the same blocks from both sources", func(t *testing.T) {
		for _, query := range []string{
			"",
			"show_deleted=on",
			"min_time=" + rfc3339(blockTime(2)),
			"min_compaction_level=2",
		} {
			t.Run(query, func(t *testing.T) {
				fromIndex := requestBlocksPageJSON(t, gateway, query+"&scan_bucket=off")
				fromScan := requestBlocksPageJSON(t, gateway, query+"&scan_bucket=on")

				require.Equal(t, blocksSourceBucketIndex, fromIndex.Source)
				require.Equal(t, blocksSourceBucketScan, fromScan.Source)
				assert.Equal(t, fromScan.blockIDs(), fromIndex.blockIDs())
			})
		}
	})

	t.Run("invalid query parameters", func(t *testing.T) {
		for _, query := range []string{
			"page_size=all",
			"min_compaction_level=low",
			"min_time=yesterday",
			"created_after=now-banana",
		} {
			t.Run(query, func(t *testing.T) {
				recorder := requestBlocksPage(t, gateway, query, "")
				assert.Equal(t, http.StatusBadRequest, recorder.Code)
			})
		}
	})

	t.Run("HTML rendering", func(t *testing.T) {
		body := requestBlocksPage(t, gateway, "", "").Body.String()
		assert.Contains(t, body, "Showing blocks for tenant: <strong>"+blocksPageTestTenant+"</strong>")
		assert.Contains(t, body, "From the bucket index, written")
		assert.Contains(t, body, first)
		assert.NotContains(t, body, deleted)
		assert.Contains(t, body, ">Series<")
		assert.NotContains(t, body, ">Size<")

		body = requestBlocksPage(t, gateway, "show_details=on&show_no_compact=on", "").Body.String()
		assert.Contains(t, body, ">Size<")
		assert.Contains(t, body, "No Compact")

		body = requestBlocksPage(t, gateway, "page_size=2&created_before=now", "").Body.String()
		assert.Contains(t, body, "Page 1 of 2")
		assert.NotContains(t, body, noCompact)
		assert.Contains(t, body, `value="now"`)
		assert.Contains(t, body, "created_before=now&amp;page=2&amp;page_size=2")
	})

	t.Run("an empty tenant is rejected", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/store-gateway/tenant//blocks", nil)
		recorder := httptest.NewRecorder()
		gateway.BlocksHandler(recorder, req.WithContext(t.Context()))

		assert.Contains(t, recorder.Body.String(), "Tenant ID can't be empty")
	})
}

func TestStoreGateway_BlocksHandler_WithoutBucketIndex(t *testing.T) {
	baseTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	gateway, ids := prepareStoreGatewayForBlocksPageWithBucketIndex(t, []blocksPageTestBlock{
		{minTime: baseTime, maxTime: baseTime.Add(2 * time.Hour), compactionLevel: 1, numSeries: 10, sizeBytes: 4096},
		{minTime: baseTime.Add(2 * time.Hour), maxTime: baseTime.Add(4 * time.Hour), compactionLevel: 2, numSeries: 20, sizeBytes: 2048},
	}, false)

	res := requestBlocksPageJSON(t, gateway, "scan_bucket=off")
	assert.Equal(t, blocksSourceBucketScan, res.Source)
	assert.Equal(t, ids, res.blockIDs())
	assert.Equal(t, 2, res.BlocksRead)

	body := requestBlocksPage(t, gateway, "", "").Body.String()
	assert.Contains(t, body, "From a live read of every block")
	assert.Contains(t, body, ">Size<")
	assert.Contains(t, body, "4.0 KiB")
}

func TestParseBlocksPageOptions(t *testing.T) {
	now := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)

	opts, err := parseBlocksPageOptions(url.Values{
		"page":                 []string{"-3"},
		"page_size":            []string{"-10"},
		"split_count":          []string{"-1"},
		"min_compaction_level": []string{"-2"},
	}, now, blocksPageDefaultPageSize, false)
	require.NoError(t, err)

	assert.Equal(t, 1, opts.Page)
	assert.Equal(t, 0, opts.PageSize)
	assert.Equal(t, 0, opts.SplitCount)
	assert.Equal(t, 0, opts.MinCompactionLevel)

	_, err = parseBlocksPageOptions(url.Values{"page_size": []string{"nope"}}, now, blocksPageDefaultPageSize, false)
	require.EqualError(t, err, `invalid page_size: "nope" is not a number`)
}

func TestParseBlocksPageTimeFilter(t *testing.T) {
	now := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)

	tests := map[string]struct {
		value        string
		expectedTime time.Time
		expectedErr  string
	}{
		"empty": {
			value:        "",
			expectedTime: time.Time{},
		},
		"now": {
			value:        "now",
			expectedTime: now,
		},
		"relative hours": {
			value:        "now-24h",
			expectedTime: now.Add(-24 * time.Hour),
		},
		"RFC3339": {
			value:        "2026-01-02T15:04:05Z",
			expectedTime: time.Date(2026, 1, 2, 15, 4, 5, 0, time.UTC),
		},
		"an invalid duration": {
			value:       "now-banana",
			expectedErr: `invalid created_after: not a valid duration string: "banana"`,
		},
		"an invalid absolute time": {
			value:       "yesterday",
			expectedErr: `invalid created_after: cannot parse "yesterday" to a valid timestamp`,
		},
	}

	for name, testData := range tests {
		t.Run(name, func(t *testing.T) {
			filter, err := parseBlocksPageTimeFilter(url.Values{"created_after": []string{testData.value}}, "created_after", now)
			if testData.expectedErr != "" {
				require.EqualError(t, err, testData.expectedErr)
				return
			}

			require.NoError(t, err)
			assert.True(t, filter.Time.Equal(testData.expectedTime), "expected %s, got %s", testData.expectedTime, filter.Time)
			assert.Equal(t, testData.value, filter.Input)
		})
	}
}

func prepareStoreGatewayForBlocksPage(t *testing.T, blocks []blocksPageTestBlock) (*StoreGateway, []string) {
	t.Helper()

	return prepareStoreGatewayForBlocksPageWithBucketIndex(t, blocks, true)
}

func prepareStoreGatewayForBlocksPageWithBucketIndex(t *testing.T, blocks []blocksPageTestBlock, writeBucketIndex bool) (*StoreGateway, []string) {
	t.Helper()

	bkt, _ := testutil.PrepareFilesystemBucket(t)

	idx := &bucketindex.Index{Version: bucketindex.IndexVersion2, UpdatedAt: time.Now().Unix()}
	ids := make([]string, 0, len(blocks))
	for _, b := range blocks {
		meta := uploadBlocksPageTestBlock(t, bkt, b)
		ids = append(ids, meta.ULID.String())

		idx.Blocks = append(idx.Blocks, bucketindex.BlockFromThanosMeta(meta))
		if b.deleted {
			idx.BlockDeletionMarks = append(idx.BlockDeletionMarks, &bucketindex.BlockDeletionMark{
				ID:           meta.ULID,
				DeletionTime: b.maxTime.Unix(),
			})
		}
	}

	if writeBucketIndex {
		require.NoError(t, bucketindex.WriteIndex(t.Context(), bkt, blocksPageTestTenant, nil, idx))
	}

	limits := validation.Limits{}
	flagext.DefaultValues(&limits)

	return &StoreGateway{
		logger: log.NewNopLogger(),
		stores: &BucketStores{bucket: bkt, limits: validation.NewOverrides(limits, nil)},
	}, ids
}

func uploadBlocksPageTestBlock(t *testing.T, bkt objstore.Bucket, b blocksPageTestBlock) block.Meta {
	t.Helper()

	blockID := ulid.MustNew(uint64(b.maxTime.UnixMilli()), nil)
	meta := block.Meta{
		BlockMeta: prom_tsdb.BlockMeta{
			ULID:    blockID,
			MinTime: b.minTime.UnixMilli(),
			MaxTime: b.maxTime.UnixMilli(),
			Version: block.TSDBVersion1,
			Compaction: prom_tsdb.BlockMetaCompaction{
				Level:   b.compactionLevel,
				Sources: []ulid.ULID{blockID},
			},
			Stats: prom_tsdb.BlockStats{
				NumSeries:  b.numSeries,
				NumSamples: b.numSamples,
				NumChunks:  b.numChunks,
			},
		},
		Thanos: block.ThanosMeta{
			Labels: b.labels,
			Source: block.TestSource,
			Files:  []block.File{{RelPath: "index", SizeBytes: b.sizeBytes}},
		},
	}

	uploadBlocksPageTestJSON(t, bkt, path.Join(blocksPageTestTenant, blockID.String(), block.MetaFilename), meta)

	if b.deleted {
		uploadBlocksPageTestJSON(t, bkt, path.Join(blocksPageTestTenant, block.DeletionMarkFilepath(blockID)), block.DeletionMark{
			ID:           blockID,
			DeletionTime: b.maxTime.Unix(),
			Version:      block.DeletionMarkVersion1,
		})
	}
	if b.noCompact {
		uploadBlocksPageTestJSON(t, bkt, path.Join(blocksPageTestTenant, block.NoCompactMarkFilepath(blockID)), block.NoCompactMark{
			ID:            blockID,
			NoCompactTime: b.maxTime.Unix(),
			Version:       block.NoCompactMarkVersion1,
			Reason:        block.ManualNoCompactReason,
		})
	}

	return meta
}

func uploadBlocksPageTestJSON(t *testing.T, bkt objstore.Bucket, objectPath string, content any) {
	t.Helper()

	encoded, err := json.Marshal(content)
	require.NoError(t, err)
	require.NoError(t, bkt.Upload(t.Context(), objectPath, bytes.NewReader(encoded)))
}

func requestBlocksPage(t *testing.T, gateway *StoreGateway, query, accept string) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(http.MethodGet, "/store-gateway/tenant/"+blocksPageTestTenant+"/blocks?"+query, nil)
	req = mux.SetURLVars(req.WithContext(t.Context()), map[string]string{"tenant": blocksPageTestTenant})
	if accept != "" {
		req.Header.Set("Accept", accept)
	}

	recorder := httptest.NewRecorder()
	gateway.BlocksHandler(recorder, req)
	return recorder
}

func requestBlocksPageJSON(t *testing.T, gateway *StoreGateway, query string) blocksPageTestResponse {
	t.Helper()

	recorder := requestBlocksPage(t, gateway, query, "application/json")
	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())

	var res blocksPageTestResponse
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &res))
	return res
}
