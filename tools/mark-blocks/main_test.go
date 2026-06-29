// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path"
	"strings"
	"testing"

	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
)

func generateBlocks(t *testing.T, tenantID string) []inputBlock {
	blocks := make([]inputBlock, 0, 5)
	for range 5 {
		blockID, err := ulid.New(ulid.Now(), nil)
		require.NoError(t, err)
		blocks = append(blocks, inputBlock{
			tenantID,
			blockID,
		})
	}
	return blocks
}

func TestAddAndRemoveMarks(t *testing.T) {
	bkt := objstore.NewInMemBucket()
	tenantID := "tenant"
	blocks := generateBlocks(t, tenantID)
	logger := log.NewNopLogger()
	ctx := context.Background()
	mvf, err := metaPresenceFunc(nil, "none")
	require.NoError(t, err)

	for _, markType := range []string{"no-compact", "deletion"} {
		mbf, suffix, err := markerBytesFunc(markType, "")
		require.NoError(t, err)
		addF := addMarksFunc(bkt, blocks, mvf, mbf, suffix, logger, false)
		removeF := removeMarksFunc(bkt, blocks, mvf, suffix, logger, false)
		for i := range blocks {
			err := addF(ctx, i)
			require.NoError(t, err)

			exists, err := bkt.Exists(ctx, localMarkPath(tenantID, blocks[i].blockID.String(), suffix))
			require.NoError(t, err)
			require.True(t, exists)

			exists, err = bkt.Exists(ctx, globalMarkPath(tenantID, blocks[i].blockID.String(), suffix))
			require.NoError(t, err)
			require.True(t, exists)
		}

		for i := range blocks {
			err := removeF(ctx, i)
			require.NoError(t, err)

			exists, err := bkt.Exists(ctx, localMarkPath(tenantID, blocks[i].blockID.String(), suffix))
			require.NoError(t, err)
			require.False(t, exists)

			exists, err = bkt.Exists(ctx, globalMarkPath(tenantID, blocks[i].blockID.String(), suffix))
			require.NoError(t, err)
			require.False(t, exists)
		}
	}

	var names []string
	err = bkt.Iter(ctx, "", func(name string) error {
		names = append(names, name)
		return nil
	})
	require.NoError(t, err)
	require.Empty(t, names)
}

func TestMetaPresencePolicy(t *testing.T) {
	tenantID := "tenant"

	id, err := ulid.New(ulid.Now(), nil)
	require.NoError(t, err)
	blockID := id.String()

	cases := map[string]struct {
		missingSkip  bool
		missingError bool
		presentSkip  bool
	}{
		"none": {
			false,
			false,
			false,
		},
		"skip-block": {
			true,
			false,
			false,
		},
		"require": {
			false,
			true,
			false,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			bkt := objstore.NewInMemBucket()

			mpf, err := metaPresenceFunc(bkt, name)
			require.NoError(t, err)

			// Block meta.json is not present
			skip, err := mpf(ctx, tenantID, blockID)
			if tc.missingError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.missingSkip, skip)

			err = bkt.Upload(ctx, metaPath(tenantID, blockID), bytes.NewReader(nil))
			require.NoError(t, err)

			// Block meta.json is present
			skip, err = mpf(ctx, tenantID, blockID)
			require.NoError(t, err)
			require.Equal(t, tc.presentSkip, skip)
		})
	}
}

func TestReadBlocks(t *testing.T) {
	blocks := generateBlocks(t, "tenant")
	blockStrings := make([]string, 0, len(blocks))
	for _, block := range blocks {
		blockStrings = append(blockStrings, block.blockID.String())
	}
	lines := strings.Join(blockStrings, "\n")

	// Only blockIDs
	result, err := readBlocks(strings.NewReader(lines), "tenant")
	require.NoError(t, err)
	require.Equal(t, blocks, result)

	// Only blockIDs, but no tenant provided (unexpected)
	result, err = readBlocks(strings.NewReader(lines), "")
	require.Error(t, err)
	require.Nil(t, result)

	blocks = append(blocks, generateBlocks(t, "tenant2")...)
	blockStrings = make([]string, 0, len(blocks))
	for _, block := range blocks {
		blockStrings = append(blockStrings, block.tenantID+"/"+block.blockID.String())
	}
	blockStrings[len(blockStrings)-1] += "/" // test support for an optional trailing slash

	lines = strings.Join(blockStrings, "\n")

	// tenantID/blockID format
	result, err = readBlocks(strings.NewReader(lines), "")
	require.NoError(t, err)
	require.Equal(t, blocks, result)

	// tenantID/blockID format, but tenant provided (not expected)
	result, err = readBlocks(strings.NewReader(lines), "tenant")
	require.Error(t, err)
	require.Nil(t, result)
}

func TestForEachJobSuccessUntil(t *testing.T) {
	until, err := forEachJobSuccessUntil(context.Background(), 5, 1, func(_ context.Context, idx int) error {
		if idx == 4 {
			return errors.New("injected failure")
		}
		return nil
	})
	require.Error(t, err)
	require.Equal(t, 4, until)

	until, err = forEachJobSuccessUntil(context.Background(), 10, 3, func(_ context.Context, _ int) error {
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 10, until)
}

func TestGetBlocks(t *testing.T) {
	blocks := generateBlocks(t, "tenant")
	blockStrings := make([]string, 0, len(blocks))
	for _, block := range blocks {
		blockStrings = append(blockStrings, block.blockID.String())
	}

	// Comma separated blocks take precedence
	commaBlocks, err := getBlocks(blockStrings, "-", "tenant")
	require.NoError(t, err)
	require.Equal(t, blocks, commaBlocks)

	// Write a file with a block per line
	tmp := t.TempDir()
	filePath := path.Join(tmp, "blockfile")

	data := []byte(strings.Join(blockStrings, "\n"))
	err = os.WriteFile(filePath, data, os.ModePerm)
	require.NoError(t, err)

	fileBlocks, err := getBlocks(nil, filePath, "tenant")
	require.NoError(t, err)
	require.Equal(t, blocks, fileBlocks)

	// Missing file
	missingBlocks, err := getBlocks(nil, path.Join(tmp, "missing"), "tenant")
	require.Error(t, err)
	require.Empty(t, missingBlocks)
}
