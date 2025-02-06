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
	"github.com/oklog/ulid"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
)

func generateBlocks(t *testing.T) []ulid.ULID {
	blocks := make([]ulid.ULID, 0, 5)
	for i := 0; i < 5; i++ {
		id, err := ulid.New(ulid.Now(), nil)
		require.NoError(t, err)
		blocks = append(blocks, id)
	}
	return blocks
}

func TestAddAndRemoveMarks(t *testing.T) {
	cfg := config{
		tenantID: "tenant",
	}
	bkt := objstore.NewInMemBucket()
	blocks := generateBlocks(t)
	logger := log.NewNopLogger()
	ctx := context.Background()
	mvf, err := metaPresenceFunc("", nil, "none")
	require.NoError(t, err)

	for _, markType := range []string{"no-compact", "deletion"} {
		mbf, suffix, err := markerBytesFunc(markType, "")
		require.NoError(t, err)
		addF := addMarksFunc(cfg, bkt, blocks, mvf, mbf, suffix, logger)
		removeF := removeMarksFunc(cfg, bkt, blocks, mvf, suffix, logger)
		for i := 0; i < len(blocks); i++ {
			err := addF(ctx, i)
			require.NoError(t, err)

			exists, err := bkt.Exists(ctx, localMarkPath("tenant", blocks[i].String(), suffix))
			require.NoError(t, err)
			require.True(t, exists)

			exists, err = bkt.Exists(ctx, globalMarkPath("tenant", blocks[i].String(), suffix))
			require.NoError(t, err)
			require.True(t, exists)
		}

		for i := 0; i < len(blocks); i++ {
			err := removeF(ctx, i)
			require.NoError(t, err)

			exists, err := bkt.Exists(ctx, localMarkPath("tenant", blocks[i].String(), suffix))
			require.NoError(t, err)
			require.False(t, exists)

			exists, err = bkt.Exists(ctx, globalMarkPath("tenant", blocks[i].String(), suffix))
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

func TestMetaValidationPolicy(t *testing.T) {
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

			mvf, err := metaPresenceFunc(tenantID, bkt, name)
			require.NoError(t, err)

			// Block meta.json is not present
			skip, err := mvf(ctx, blockID)
			if tc.missingError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.missingSkip, skip)

			err = bkt.Upload(ctx, metaPath(tenantID, blockID), bytes.NewReader(nil))
			require.NoError(t, err)

			// Block meta.json is present
			skip, err = mvf(ctx, blockID)
			require.NoError(t, err)
			require.Equal(t, tc.presentSkip, skip)
		})
	}
}

func TestReadBlocks(t *testing.T) {
	blocks := generateBlocks(t)
	blockStrings := make([]string, 0, len(blocks))
	for _, block := range blocks {
		blockStrings = append(blockStrings, block.String())
	}
	r := strings.NewReader(strings.Join(blockStrings, "\n"))
	readBlocks, err := readBlocks(r)
	require.NoError(t, err)
	require.Equal(t, blocks, readBlocks)
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
	blockIDs := generateBlocks(t)
	blockStrings := make([]string, 0, len(blockIDs))
	for _, blockID := range blockIDs {
		blockStrings = append(blockStrings, blockID.String())
	}

	// Comma separated blocks take precedence
	commaBlocks, err := getBlocks(blockStrings, "-")
	require.NoError(t, err)
	require.Equal(t, blockIDs, commaBlocks)

	// Write a file with a block per line
	tmp := t.TempDir()
	filePath := path.Join(tmp, "blockfile")

	data := []byte(strings.Join(blockStrings, "\n"))
	err = os.WriteFile(filePath, data, os.ModePerm)
	require.NoError(t, err)

	fileBlocks, err := getBlocks(nil, filePath)
	require.NoError(t, err)
	require.Equal(t, blockIDs, fileBlocks)

	// Missing file
	missingBlocks, err := getBlocks(nil, path.Join(tmp, "missing"))
	require.Error(t, err)
	require.Empty(t, missingBlocks)
}
