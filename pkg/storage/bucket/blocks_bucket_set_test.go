// SPDX-License-Identifier: AGPL-3.0-only

package bucket

import (
	"strings"
	"testing"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
)

func TestNewBlocksBucketSet(t *testing.T) {
	longName := strings.Repeat("a", maxBucketNameLengthInMetadataCacheID+1)

	tests := map[string]struct {
		writeBucketName           string
		additionalReadBucketNames []string
		expectedErr               string
		expectedMetadataIDs       []string
	}{
		"one bucket has no metadata cache ID": {
			writeBucketName:     "blocks",
			expectedMetadataIDs: []string{""},
		},
		"one bucket without a name is valid": {
			writeBucketName:     "",
			expectedMetadataIDs: []string{""},
		},
		"each of more than one bucket has a metadata cache ID": {
			writeBucketName:           "new-blocks",
			additionalReadBucketNames: []string{"old-blocks"},
			expectedMetadataIDs:       []string{"blocks-bucket-new-blocks", "blocks-bucket-old-blocks"},
		},
		"a long bucket name is hashed": {
			writeBucketName:           "new-blocks",
			additionalReadBucketNames: []string{longName},
			expectedMetadataIDs:       []string{"blocks-bucket-new-blocks", metadataCacheID(longName)},
		},
		"a bucket name with a space is hashed": {
			writeBucketName:           "new-blocks",
			additionalReadBucketNames: []string{"old blocks"},
			expectedMetadataIDs:       []string{"blocks-bucket-new-blocks", metadataCacheID("old blocks")},
		},
		"an empty name with more than one bucket is not valid": {
			writeBucketName:           "new-blocks",
			additionalReadBucketNames: []string{""},
			expectedErr:               "the name of a blocks bucket must not be empty when there is more than one blocks bucket",
		},
		"a duplicate name is not valid": {
			writeBucketName:           "new-blocks",
			additionalReadBucketNames: []string{"old-blocks", "new-blocks"},
			expectedErr:               `the blocks bucket name "new-blocks" is used more than once`,
		},
	}

	for name, testData := range tests {
		t.Run(name, func(t *testing.T) {
			writeBucket := BlocksBucket{Name: testData.writeBucketName, Client: objstore.NewInMemBucket()}
			additionalReadBuckets := make([]BlocksBucket, 0, len(testData.additionalReadBucketNames))
			for _, bucketName := range testData.additionalReadBucketNames {
				additionalReadBuckets = append(additionalReadBuckets, BlocksBucket{Name: bucketName, Client: objstore.NewInMemBucket()})
			}

			set, err := NewBlocksBucketSet(writeBucket, additionalReadBuckets)
			if testData.expectedErr != "" {
				require.EqualError(t, err, testData.expectedErr)
				return
			}
			require.NoError(t, err)

			assert.Equal(t, writeBucket.Name, set.WriteBucket().Name)
			assert.Same(t, writeBucket.Client, set.WriteBucket().Client)

			expectedReadBuckets := append([]BlocksBucket{writeBucket}, additionalReadBuckets...)
			readBuckets := set.ReadBuckets()
			require.Len(t, readBuckets, len(expectedReadBuckets))
			for i, b := range readBuckets {
				assert.Equal(t, expectedReadBuckets[i].Name, b.Name)
				assert.Same(t, expectedReadBuckets[i].Client, b.Client)
				assert.Equal(t, testData.expectedMetadataIDs[i], b.MetadataCacheID)
			}
			assert.Equal(t, readBuckets[0], set.WriteBucket())
		})
	}
}

func TestBlocksBucketSet_ReadBucketsReturnsACopy(t *testing.T) {
	set, err := NewBlocksBucketSet(
		BlocksBucket{Name: "new-blocks", Client: objstore.NewInMemBucket()},
		[]BlocksBucket{{Name: "old-blocks", Client: objstore.NewInMemBucket()}},
	)
	require.NoError(t, err)

	readBuckets := set.ReadBuckets()
	readBuckets[0].Name = "changed"
	readBuckets[1].Name = "changed"

	assert.Equal(t, "new-blocks", set.ReadBuckets()[0].Name)
	assert.Equal(t, "old-blocks", set.ReadBuckets()[1].Name)
}

func TestMetadataCacheID(t *testing.T) {
	assert.Equal(t, "blocks-bucket-old-blocks", metadataCacheID("old-blocks"))

	// The hash keeps the ID short, and different names give different IDs.
	first := metadataCacheID(strings.Repeat("a", 100))
	second := metadataCacheID(strings.Repeat("b", 100))
	assert.NotEqual(t, first, second)
	assert.Less(t, len(first), len(metadataCacheIDPrefix)+maxBucketNameLengthInMetadataCacheID)

	for _, name := range []string{"with space", "with\ttab", "with-ümlaut", "with\x7fdelete"} {
		id := metadataCacheID(name)
		assert.NotContains(t, id, name)
		assert.True(t, strings.HasPrefix(id, metadataCacheIDPrefix))
	}
}

func TestNewBlocksBucketSetFromConfigs(t *testing.T) {
	newFilesystemConfig := func(dir string) Config {
		cfg := Config{}
		cfg.Backend = Filesystem
		cfg.Filesystem.Directory = dir
		return cfg
	}

	t.Run("creates one client for each config, with the write bucket first", func(t *testing.T) {
		writeDir, oldDir, olderDir := t.TempDir(), t.TempDir(), t.TempDir()

		// A pedantic registry fails if two clients register the same metrics.
		reg := prometheus.NewPedanticRegistry()
		set, err := NewBlocksBucketSetFromConfigs(t.Context(), newFilesystemConfig(writeDir), []Config{newFilesystemConfig(oldDir), newFilesystemConfig(olderDir)}, "test", log.NewNopLogger(), reg)
		require.NoError(t, err)

		readBuckets := set.ReadBuckets()
		require.Len(t, readBuckets, 3)
		for i, expectedName := range []string{writeDir, oldDir, olderDir} {
			assert.Equal(t, expectedName, readBuckets[i].Name)
			assert.Equal(t, metadataCacheID(expectedName), readBuckets[i].MetadataCacheID)
		}

		// Each client writes to its own bucket.
		require.NoError(t, set.WriteBucket().Client.Upload(t.Context(), "object", strings.NewReader("content")))
		for i, b := range readBuckets {
			exists, err := b.Client.Exists(t.Context(), "object")
			require.NoError(t, err)
			assert.Equal(t, i == 0, exists)
		}

		_, err = reg.Gather()
		require.NoError(t, err)
	})

	t.Run("without additional read buckets the set has only the write bucket", func(t *testing.T) {
		writeDir := t.TempDir()

		set, err := NewBlocksBucketSetFromConfigs(t.Context(), newFilesystemConfig(writeDir), nil, "test", log.NewNopLogger(), nil)
		require.NoError(t, err)

		require.Len(t, set.ReadBuckets(), 1)
		assert.Equal(t, writeDir, set.WriteBucket().Name)
		assert.Empty(t, set.WriteBucket().MetadataCacheID)
	})

	t.Run("rejects a duplicate bucket name before it creates the clients", func(t *testing.T) {
		writeDir := t.TempDir()

		_, err := NewBlocksBucketSetFromConfigs(t.Context(), newFilesystemConfig(writeDir), []Config{newFilesystemConfig(writeDir)}, "test", log.NewNopLogger(), prometheus.NewPedanticRegistry())
		require.EqualError(t, err, `the blocks bucket name "`+writeDir+`" is used more than once`)
	})
}
