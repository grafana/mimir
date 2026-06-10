// SPDX-License-Identifier: AGPL-3.0-only

package objtools

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"os"
	"path"
	"path/filepath"
	"testing"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testBlockID(ms uint64) ulid.ULID {
	return ulid.MustNew(ms, rand.Reader)
}

// blockObj returns the canonical object name for a file inside a tenant's block directory
func blockObj(tenant string, block ulid.ULID, file string) string {
	return path.Join(tenant, block.String(), file)
}

func newTestFilesystemBucket(t *testing.T) *fsBucket {
	t.Helper()
	bkt, err := (&FilesystemClientConfig{Directory: t.TempDir()}).ToBucket()
	require.NoError(t, err)
	return bkt.(*fsBucket)
}

func uploadStringContent(t *testing.T, bkt Bucket, objectName, content string) {
	t.Helper()
	require.NoError(t, bkt.Upload(context.Background(), objectName, bytes.NewReader([]byte(content)), int64(len(content))))
}

func getStringContent(t *testing.T, bkt Bucket, objectName string) string {
	t.Helper()
	r, err := bkt.Get(context.Background(), objectName, GetOptions{})
	require.NoError(t, err)
	defer r.Close()
	content, err := io.ReadAll(r)
	require.NoError(t, err)
	return string(content)
}

func TestFilesystemBucket_UploadGetExists(t *testing.T) {
	bkt := newTestFilesystemBucket(t)
	ctx := context.Background()
	block := testBlockID(1)

	metaName := blockObj("tenant", block, "meta.json")
	uploadStringContent(t, bkt, metaName, "foo")
	assert.Equal(t, "foo", getStringContent(t, bkt, metaName))

	exists, err := bkt.Exists(ctx, metaName, ExistsOptions{})
	require.NoError(t, err)
	assert.True(t, exists)

	missing := blockObj("tenant", block, "missing.json")
	exists, err = bkt.Exists(ctx, missing, ExistsOptions{})
	require.NoError(t, err)
	assert.False(t, exists)

	_, err = bkt.Get(ctx, missing, GetOptions{})
	assert.ErrorIs(t, err, os.ErrNotExist)

	// A directory must not be treated as an object
	exists, err = bkt.Exists(ctx, "tenant/"+block.String(), ExistsOptions{})
	require.NoError(t, err)
	assert.False(t, exists)
}

func TestFilesystemBucket_List(t *testing.T) {
	bkt := newTestFilesystemBucket(t)
	ctx := context.Background()
	block1 := testBlockID(1)
	block2 := testBlockID(2)

	uploadStringContent(t, bkt, blockObj("tenant1", block1, "meta.json"), "a")
	uploadStringContent(t, bkt, blockObj("tenant1", block1, "index"), "b")
	uploadStringContent(t, bkt, blockObj("tenant1", block2, "meta.json"), "c")
	uploadStringContent(t, bkt, blockObj("tenant2", block1, "meta.json"), "d")

	// Shallow listing of the whole bucket returns tenants
	result, err := bkt.List(ctx, ListOptions{})
	require.NoError(t, err)
	assert.Equal(t, []string{"tenant1", "tenant2"}, result.ToNames())

	// Shallow listing of a tenant returns its blocks
	result, err = bkt.List(ctx, ListOptions{Prefix: "tenant1"})
	require.NoError(t, err)
	names, err := result.ToNamesWithoutPrefix("tenant1")
	require.NoError(t, err)
	assert.Equal(t, []string{block1.String(), block2.String()}, names)

	// Recursive listing
	result, err = bkt.List(ctx, ListOptions{Prefix: "tenant1/" + block1.String(), Recursive: true})
	require.NoError(t, err)
	assert.Equal(t, []string{blockObj("tenant1", block1, "index"), blockObj("tenant1", block1, "meta.json")}, result.ToNames())

	// Listing a nonexistent prefix returns an empty result rather than an error
	result, err = bkt.List(ctx, ListOptions{Prefix: "missing"})
	require.NoError(t, err)
	assert.Empty(t, result.ToNames())

	// The temporary write directory is never surfaced in listings
	require.NoError(t, os.WriteFile(filepath.Join(bkt.tempWriteDir(), "leftover"), []byte("x"), 0o600))
	result, err = bkt.List(ctx, ListOptions{})
	require.NoError(t, err)
	assert.Equal(t, []string{"tenant1", "tenant2"}, result.ToNames())
	result, err = bkt.List(ctx, ListOptions{Recursive: true})
	require.NoError(t, err)
	assert.Equal(t, []string{
		blockObj("tenant1", block1, "index"),
		blockObj("tenant1", block1, "meta.json"),
		blockObj("tenant1", block2, "meta.json"),
		blockObj("tenant2", block1, "meta.json"),
	}, result.ToNames())
	_, err = bkt.List(ctx, ListOptions{Prefix: tmpWriteDirName})
	assert.ErrorIs(t, err, errTmpWriteDirReserved)
}

func TestFilesystemBucket_Copy(t *testing.T) {
	block := testBlockID(1)
	name := blockObj("tenant", block, "meta.json")

	type copyFn func(src, dst *fsBucket, name string, opts CopyOptions) error

	for _, tc := range []struct {
		name string
		copy copyFn
	}{
		{"server-side copy", func(src, dst *fsBucket, n string, opts CopyOptions) error {
			return src.ServerSideCopy(context.Background(), n, dst, opts)
		}},
		{"client-side copy", func(src, dst *fsBucket, n string, opts CopyOptions) error {
			return src.ClientSideCopy(context.Background(), n, dst, opts)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src := newTestFilesystemBucket(t)
			dst := newTestFilesystemBucket(t)
			uploadStringContent(t, src, name, "payload")

			require.NoError(t, tc.copy(src, dst, name, CopyOptions{}))
			assert.Equal(t, "payload", getStringContent(t, dst, name))

			// With a renamed destination object
			renamed := blockObj("other", block, "meta.json")
			require.NoError(t, tc.copy(src, dst, name, CopyOptions{DestinationObjectName: renamed}))
			assert.Equal(t, "payload", getStringContent(t, dst, renamed))
		})
	}
}

func TestFilesystemBucket_DeletePrunesEmptyParents(t *testing.T) {
	bkt := newTestFilesystemBucket(t)
	ctx := context.Background()
	block := testBlockID(1)

	lower := blockObj("tenant", block, "meta.json")
	uploadStringContent(t, bkt, lower, "foo")

	require.NoError(t, bkt.Delete(ctx, lower, DeleteOptions{}))

	// The empty tenant is pruned
	_, err := os.Stat(filepath.Join(bkt.directory, "tenant", block.String()))
	assert.ErrorIs(t, err, os.ErrNotExist)

	// Reupload
	uploadStringContent(t, bkt, lower, "foo")

	upper := "tenant/keep.json"
	uploadStringContent(t, bkt, upper, "bar")

	require.NoError(t, bkt.Delete(ctx, lower, DeleteOptions{}))

	// The empty block directory is pruned, but the tenant directory remains
	_, err = os.Stat(filepath.Join(bkt.directory, "tenant", block.String()))
	assert.ErrorIs(t, err, os.ErrNotExist)
	_, err = os.Stat(filepath.Join(bkt.directory, upper))
	require.NoError(t, err)

	// Delete the last file, which causes the tenant directory to be pruned
	require.NoError(t, bkt.Delete(ctx, upper, DeleteOptions{}))
	_, err = os.Stat(filepath.Join(bkt.directory, "tenant"))
	assert.ErrorIs(t, err, os.ErrNotExist)
}

func TestFilesystemBucket_InvalidNamesRejected(t *testing.T) {
	bkt := newTestFilesystemBucket(t)
	ctx := context.Background()
	block := testBlockID(1)

	uploadStringContent(t, bkt, blockObj("tenant", block, "meta.json"), "payload")
	dst := newTestFilesystemBucket(t)

	// All object-name operations reject names that resolve into the temporar write directory
	// or that are not canonical paths.
	for _, tc := range []struct {
		name    string
		wantErr error
	}{
		{tmpWriteDirName + "/object", errTmpWriteDirReserved},
		{"tenant/" + block.String() + "/", errNonCanonicalName}, // trailing delimiter (directory-like)
		{"tenant//" + block.String(), errNonCanonicalName},      // repeated delimiter
		{"tenant/./" + block.String(), errNonCanonicalName},     // "." segment
		{"tenant/../" + block.String(), errNonCanonicalName},    // ".." segment
		{"./tenant", errNonCanonicalName},                       // leading "."
		{"../escape", errNonCanonicalName},                      // escapes the bucket
		{"/tenant/" + block.String(), errNonCanonicalName},      // leading delimiter (filepath.Join would map it onto the slash-less path)
		{".", errNonCanonicalName},
		{"", errNonCanonicalName},
	} {
		err := bkt.Upload(ctx, tc.name, bytes.NewReader([]byte("x")), 1)
		assert.ErrorIsf(t, err, tc.wantErr, "Upload(%q)", tc.name)

		err = bkt.Delete(ctx, tc.name, DeleteOptions{})
		assert.ErrorIsf(t, err, tc.wantErr, "Delete(%q)", tc.name)

		_, err = bkt.Get(ctx, tc.name, GetOptions{})
		assert.ErrorIsf(t, err, tc.wantErr, "Get(%q)", tc.name)

		_, err = bkt.Exists(ctx, tc.name, ExistsOptions{})
		assert.ErrorIsf(t, err, tc.wantErr, "Exists(%q)", tc.name)

		err = bkt.ServerSideCopy(ctx, tc.name, dst, CopyOptions{})
		assert.ErrorIsf(t, err, tc.wantErr, "ServerSideCopy source %q", tc.name)

		err = bkt.ClientSideCopy(ctx, tc.name, dst, CopyOptions{})
		assert.ErrorIsf(t, err, tc.wantErr, "ClientSideCopy source %q", tc.name)

		// An empty destination name falls back to the source object name, so only
		// non-empty names are meaningful to test as copy destinations
		if tc.name != "" {
			err = bkt.ServerSideCopy(ctx, blockObj("tenant", block, "meta.json"), dst, CopyOptions{DestinationObjectName: tc.name})
			assert.ErrorIsf(t, err, tc.wantErr, "ServerSideCopy destination %q", tc.name)

			err = bkt.ClientSideCopy(ctx, blockObj("tenant", block, "meta.json"), dst, CopyOptions{DestinationObjectName: tc.name})
			assert.ErrorIsf(t, err, tc.wantErr, "ClientSideCopy destination %q", tc.name)
		}
	}
}

func TestFilesystemBucket_ListPrefixContainment(t *testing.T) {
	bkt := newTestFilesystemBucket(t)
	ctx := context.Background()
	block := testBlockID(1)

	uploadStringContent(t, bkt, blockObj("tenant", block, "meta.json"), "payload")

	// A non-canonical listing prefix is rejected. Because paths are treated as relative
	// and canonical form forbids ".." segments, this also prevents a prefix from escaping the bucket directory.
	_, err := bkt.List(ctx, ListOptions{Prefix: "../escape"})
	assert.ErrorIs(t, err, errNonCanonicalName)

	// A whole-bucket listing resolves to the bucket directory and is permitted,
	result, err := bkt.List(ctx, ListOptions{})
	require.NoError(t, err)
	assert.Equal(t, []string{"tenant"}, result.ToNames())
}

func TestFilesystemBucket_DirectoryTargetRejected(t *testing.T) {
	bkt := newTestFilesystemBucket(t)
	ctx := context.Background()
	block := testBlockID(1)

	name := blockObj("tenant", block, "meta.json")
	uploadStringContent(t, bkt, name, "payload")
	dirTarget := "tenant/" + block.String()

	// dirTarget exists as a directory, so it can't be used as an object name
	_, err := bkt.Get(ctx, dirTarget, GetOptions{})
	assert.ErrorIs(t, err, errObjectIsDirectory)

	err = bkt.Upload(ctx, dirTarget, bytes.NewReader([]byte("foo")), 1)
	assert.ErrorIs(t, err, errObjectIsDirectory)

	err = bkt.Delete(ctx, dirTarget, DeleteOptions{})
	assert.ErrorIs(t, err, errObjectIsDirectory)

	dst := newTestFilesystemBucket(t)
	uploadStringContent(t, dst, blockObj("other", block, "keep.json"), "foo")
	dstDir := "other/" + block.String()
	err = bkt.ServerSideCopy(ctx, name, dst, CopyOptions{DestinationObjectName: dstDir})
	assert.ErrorIs(t, err, errObjectIsDirectory)

	err = bkt.ClientSideCopy(ctx, name, dst, CopyOptions{DestinationObjectName: dstDir})
	assert.ErrorIs(t, err, errObjectIsDirectory)

	// The directory and its contents are untouched
	assert.Equal(t, "payload", getStringContent(t, bkt, name))
}

func TestFilesystemBucket_VersioningUnsupported(t *testing.T) {
	bkt := newTestFilesystemBucket(t)
	ctx := context.Background()
	block := testBlockID(1)

	name := blockObj("tenant", block, "meta.json")
	uploadStringContent(t, bkt, name, "a")

	_, err := bkt.Get(ctx, name, GetOptions{VersionID: "1"})
	assert.ErrorIs(t, err, errFilesystemVersioningUnsupported)

	_, err = bkt.Exists(ctx, name, ExistsOptions{VersionID: "1"})
	assert.ErrorIs(t, err, errFilesystemVersioningUnsupported)

	err = bkt.Delete(ctx, name, DeleteOptions{VersionID: "1"})
	assert.ErrorIs(t, err, errFilesystemVersioningUnsupported)

	err = bkt.RestoreVersion(ctx, name, VersionInfo{VersionID: "1"})
	assert.ErrorIs(t, err, errFilesystemVersioningUnsupported)
}
