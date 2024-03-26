// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/block/block.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package block

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/runutil"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/objstore"
)

const (
	// MetaFilename is the known JSON filename for meta information.
	MetaFilename = "meta.json"
	// IndexFilename is the known index file for block index.
	IndexFilename = "index"
	// IndexHeaderFilename is the canonical name for binary index header file that stores essential information.
	IndexHeaderFilename = "index-header"
	// SparseIndexHeaderFilename is the canonical name for sparse index header file that stores abbreviated slices of index-header.
	SparseIndexHeaderFilename = "sparse-index-header"
	// ChunksDirname is the known dir name for chunks with compressed samples.
	ChunksDirname = "chunks"

	// DebugMetas is a directory for debug meta files that happen in the past. Useful for debugging.
	DebugMetas = "debug/metas"
)

// Download downloads a directory meant to be a block directory. If any one of the files
// has a hash calculated in the meta file and it matches with what is in the destination path then
// we do not download it. We always re-download the meta file.
func Download(ctx context.Context, logger log.Logger, bucket objstore.Bucket, id ulid.ULID, dst string, options ...objstore.DownloadOption) error {
	if err := os.MkdirAll(dst, 0750); err != nil {
		return errors.Wrap(err, "create dir")
	}

	if err := objstore.DownloadFile(ctx, logger, bucket, path.Join(id.String(), MetaFilename), filepath.Join(dst, MetaFilename)); err != nil {
		return err
	}

	ignoredPaths := []string{MetaFilename}
	if err := objstore.DownloadDir(ctx, logger, bucket, id.String(), id.String(), dst, append(options, objstore.WithDownloadIgnoredPaths(ignoredPaths...))...); err != nil {
		return err
	}

	chunksDir := filepath.Join(dst, ChunksDirname)
	_, err := os.Stat(chunksDir)
	if os.IsNotExist(err) {
		// This can happen if block is empty. We cannot easily upload empty directory, so create one here.
		return os.Mkdir(chunksDir, os.ModePerm)
	}
	if err != nil {
		return errors.Wrapf(err, "stat %s", chunksDir)
	}

	return nil
}

// Upload uploads a TSDB block to the object storage. Notes:
//
// - If meta parameter is supplied (not nil), then uploaded meta.json file reflects meta parameter. However local
// meta.json file must still exist.
//
// - Meta struct is updated with gatherFileStats
func Upload(ctx context.Context, logger log.Logger, bkt objstore.Bucket, blockDir string, meta *Meta) error {
	df, err := os.Stat(blockDir)
	if err != nil {
		return err
	}
	if !df.IsDir() {
		return errors.Errorf("%s is not a directory", blockDir)
	}

	// Verify dir.
	id, err := ulid.Parse(df.Name())
	if err != nil {
		return errors.Wrap(err, "not a block dir")
	}

	if meta == nil {
		meta, err = ReadMetaFromDir(blockDir)
		if err != nil {
			// No meta or broken meta file.
			return errors.Wrap(err, "read meta")
		}
	}

	// Note that entry for meta.json file will be incorrect and will reflect local file,
	// not updated Meta struct.
	meta.Thanos.Files, err = GatherFileStats(blockDir)
	if err != nil {
		return errors.Wrap(err, "gather meta file stats")
	}

	metaEncoded := strings.Builder{}
	if err := meta.Write(&metaEncoded); err != nil {
		return errors.Wrap(err, "encode meta file")
	}

	if err := objstore.UploadDir(ctx, logger, bkt, filepath.Join(blockDir, ChunksDirname), path.Join(id.String(), ChunksDirname)); err != nil {
		return cleanUp(logger, bkt, id, errors.Wrap(err, "upload chunks"))
	}

	if err := objstore.UploadFile(ctx, logger, bkt, filepath.Join(blockDir, IndexFilename), path.Join(id.String(), IndexFilename)); err != nil {
		return cleanUp(logger, bkt, id, errors.Wrap(err, "upload index"))
	}

	// Meta.json always need to be uploaded as a last item. This will allow to assume block directories without meta file to be pending uploads.
	if err := bkt.Upload(ctx, path.Join(id.String(), MetaFilename), strings.NewReader(metaEncoded.String())); err != nil {
		// Don't call cleanUp here. Despite getting error, meta.json may have been uploaded in certain cases,
		// and even though cleanUp will not see it yet, meta.json may appear in the bucket later.
		// (Eg. S3 is known to behave this way when it returns 503 "SlowDown" error).
		// If meta.json is not uploaded, this will produce partial blocks, but such blocks will be cleaned later.
		return errors.Wrap(err, "upload meta file")
	}

	return nil
}

func cleanUp(logger log.Logger, bkt objstore.Bucket, id ulid.ULID, origErr error) error {
	// Cleanup the dir with an uncancelable context.
	cleanErr := Delete(context.Background(), logger, bkt, id)
	if cleanErr != nil {
		return errors.Wrapf(origErr, "failed to clean block after upload issue. Partial block in system. Err: %s", cleanErr.Error())
	}
	return origErr
}

// MarkForDeletion creates a file which stores information about when the block was marked for deletion.
func MarkForDeletion(ctx context.Context, logger log.Logger, bkt objstore.Bucket, id ulid.ULID, details string, markedForDeletion prometheus.Counter) error {
	deletionMarkFile := path.Join(id.String(), DeletionMarkFilename)
	deletionMarkExists, err := bkt.Exists(ctx, deletionMarkFile)
	if err != nil {
		return errors.Wrapf(err, "check exists %s in bucket", deletionMarkFile)
	}
	if deletionMarkExists {
		level.Warn(logger).Log("msg", "requested to mark for deletion, but file already exists; this should not happen; investigate", "err", errors.Errorf("file %s already exists in bucket", deletionMarkFile))
		return nil
	}

	deletionMark, err := json.Marshal(DeletionMark{
		ID:           id,
		DeletionTime: time.Now().Unix(),
		Version:      DeletionMarkVersion1,
		Details:      details,
	})
	if err != nil {
		return errors.Wrap(err, "json encode deletion mark")
	}

	if err := bkt.Upload(ctx, deletionMarkFile, bytes.NewBuffer(deletionMark)); err != nil {
		return errors.Wrapf(err, "upload file %s to bucket", deletionMarkFile)
	}
	markedForDeletion.Inc()
	level.Info(logger).Log("msg", "block has been marked for deletion", "block", id)
	return nil
}

// Delete removes directory that is meant to be block directory.
// NOTE: Always prefer this method for deleting blocks.
//   - We have to delete block's files in the certain order (meta.json first and deletion-mark.json last)
//     to ensure we don't end up with malformed partial blocks. Thanos system handles well partial blocks
//     only if they don't have meta.json. If meta.json is present Thanos assumes valid block.
//   - This avoids deleting empty dir (whole bucket) by mistake.
func Delete(ctx context.Context, logger log.Logger, bkt objstore.Bucket, id ulid.ULID) error {
	metaFile := path.Join(id.String(), MetaFilename)
	deletionMarkFile := path.Join(id.String(), DeletionMarkFilename)

	// Delete block meta file.
	ok, err := bkt.Exists(ctx, metaFile)
	if err != nil {
		return errors.Wrapf(err, "stat %s", metaFile)
	}

	if ok {
		if err := bkt.Delete(ctx, metaFile); err != nil {
			return errors.Wrapf(err, "delete %s", metaFile)
		}
		level.Debug(logger).Log("msg", "deleted file", "file", metaFile, "bucket", bkt.Name())
	}

	// Delete the block objects, but skip:
	// - The metaFile as we just deleted. This is required for eventual object storages (list after write).
	// - The deletionMarkFile as we'll delete it at last.
	err = deleteDirRec(ctx, logger, bkt, id.String(), func(name string) bool {
		return name == metaFile || name == deletionMarkFile
	})
	if err != nil {
		return err
	}

	// Delete block deletion mark.
	ok, err = bkt.Exists(ctx, deletionMarkFile)
	if err != nil {
		return errors.Wrapf(err, "stat %s", deletionMarkFile)
	}

	if ok {
		if err := bkt.Delete(ctx, deletionMarkFile); err != nil {
			return errors.Wrapf(err, "delete %s", deletionMarkFile)
		}
		level.Debug(logger).Log("msg", "deleted file", "file", deletionMarkFile, "bucket", bkt.Name())
	}

	return nil
}

// deleteDirRec removes all objects prefixed with dir from the bucket. It skips objects that return true for the passed keep function.
// NOTE: For objects removal use `block.Delete` strictly.
func deleteDirRec(ctx context.Context, logger log.Logger, bkt objstore.Bucket, dir string, keep func(name string) bool) error {
	return bkt.Iter(ctx, dir, func(name string) error {
		// If we hit a directory, call DeleteDir recursively.
		if strings.HasSuffix(name, objstore.DirDelim) {
			return deleteDirRec(ctx, logger, bkt, name, keep)
		}
		if keep(name) {
			return nil
		}
		if err := bkt.Delete(ctx, name); err != nil {
			return err
		}
		level.Debug(logger).Log("msg", "deleted file", "file", name, "bucket", bkt.Name())
		return nil
	})
}

// DownloadMeta downloads only meta file from bucket by block ID.
// TODO(bwplotka): Differentiate between network error & partial upload.
func DownloadMeta(ctx context.Context, logger log.Logger, bkt objstore.Bucket, id ulid.ULID) (Meta, error) {
	rc, err := bkt.Get(ctx, path.Join(id.String(), MetaFilename))
	if err != nil {
		return Meta{}, errors.Wrapf(err, "meta.json bkt get for %s", id.String())
	}
	defer runutil.CloseWithLogOnErr(logger, rc, "download meta bucket client")

	var m Meta

	obj, err := io.ReadAll(rc)
	if err != nil {
		return Meta{}, errors.Wrapf(err, "read meta.json for block %s", id.String())
	}

	if err = json.Unmarshal(obj, &m); err != nil {
		return Meta{}, errors.Wrapf(err, "unmarshal meta.json for block %s", id.String())
	}

	return m, nil
}

func IsBlockDir(path string) (id ulid.ULID, ok bool) {
	id, err := ulid.Parse(filepath.Base(path))
	return id, err == nil
}

// GetSegmentFiles returns list of segment files for given block. Paths are relative to the chunks directory.
// In case of errors, nil is returned.
func GetSegmentFiles(blockDir string) []string {
	files, err := os.ReadDir(filepath.Join(blockDir, ChunksDirname))
	if err != nil {
		return nil
	}

	// ReadDir returns files in sorted order already.
	var result []string
	for _, f := range files {
		result = append(result, f.Name())
	}
	return result
}

// GatherFileStats returns File entry for files inside TSDB block (index, chunks, meta.json).
func GatherFileStats(blockDir string) (res []File, _ error) {
	files, err := os.ReadDir(filepath.Join(blockDir, ChunksDirname))
	if err != nil {
		return nil, errors.Wrapf(err, "read dir %v", filepath.Join(blockDir, ChunksDirname))
	}
	for _, f := range files {
		fi, err := f.Info()
		if err != nil {
			return nil, errors.Wrapf(err, "getting file info %v", filepath.Join(ChunksDirname, f.Name()))
		}

		mf := File{
			RelPath:   filepath.Join(ChunksDirname, f.Name()),
			SizeBytes: fi.Size(),
		}
		res = append(res, mf)
	}

	indexFile, err := os.Stat(filepath.Join(blockDir, IndexFilename))
	if err != nil {
		return nil, errors.Wrapf(err, "stat %v", filepath.Join(blockDir, IndexFilename))
	}
	mf := File{
		RelPath:   indexFile.Name(),
		SizeBytes: indexFile.Size(),
	}
	res = append(res, mf)

	metaFile, err := os.Stat(filepath.Join(blockDir, MetaFilename))
	if err != nil {
		return nil, errors.Wrapf(err, "stat %v", filepath.Join(blockDir, MetaFilename))
	}
	res = append(res, File{RelPath: metaFile.Name()})

	sort.Slice(res, func(i, j int) bool {
		return strings.Compare(res[i].RelPath, res[j].RelPath) < 0
	})
	return res, err
}

// GetMetaAttributes returns the attributes for the block associated with the meta, using the userBucket to read the attributes.
func GetMetaAttributes(ctx context.Context, meta *Meta, bucketReader objstore.BucketReader) (objstore.ObjectAttributes, error) {
	metaPath := path.Join(meta.ULID.String(), MetaFilename)
	attrs, err := bucketReader.Attributes(ctx, metaPath)
	if err != nil {
		return objstore.ObjectAttributes{}, errors.Wrapf(err, "unable to get object attributes for %s", metaPath)
	}
	return attrs, nil
}

// MarkForNoCompact creates a file which marks block to be not compacted.
func MarkForNoCompact(ctx context.Context, logger log.Logger, bkt objstore.Bucket, id ulid.ULID, reason NoCompactReason, details string, markedForNoCompact prometheus.Counter) error {
	m := path.Join(id.String(), NoCompactMarkFilename)
	noCompactMarkExists, err := bkt.Exists(ctx, m)
	if err != nil {
		return errors.Wrapf(err, "check exists %s in bucket", m)
	}
	if noCompactMarkExists {
		level.Warn(logger).Log("msg", "requested to mark for no compaction, but file already exists; this should not happen; investigate", "err", errors.Errorf("file %s already exists in bucket", m))
		return nil
	}

	noCompactMark, err := json.Marshal(NoCompactMark{
		ID:      id,
		Version: NoCompactMarkVersion1,

		NoCompactTime: time.Now().Unix(),
		Reason:        reason,
		Details:       details,
	})
	if err != nil {
		return errors.Wrap(err, "json encode no compact mark")
	}

	if err := bkt.Upload(ctx, m, bytes.NewBuffer(noCompactMark)); err != nil {
		return errors.Wrapf(err, "upload file %s to bucket", m)
	}
	markedForNoCompact.Inc()
	level.Info(logger).Log("msg", "block has been marked for no compaction", "block", id)
	return nil
}

func DeleteNoCompactMarker(ctx context.Context, logger log.Logger, bkt objstore.Bucket, id ulid.ULID) error {
	m := path.Join(id.String(), NoCompactMarkFilename)
	if err := bkt.Delete(ctx, m); err != nil {
		return errors.Wrapf(err, "deletion of no-compaction marker for block %s has failed", id.String())
	}
	level.Info(logger).Log("msg", "no-compaction marker has been deleted; block can be compacted in the future", "block", id)
	return nil
}
