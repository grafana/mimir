// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/block/block.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package tsdb

import (
	"context"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"

	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/objstore"
)

// UploadBlock is copy of block.Upload with following modifications:
//
// - If meta parameter is supplied (not nil), then uploaded meta.json file reflects meta parameter. However local
// meta.json file must still exist.
//
// - Meta struct is updated with gatherFileStats
//
// - external labels are not checked for
func UploadBlock(ctx context.Context, logger log.Logger, bkt objstore.Bucket, blockDir string, meta *metadata.Meta) error {
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
		meta, err = metadata.ReadFromDir(blockDir)
		if err != nil {
			// No meta or broken meta file.
			return errors.Wrap(err, "read meta")
		}
	}

	// Note that entry for meta.json file will be incorrect and will reflect local file,
	// not updated Meta struct.
	meta.Thanos.Files, err = gatherFileStats(blockDir)
	if err != nil {
		return errors.Wrap(err, "gather meta file stats")
	}

	metaEncoded := strings.Builder{}
	if err := meta.Write(&metaEncoded); err != nil {
		return errors.Wrap(err, "encode meta file")
	}

	if err := objstore.UploadDir(ctx, logger, bkt, filepath.Join(blockDir, block.ChunksDirname), path.Join(id.String(), block.ChunksDirname)); err != nil {
		return cleanUp(logger, bkt, id, errors.Wrap(err, "upload chunks"))
	}

	if err := objstore.UploadFile(ctx, logger, bkt, filepath.Join(blockDir, block.IndexFilename), path.Join(id.String(), block.IndexFilename)); err != nil {
		return cleanUp(logger, bkt, id, errors.Wrap(err, "upload index"))
	}

	// Meta.json always need to be uploaded as a last item. This will allow to assume block directories without meta file to be pending uploads.
	if err := bkt.Upload(ctx, path.Join(id.String(), block.MetaFilename), strings.NewReader(metaEncoded.String())); err != nil {
		// Don't call cleanUp here. Despite getting error, meta.json may have been uploaded in certain cases,
		// and even though cleanUp will not see it yet, meta.json may appear in the bucket later.
		// (Eg. S3 is known to behave this way when it returns 503 "SlowDown" error).
		// If meta.json is not uploaded, this will produce partial blocks, but such blocks will be cleaned later.
		return errors.Wrap(err, "upload meta file")
	}

	return nil
}

// gatherFileStats can hopefully be replaced with Thanos public function: https://github.com/thanos-io/thanos/pull/5400
func gatherFileStats(blockDir string) (res []metadata.File, _ error) {
	files, err := ioutil.ReadDir(filepath.Join(blockDir, block.ChunksDirname))
	if err != nil {
		return nil, errors.Wrapf(err, "read dir %v", filepath.Join(blockDir, block.ChunksDirname))
	}
	for _, f := range files {
		mf := metadata.File{
			RelPath:   filepath.Join(block.ChunksDirname, f.Name()),
			SizeBytes: f.Size(),
		}
		res = append(res, mf)
	}

	indexFile, err := os.Stat(filepath.Join(blockDir, block.IndexFilename))
	if err != nil {
		return nil, errors.Wrapf(err, "stat %v", filepath.Join(blockDir, block.IndexFilename))
	}
	mf := metadata.File{
		RelPath:   indexFile.Name(),
		SizeBytes: indexFile.Size(),
	}
	res = append(res, mf)

	metaFile, err := os.Stat(filepath.Join(blockDir, block.MetaFilename))
	if err != nil {
		return nil, errors.Wrapf(err, "stat %v", filepath.Join(blockDir, block.MetaFilename))
	}
	res = append(res, metadata.File{RelPath: metaFile.Name()})

	sort.Slice(res, func(i, j int) bool {
		return res[i].RelPath < res[j].RelPath
	})
	return res, err
}

func cleanUp(logger log.Logger, bkt objstore.Bucket, id ulid.ULID, origErr error) error {
	// Cleanup the dir with an uncancelable context.
	cleanErr := block.Delete(context.Background(), logger, bkt, id)
	if cleanErr != nil {
		return errors.Wrapf(origErr, "failed to clean block after upload issue. Partial block in system. Err: %s", cleanErr.Error())
	}
	return origErr
}
