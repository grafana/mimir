// SPDX-License-Identifier: AGPL-3.0-only

package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
)

func (c *MimirClient) Backfill(ctx context.Context, blocks []string, logger log.Logger) error {
	// Upload each block
	var failed []string
	var succeeded []string
	var alreadyExists []string
	for _, b := range blocks {
		if err := c.backfillBlock(ctx, b, logger); err != nil {
			if errors.Is(err, errConflict) {
				level.Warn(logger).Log("msg", "failed uploading block since it already exists on server",
					"path", b)
				alreadyExists = append(alreadyExists, b)
			} else {
				level.Warn(logger).Log("msg", "failed uploading block", "path", b, "err", err)
				failed = append(failed, b)
			}
			continue
		}

		level.Info(logger).Log("msg", "successfully uploaded block", "path", b)
		succeeded = append(succeeded, b)
	}

	level.Info(logger).Log("msg", "finished uploading block(s)", "succeeded", len(succeeded),
		"already_exists", len(alreadyExists), "failed", len(failed))

	if len(failed) > 0 {
		return fmt.Errorf("failed to upload %d block(s)", len(failed))
	}

	return nil
}

func closeResp(resp *http.Response) {
	// Drain and close the body to let the transport reuse the connection
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
}

func (c *MimirClient) backfillBlock(ctx context.Context, blockDir string, logger log.Logger) error {
	blockMeta, err := getBlockMeta(blockDir)
	if err != nil {
		return err
	}

	blockID := blockMeta.ULID.String()
	logger = log.With(logger, "user", c.id, "block", blockID)

	level.Info(logger).Log("msg", "making request to start block upload")

	blockPrefix := path.Join("/api/v1/upload/block", url.PathEscape(blockID))

	buf := bytes.NewBuffer(nil)
	if err := json.NewEncoder(buf).Encode(blockMeta); err != nil {
		return errors.Wrap(err, "failed to JSON encode payload")
	}
	resp, err := c.doRequest(blockPrefix, http.MethodPost, buf, int64(buf.Len()))
	if err != nil {
		return errors.Wrap(err, "request to start block upload failed")
	}
	closeResp(resp)
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("request to start block upload failed, with HTTP status %d %s",
			resp.StatusCode, resp.Status)
	}

	// Upload each block file
	for _, tf := range blockMeta.Thanos.Files {
		if tf.RelPath == block.MetaFilename {
			// Don't upload meta file in this step
			continue
		}

		if err := c.uploadBlockFile(tf, blockDir, blockPrefix, logger); err != nil {
			return err
		}
	}

	resp, err = c.doRequest(fmt.Sprintf("%s?uploadComplete=true", blockPrefix), http.MethodPost,
		nil, -1)
	if err != nil {
		return errors.Wrap(err, "request to finish block upload failed")
	}
	closeResp(resp)
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("request to finish block upload failed, with HTTP status %d %s",
			resp.StatusCode, resp.Status)
	}

	level.Info(logger).Log("msg", "block uploaded successfully")

	return nil
}

func (c *MimirClient) uploadBlockFile(tf metadata.File, blockDir, blockPrefix string, logger log.Logger) error {
	pth := filepath.Join(blockDir, filepath.FromSlash(tf.RelPath))
	f, err := os.Open(pth)
	if err != nil {
		return errors.Wrapf(err, "failed to open %q", pth)
	}
	defer func() {
		_ = f.Close()
	}()

	st, err := f.Stat()
	if err != nil {
		return errors.Wrapf(err, "failed to get file info for %q", pth)
	}

	escapedPath := url.QueryEscape(tf.RelPath)
	level.Info(logger).Log("msg", "uploading block file", "path", pth, "size", st.Size())
	resp, err := c.doRequest(path.Join(blockPrefix, fmt.Sprintf("files?path=%s", escapedPath)),
		http.MethodPost, f, st.Size())
	if err != nil {
		return errors.Wrapf(err, "request to upload file %q failed", pth)
	}
	closeResp(resp)
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("request to upload block file failed, with HTTP status %d %s",
			resp.StatusCode, resp.Status)
	}

	return nil
}

func getBlockMeta(blockDir string) (metadata.Meta, error) {
	var blockMeta metadata.Meta

	metaPath := filepath.Join(blockDir, block.MetaFilename)
	f, err := os.Open(metaPath)
	if err != nil {
		return blockMeta, errors.Wrapf(err, "failed to open %q", metaPath)
	}
	defer func() {
		_ = f.Close()
	}()

	if err := json.NewDecoder(f).Decode(&blockMeta); err != nil {
		return blockMeta, errors.Wrapf(err, "failed to decode %q", metaPath)
	}

	idxPath := filepath.Join(blockDir, block.IndexFilename)
	idxSt, err := os.Stat(idxPath)
	if err != nil {
		return blockMeta, errors.Wrapf(err, "failed to stat %q", idxPath)
	}
	blockMeta.Thanos.Files = []metadata.File{
		{
			RelPath:   block.IndexFilename,
			SizeBytes: idxSt.Size(),
		},
		{
			RelPath: block.MetaFilename,
		},
	}

	chunksDir := filepath.Join(blockDir, block.ChunksDirname)
	entries, err := os.ReadDir(chunksDir)
	if err != nil {
		return blockMeta, errors.Wrapf(err, "failed to read dir %q", chunksDir)
	}
	for _, e := range entries {
		pth := filepath.Join(chunksDir, e.Name())

		if e.IsDir() {
			return blockMeta, fmt.Errorf("%q is a directory, there should only be files", pth)
		}

		// Note that we don't need to be strict about chunk files, the server will validate
		// the file list
		st, err := os.Stat(pth)
		if err != nil {
			return blockMeta, errors.Wrapf(err, "failed to stat %q", pth)
		}

		blockMeta.Thanos.Files = append(blockMeta.Thanos.Files, metadata.File{
			RelPath:   path.Join(block.ChunksDirname, e.Name()),
			SizeBytes: st.Size(),
		})
	}

	return blockMeta, nil
}
