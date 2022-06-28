// SPDX-License-Identifier: AGPL-3.0-only

package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/thanos-io/thanos/pkg/block/metadata"
)

func (c *MimirClient) Backfill(ctx context.Context, source string, logger log.Logger) error {
	// Scan blocks in source directory
	es, err := os.ReadDir(source)
	if err != nil {
		return errors.Wrapf(err, "failed to read directory %q", source)
	}
	for _, e := range es {
		if err := c.backfillBlock(ctx, filepath.Join(source, e.Name()), logger); err != nil {
			return err
		}
	}

	return nil
}

func closeResp(resp *http.Response) {
	// Drain and close the body to let the transport reuse the connection
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
}

func (c *MimirClient) backfillBlock(ctx context.Context, dpath string, logger log.Logger) error {
	blockMeta, err := getBlockMeta(dpath)
	if err != nil {
		return err
	}

	blockID := blockMeta.ULID.String()

	level.Info(logger).Log("msg", "Making request to start block backfill", "user", c.id, "block", blockID)

	blockPrefix := path.Join("/api/v1/upload/block", url.PathEscape(blockID))

	buf := bytes.NewBuffer(nil)
	if err := json.NewEncoder(buf).Encode(blockMeta); err != nil {
		return errors.Wrap(err, "failed to JSON encode payload")
	}
	resp, err := c.doRequest(blockPrefix, http.MethodPost, buf, int64(buf.Len()))
	if err != nil {
		return errors.Wrap(err, "request to start backfill failed")
	}
	defer closeResp(resp)
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("request to start backfill failed, status code %d", resp.StatusCode)
	}

	// Upload each block file
	if err := filepath.WalkDir(dpath, func(pth string, e fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if e.IsDir() {
			return nil
		}

		if filepath.Base(pth) == "meta.json" {
			// Don't upload meta.json in this step
			return nil
		}

		f, err := os.Open(pth)
		if err != nil {
			return errors.Wrapf(err, "failed to open %q", pth)
		}
		defer f.Close()

		st, err := f.Stat()
		if err != nil {
			return errors.Wrap(err, "failed to get file info")
		}

		relPath, err := filepath.Rel(dpath, pth)
		if err != nil {
			return errors.Wrap(err, "failed to get relative path")
		}
		relPath = filepath.ToSlash(relPath)
		escapedPath := url.QueryEscape(relPath)
		level.Info(logger).Log("msg", "uploading block file", "path", pth, "user",
			c.id, "block", blockID, "size", st.Size())
		resp, err := c.doRequest(path.Join(blockPrefix, fmt.Sprintf("files?path=%s", escapedPath)), http.MethodPost, f, st.Size())
		if err != nil {
			return errors.Wrapf(err, "request to upload backfill of file %q failed", pth)
		}
		defer closeResp(resp)
		if resp.StatusCode/100 != 2 {
			return fmt.Errorf("request to upload backfill file failed, status code %d", resp.StatusCode)
		}

		return nil
	}); err != nil {
		return errors.Wrapf(err, "failed to traverse %q", dpath)
	}

	resp, err = c.doRequest(fmt.Sprintf("%s?uploadComplete=true", blockPrefix), http.MethodPost,
		nil, -1)
	if err != nil {
		return errors.Wrap(err, "request to finish backfill failed")
	}
	defer closeResp(resp)
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("request to finish backfill failed, status code %d", resp.StatusCode)
	}

	level.Info(logger).Log("msg", "Block backfill successful", "user", c.id, "block", blockID)

	return nil
}

func getBlockMeta(dpath string) (metadata.Meta, error) {
	var blockMeta metadata.Meta

	metaPath := filepath.Join(dpath, "meta.json")
	f, err := os.Open(metaPath)
	if err != nil {
		return blockMeta, errors.Wrapf(err, "failed to open %q", metaPath)
	}

	if err := json.NewDecoder(f).Decode(&blockMeta); err != nil {
		return blockMeta, errors.Wrapf(err, "failed to decode %q", metaPath)
	}

	idxPath := filepath.Join(dpath, "index")
	idxSt, err := os.Stat(idxPath)
	if err != nil {
		return blockMeta, errors.Wrapf(err, "failed to stat %q", idxPath)
	}
	blockMeta.Thanos.Files = []metadata.File{
		{
			RelPath:   "index",
			SizeBytes: idxSt.Size(),
		},
		{
			RelPath: "meta.json",
		},
	}

	chunksDir := filepath.Join(dpath, "chunks")
	entries, err := os.ReadDir(chunksDir)
	if err != nil {
		return blockMeta, errors.Wrapf(err, "failed to read dir %q", chunksDir)
	}
	for _, e := range entries {
		pth := filepath.Join(chunksDir, e.Name())
		st, err := os.Stat(pth)
		if err != nil {
			return blockMeta, errors.Wrapf(err, "failed to stat %q", pth)
		}

		blockMeta.Thanos.Files = append(blockMeta.Thanos.Files, metadata.File{
			RelPath:   path.Join("chunks", e.Name()),
			SizeBytes: st.Size(),
		})
	}

	return blockMeta, nil
}
