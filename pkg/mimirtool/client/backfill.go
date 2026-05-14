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
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
)

func (c *MimirClient) Backfill(ctx context.Context, blocks []string, sleepTime time.Duration) error {
	// Upload each block
	var succeeded, failed, alreadyExists int

	for _, b := range blocks {
		logctx := log.With(c.logger, "path", b)
		if err := c.backfillBlock(ctx, b, logctx, sleepTime); err != nil {
			if errors.Is(err, errConflict) {
				level.Warn(logctx).Log("msg", "block already exists on the server")
				alreadyExists++
			} else {
				level.Error(logctx).Log("msg", "failed uploading block", "err", err)
				failed++
			}
			continue
		}

		// no logging, backfillBlock already logged result.
		succeeded++
	}

	level.Info(c.logger).Log("msg", "finished uploading blocks", "succeeded", succeeded, "already_exists", alreadyExists, "failed", failed)

	if failed > 0 {
		return fmt.Errorf("blocks failed to upload %d block(s)", failed)
	}

	return nil
}

// drainAndCloseBody drains and closes the body to let the transport reuse the connection.
func drainAndCloseBody(resp *http.Response) {
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
}

func (c *MimirClient) backfillBlock(ctx context.Context, blockDir string, logctx log.Logger, sleepTime time.Duration) error {
	// blockMeta returned by getBlockMeta will have thanos.files section pre-populated.
	blockMeta, err := GetBlockMeta(blockDir)
	if err != nil {
		return err
	}

	blockID := blockMeta.ULID.String()
	logctx = log.With(logctx, "block", blockID)

	level.Info(logctx).Log("msg", "making request to start block upload", "file", block.MetaFilename)

	const (
		endpointPrefix    = "/api/v1/upload/block"
		startBlockUpload  = "start"
		uploadFile        = "files"
		finishBlockUpload = "finish"
		checkBlockUpload  = "check"
	)

	buf := bytes.NewBuffer(nil)
	if err := json.NewEncoder(buf).Encode(blockMeta); err != nil {
		return errors.Wrap(err, "failed to JSON encode payload")
	}
	resp, err := c.doRequest(ctx, path.Join(endpointPrefix, url.PathEscape(blockID), startBlockUpload), http.MethodPost, buf, int64(buf.Len()))
	if err != nil {
		return errors.Wrap(err, "request to start block upload failed")
	}
	drainAndCloseBody(resp)

	// Upload each block file
	for _, tf := range blockMeta.Thanos.Files {
		if tf.RelPath == block.MetaFilename {
			// Don't upload meta file in this step
			continue
		}

		if err := c.uploadBlockFile(ctx, tf, blockDir, path.Join(endpointPrefix, url.PathEscape(blockID), uploadFile), logctx); err != nil {
			return err
		}
	}

	for {
		resp, err = c.doRequest(ctx, path.Join(endpointPrefix, url.PathEscape(blockID), finishBlockUpload), http.MethodPost, nil, -1)
		if err == nil {
			drainAndCloseBody(resp)
			break
		}
		if !errors.Is(err, errTooManyRequests) {
			return errors.Wrap(err, "request to finish block upload failed")
		}
		level.Warn(logctx).Log("msg", "will sleep and try again", "err", err)
		time.Sleep(sleepTime)
	}

	for {
		uploadResult, err := c.getBlockUpload(ctx, path.Join(endpointPrefix, url.PathEscape(blockID), checkBlockUpload))
		if err != nil {
			return errors.Wrap(err, "failed to check state of block upload")
		}
		level.Debug(logctx).Log("msg", "checked block upload state", "state", uploadResult.State)

		if uploadResult.State == "complete" {
			level.Info(logctx).Log("msg", "block uploaded successfully")
			return nil
		}

		if uploadResult.State == "failed" {
			return errors.Errorf("block validation failed: %s", uploadResult.Error)
		}

		// Sleep and then try to get the state again.
		time.Sleep(sleepTime)
	}
}

type result struct {
	State string `json:"result"`
	Error string `json:"error,omitempty"`
}

func (c *MimirClient) getBlockUpload(ctx context.Context, url string) (result, error) {
	resp, err := c.doRequest(ctx, url, http.MethodGet, nil, -1)
	if err != nil {
		return result{}, err
	}
	defer drainAndCloseBody(resp)

	var r result

	dec := json.NewDecoder(resp.Body)
	if err := dec.Decode(&r); err != nil {
		return result{}, err
	}

	return r, nil
}

func (c *MimirClient) uploadBlockFile(ctx context.Context, tf block.File, blockDir, fileUploadEndpoint string, logctx log.Logger) error {
	pth := filepath.Join(blockDir, filepath.FromSlash(tf.RelPath))
	f, err := os.Open(pth)
	if err != nil {
		return errors.Wrapf(err, "failed to open %q", pth)
	}
	defer func() {
		_ = f.Close()
	}()

	level.Info(logctx).Log("msg", "uploading block file", "file", tf.RelPath, "size", tf.SizeBytes)

	resp, err := c.doRequest(ctx, fmt.Sprintf("%s?path=%s", fileUploadEndpoint, url.QueryEscape(tf.RelPath)), http.MethodPost, f, tf.SizeBytes)
	if err != nil {
		return errors.Wrapf(err, "request to upload file %q failed", pth)
	}
	drainAndCloseBody(resp)

	return nil
}

// GetBlockMeta reads meta.json file, and adds (or replaces) thanos.files section with
// list of local files from the local block.
func GetBlockMeta(blockDir string) (block.Meta, error) {
	var blockMeta block.Meta

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

	if blockMeta.Version != 1 {
		return blockMeta, errors.Errorf("only version 1 of %s is supported, found: %d",
			block.MetaFilename, blockMeta.Version)
	}

	blockMeta.Thanos.Files = []block.File{
		{
			RelPath: block.MetaFilename,
		},
	}

	relPaths := []string{block.IndexFilename}

	// Add segment files to relPaths.
	{
		chunksDir := filepath.Join(blockDir, block.ChunksDirname)
		entries, err := os.ReadDir(chunksDir)
		if err != nil {
			return blockMeta, errors.Wrapf(err, "failed to read dir %q", chunksDir)
		}

		for _, c := range entries {
			relPaths = append(relPaths, path.Join(block.ChunksDirname, c.Name()))
		}
	}

	for _, relPath := range relPaths {
		p := filepath.Join(blockDir, filepath.FromSlash(relPath))
		st, err := os.Stat(p)
		if err != nil {
			return blockMeta, errors.Wrapf(err, "failed to stat %q", p)
		}

		if !st.Mode().IsRegular() {
			return blockMeta, fmt.Errorf("not a file: %q", p)
		}

		blockMeta.Thanos.Files = append(blockMeta.Thanos.Files, block.File{
			RelPath:   relPath,
			SizeBytes: st.Size(),
		})
	}

	return blockMeta, nil
}
