// SPDX-License-Identifier: AGPL-3.0-only

package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
)

func (c *MimirClient) Backfill(blocks []string) error {
	// Upload each block
	var succeeded, failed, alreadyExists int

	for _, b := range blocks {
		logctx := logrus.WithFields(logrus.Fields{"path": b})
		if err := c.backfillBlock(b, logctx); err != nil {
			if errors.Is(err, errConflict) {
				logctx.Warning("block already exists on the server")
				alreadyExists++
			} else {
				logctx.WithField("error", err).Error("failed uploading block")
				failed++
			}
			continue
		}

		// no logging, backfillBlock already logged result.
		succeeded++
	}

	logrus.WithFields(logrus.Fields{"succeeded": succeeded, "already_exists": alreadyExists, "failed": failed}).Info("finished uploading blocks")

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

func (c *MimirClient) backfillBlock(blockDir string, logctx *logrus.Entry) error {
	// blockMeta returned by getBlockMeta will have thanos.files section pre-populated.
	blockMeta, err := getBlockMeta(blockDir)
	if err != nil {
		return err
	}

	blockID := blockMeta.ULID.String()
	logctx = logctx.WithFields(logrus.Fields{"block": blockID})

	logctx.WithField("file", "meta.json").Info("making request to start block upload")

	const (
		endpointPrefix    = "/api/v1/upload/block"
		startBlockUpload  = "start"
		uploadFile        = "files"
		finishBlockUpload = "finish"
	)

	buf := bytes.NewBuffer(nil)
	if err := json.NewEncoder(buf).Encode(blockMeta); err != nil {
		return errors.Wrap(err, "failed to JSON encode payload")
	}
	resp, err := c.doRequest(path.Join(endpointPrefix, url.PathEscape(blockID), startBlockUpload), http.MethodPost, buf, int64(buf.Len()))
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

		if err := c.uploadBlockFile(tf, blockDir, path.Join(endpointPrefix, url.PathEscape(blockID), uploadFile), logctx); err != nil {
			return err
		}
	}

	resp, err = c.doRequest(path.Join(endpointPrefix, url.PathEscape(blockID), finishBlockUpload), http.MethodPost, nil, -1)
	if err != nil {
		return errors.Wrap(err, "request to finish block upload failed")
	}
	drainAndCloseBody(resp)

	logctx.Info("block uploaded successfully")

	return nil
}

func (c *MimirClient) uploadBlockFile(tf metadata.File, blockDir, fileUploadEndpoint string, logctx *logrus.Entry) error {
	pth := filepath.Join(blockDir, filepath.FromSlash(tf.RelPath))
	f, err := os.Open(pth)
	if err != nil {
		return errors.Wrapf(err, "failed to open %q", pth)
	}
	defer func() {
		_ = f.Close()
	}()

	logctx.WithFields(logrus.Fields{"file": tf.RelPath, "size": tf.SizeBytes}).Info("uploading block file")

	resp, err := c.doRequest(fmt.Sprintf("%s?path=%s", fileUploadEndpoint, url.QueryEscape(tf.RelPath)), http.MethodPost, f, tf.SizeBytes)
	if err != nil {
		return errors.Wrapf(err, "request to upload file %q failed", pth)
	}
	drainAndCloseBody(resp)

	return nil
}

// getBlockMeta reads meta.json file, and adds (or replaces) thanos.files section with
// list of local files from the local block.
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

	if blockMeta.Version != 1 {
		return blockMeta, errors.Errorf("only version 1 of meta.json is supported, found: %d", blockMeta.Version)
	}

	blockMeta.Thanos.Files = []metadata.File{
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

		blockMeta.Thanos.Files = append(blockMeta.Thanos.Files, metadata.File{
			RelPath:   relPath,
			SizeBytes: st.Size(),
		})
	}

	return blockMeta, nil
}
