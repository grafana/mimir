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
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/thanos-io/thanos/pkg/block"

	"github.com/grafana/mimir/pkg/storage/tsdb/metadata"
)

func (c *MimirClient) Backfill(blocks []string, sleepTime time.Duration) error {
	// Upload each block
	var succeeded, failed, alreadyExists int

	for _, b := range blocks {
		logctx := logrus.WithFields(logrus.Fields{"path": b})
		if err := c.backfillBlock(b, logctx, sleepTime); err != nil {
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

func (c *MimirClient) backfillBlock(blockDir string, logctx *logrus.Entry, sleepTime time.Duration) error {
	// blockMeta returned by getBlockMeta will have thanos.files section pre-populated.
	blockMeta, err := getBlockMeta(blockDir)
	if err != nil {
		return err
	}

	blockID := blockMeta.ULID.String()
	logctx = logctx.WithFields(logrus.Fields{"block": blockID})

	logctx.WithField("file", block.MetaFilename).Info("making request to start block upload")

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

	for {
		uploadResult, err := c.getBlockUpload(path.Join(endpointPrefix, url.PathEscape(blockID), checkBlockUpload))
		if err != nil {
			return errors.Wrap(err, "failed to check state of block upload")
		}
		logctx.WithField("state", uploadResult.State).Debug("checked block upload state")

		if uploadResult.State == "complete" {
			logctx.Info("block uploaded successfully")
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

func (c *MimirClient) getBlockUpload(url string) (result, error) {
	resp, err := c.doRequest(url, http.MethodGet, nil, -1)
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
		return blockMeta, errors.Errorf("only version 1 of %s is supported, found: %d",
			block.MetaFilename, blockMeta.Version)
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
