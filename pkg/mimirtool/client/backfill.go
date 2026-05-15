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
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/filesystem"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
)

var ErrBlockInvalid = errors.New("block is invalid")

func (c *MimirClient) doBackfillRequest(ctx context.Context, path, method string, payload io.Reader, contentLength int64) (*http.Response, error) {
	req, resp, err := c.executeRequest(ctx, path, method, payload, contentLength)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode == http.StatusRequestEntityTooLarge || resp.StatusCode == http.StatusUnprocessableEntity {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		_ = resp.Body.Close()
		return nil, fmt.Errorf("%w: %s %s: %s", ErrBlockInvalid, req.Method, req.URL.String(), body)
	}

	if err := c.checkResponse(resp); err != nil {
		_ = resp.Body.Close()
		return nil, errors.Wrapf(err, "%s request to %s failed", req.Method, req.URL.String())
	}

	return resp, nil
}

func (c *MimirClient) Backfill(ctx context.Context, blocks []string, sleepTime time.Duration) error {
	// Upload each block
	var succeeded, failed, alreadyExists int

	buckets := make(map[string]objstore.BucketReader, 1)
	for _, b := range blocks {
		logctx := log.With(c.logger, "path", b)

		dir := filepath.Dir(b)
		fsBkt, ok := buckets[dir]
		if !ok {
			var err error
			fsBkt, err = filesystem.NewBucket(dir)
			if err != nil {
				level.Error(logctx).Log("msg", "failed to create filesystem bucket", "err", err)
				failed++
				continue
			}
			buckets[dir] = fsBkt
		}

		blockID, err := ulid.Parse(filepath.Base(b))
		if err != nil {
			level.Error(logctx).Log("msg", "failed to parse block ID from path", "err", err)
			failed++
			continue
		}

		if err := c.backfillBlock(ctx, fsBkt, blockID, logctx, sleepTime); err != nil {
			if errors.Is(err, ErrConflict) {
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

// BackfillBlock uploads a single TSDB block from a bucket to a Mimir instance
// via the block upload API.
func (c *MimirClient) BackfillBlock(ctx context.Context, bkt objstore.BucketReader, blockID ulid.ULID, sleepTime time.Duration) error {
	return c.backfillBlock(ctx, bkt, blockID, c.logger, sleepTime)
}

// drainAndCloseBody drains and closes the body to let the transport reuse the connection.
func drainAndCloseBody(resp *http.Response) {
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
}

func (c *MimirClient) backfillBlock(ctx context.Context, bkt objstore.BucketReader, blockID ulid.ULID, logctx log.Logger, sleepTime time.Duration) error {
	// blockMeta returned by getBlockMetaFromBucket will have thanos.files section pre-populated.
	blockMeta, err := GetBlockMeta(ctx, bkt, blockID)
	if err != nil {
		return err
	}

	const (
		endpointPrefix    = "/api/v1/upload/block"
		startBlockUpload  = "start"
		uploadFile        = "files"
		finishBlockUpload = "finish"
		checkBlockUpload  = "check"
	)

	blockIDStr := blockMeta.ULID.String()
	blockPath := path.Join(endpointPrefix, url.PathEscape(blockIDStr))
	logctx = log.With(logctx, "block", blockIDStr)

	level.Info(logctx).Log("msg", "making request to start block upload", "file", block.MetaFilename)

	buf := bytes.NewBuffer(nil)
	if err := json.NewEncoder(buf).Encode(blockMeta); err != nil {
		return errors.Wrap(err, "failed to JSON encode payload")
	}
	resp, err := c.doBackfillRequest(ctx, path.Join(blockPath, startBlockUpload), http.MethodPost, buf, int64(buf.Len()))
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

		if err := c.uploadBlockFile(ctx, bkt, blockID, tf, path.Join(blockPath, uploadFile), logctx); err != nil {
			return err
		}
	}

	for {
		resp, err = c.doBackfillRequest(ctx, path.Join(blockPath, finishBlockUpload), http.MethodPost, nil, -1)
		if err == nil {
			drainAndCloseBody(resp)
			break
		}
		if !errors.Is(err, errTooManyRequests) {
			return errors.Wrap(err, "request to finish block upload failed")
		}
		level.Warn(logctx).Log("msg", "will sleep and try again", "err", err)
		select {
		case <-time.After(sleepTime):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	for {
		uploadResult, err := c.getBlockUpload(ctx, path.Join(blockPath, checkBlockUpload))
		if err != nil {
			return errors.Wrap(err, "failed to check state of block upload")
		}
		level.Debug(logctx).Log("msg", "checked block upload state", "state", uploadResult.State)

		if uploadResult.State == "complete" {
			level.Info(logctx).Log("msg", "block uploaded successfully")
			return nil
		}

		if uploadResult.State == "failed" {
			return fmt.Errorf("%w: block validation failed: %s", ErrBlockInvalid, uploadResult.Error)
		}

		// Sleep and then try to get the state again.
		select {
		case <-time.After(sleepTime):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

type result struct {
	State string `json:"result"`
	Error string `json:"error,omitempty"`
}

func (c *MimirClient) getBlockUpload(ctx context.Context, url string) (result, error) {
	resp, err := c.doBackfillRequest(ctx, url, http.MethodGet, nil, -1)
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

func (c *MimirClient) uploadBlockFile(ctx context.Context, bkt objstore.BucketReader, blockID ulid.ULID, tf block.File, fileUploadEndpoint string, logctx log.Logger) error {
	objectName := path.Join(blockID.String(), tf.RelPath)
	r, err := bkt.Get(ctx, objectName)
	if err != nil {
		return errors.Wrapf(err, "failed to read %q from bucket", objectName)
	}
	defer func() {
		_ = r.Close()
	}()

	level.Info(logctx).Log("msg", "uploading block file", "file", tf.RelPath, "size", tf.SizeBytes)

	resp, err := c.doBackfillRequest(ctx, fmt.Sprintf("%s?path=%s", fileUploadEndpoint, url.QueryEscape(tf.RelPath)), http.MethodPost, r, tf.SizeBytes)
	if err != nil {
		return errors.Wrapf(err, "request to upload file %q failed", objectName)
	}
	drainAndCloseBody(resp)

	return nil
}

// GetBlockMeta reads meta.json from the bucket and adds (or replaces)
// the thanos.files section with the list of files from the block in the bucket.
func GetBlockMeta(ctx context.Context, bkt objstore.BucketReader, blockID ulid.ULID) (block.Meta, error) {
	var blockMeta block.Meta

	metaPath := path.Join(blockID.String(), block.MetaFilename)
	r, err := bkt.Get(ctx, metaPath)
	if err != nil {
		return blockMeta, errors.Wrapf(err, "failed to read %q", metaPath)
	}

	decErr := json.NewDecoder(r).Decode(&blockMeta)
	closeErr := r.Close()
	if decErr != nil {
		return blockMeta, errors.Wrapf(decErr, "failed to decode %q", metaPath)
	}
	if closeErr != nil {
		return blockMeta, errors.Wrapf(closeErr, "failed to close %q", metaPath)
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
	blockPrefix := blockID.String()
	chunksPrefix := path.Join(blockPrefix, block.ChunksDirname)
	err = bkt.Iter(ctx, chunksPrefix, func(name string) error {
		rel := strings.TrimPrefix(name, blockPrefix+"/")
		relPaths = append(relPaths, rel)
		return nil
	})
	if err != nil {
		return blockMeta, errors.Wrapf(err, "failed to list chunks in %q", chunksPrefix)
	}

	for _, relPath := range relPaths {
		objPath := path.Join(blockPrefix, relPath)
		attrs, err := bkt.Attributes(ctx, objPath)
		if err != nil {
			return blockMeta, errors.Wrapf(err, "failed to get attributes for %q", objPath)
		}

		blockMeta.Thanos.Files = append(blockMeta.Thanos.Files, block.File{
			RelPath:   relPath,
			SizeBytes: attrs.Size,
		})
	}

	return blockMeta, nil
}
