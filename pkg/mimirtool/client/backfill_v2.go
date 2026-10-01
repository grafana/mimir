// SPDX-License-Identifier: AGPL-3.0-only

package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"path"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/backoff"
	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/filesystem"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
)

const backfillV2EndpointPrefix = "/api/v1/backfill"

type backfillJobStartResult struct {
	JobID string `json:"job"`
}

type backfillRequestBody func() (body io.ReadCloser, size int64, err error)

// TODO: add manifest support
func (c *MimirClient) StartBackfillJob(ctx context.Context) (string, error) {
	resp, _, err := c.sendBackfillV2Request(ctx, path.Join(backfillV2EndpointPrefix, "start"), nil)
	if err != nil {
		return "", errors.Wrap(err, "failed to start backfill job")
	}
	defer drainAndCloseBody(resp)

	var res backfillJobStartResult
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return "", errors.Wrap(err, "failed to decode backfill job start response")
	}
	if res.JobID == "" {
		return "", errors.New("backfill job start response has no job ID")
	}
	return res.JobID, nil
}

func (c *MimirClient) FinishBackfillJob(ctx context.Context, jobID string) error {
	if _, err := c.doBackfillV2Request(ctx, path.Join(backfillV2EndpointPrefix, url.PathEscape(jobID), "finish"), nil); err != nil {
		return errors.Wrapf(err, "failed to finish backfill job %s", jobID)
	}
	return nil
}

func (c *MimirClient) UploadBackfillBlocks(ctx context.Context, jobID string, blockDirs []string) error {
	logger := log.With(c.logger, "job", jobID)

	var succeeded, alreadyExists int
	var failed []string
	// TODO: upload blocks in parallel.
	for _, blockDir := range blockDirs {
		blockLogger := log.With(logger, "path", blockDir)

		err := c.uploadBackfillBlock(ctx, jobID, blockDir, blockLogger)
		switch {
		case err == nil:
			succeeded++
		case errors.Is(err, ErrConflict):
			level.Warn(blockLogger).Log("msg", "block already exists in the backfill job")
			alreadyExists++
		default:
			level.Error(blockLogger).Log("msg", "failed uploading block", "err", err)
			failed = append(failed, blockDir)
		}
	}

	level.Info(logger).Log("msg", "finished uploading blocks", "succeeded", succeeded, "already_exists", alreadyExists, "failed", len(failed))

	if len(failed) > 0 {
		return fmt.Errorf("failed to upload %d block(s) to backfill job %s: %s", len(failed), jobID, strings.Join(failed, ", "))
	}
	return nil
}

func (c *MimirClient) uploadBackfillBlock(ctx context.Context, jobID, blockDir string, logger log.Logger) error {
	bkt, err := filesystem.NewBucket(filepath.Dir(blockDir))
	if err != nil {
		return errors.Wrap(err, "failed to create filesystem bucket")
	}

	blockID, err := ulid.Parse(filepath.Base(blockDir))
	if err != nil {
		return errors.Wrap(err, "failed to parse block ID from path")
	}

	meta, err := GetBlockMeta(ctx, bkt, blockID)
	if err != nil {
		return err
	}

	metaJSON, err := json.Marshal(meta)
	if err != nil {
		return errors.Wrap(err, "failed to JSON encode block meta")
	}

	blockPath := path.Join(backfillV2EndpointPrefix, url.PathEscape(jobID), "block", blockID.String())
	logger = log.With(logger, "block", blockID)

	level.Info(logger).Log("msg", "starting block upload")
	if _, err := c.doBackfillV2Request(ctx, path.Join(blockPath, "start"), bytesRequestBody(metaJSON)); err != nil {
		return errors.Wrap(err, "request to start block upload failed")
	}

	// TODO: support for skipping already uploaded files
	for _, f := range meta.Thanos.Files {
		if f.RelPath == block.MetaFilename {
			continue
		}

		level.Info(logger).Log("msg", "uploading block file", "file", f.RelPath, "size", f.SizeBytes)
		filePath := fmt.Sprintf("%s?path=%s", path.Join(blockPath, "files"), url.QueryEscape(f.RelPath))
		if _, err := c.doBackfillV2Request(ctx, filePath, bucketObjectRequestBody(ctx, bkt, path.Join(blockID.String(), f.RelPath), f.SizeBytes)); err != nil {
			return errors.Wrapf(err, "request to upload file %q failed", f.RelPath)
		}
	}

	retried, err := c.doBackfillV2Request(ctx, path.Join(blockPath, "finish"), nil)
	if retried && errors.Is(err, ErrConflict) {
		level.Debug(logger).Log("msg", "an earlier attempt already finished the block upload")
	} else if err != nil {
		return errors.Wrap(err, "request to finish block upload failed")
	}

	level.Info(logger).Log("msg", "block uploaded successfully")
	return nil
}

func (c *MimirClient) doBackfillV2Request(ctx context.Context, path string, newBody backfillRequestBody) (bool, error) {
	retries := backoff.New(ctx, backoff.Config{
		MinBackoff: time.Second,
		MaxBackoff: 30 * time.Second,
		MaxRetries: 10,
	})
	for {
		resp, retryable, err := c.sendBackfillV2Request(ctx, path, newBody)
		if err == nil {
			drainAndCloseBody(resp)
		}
		if err == nil || !retryable {
			return retries.NumRetries() > 0, err
		}

		retries.Wait()
		if !retries.Ongoing() {
			return true, err
		}
		level.Warn(c.logger).Log("msg", "retrying backfill request", "path", path, "retry", retries.NumRetries(), "err", err)
	}
}

func (c *MimirClient) sendBackfillV2Request(ctx context.Context, path string, newBody backfillRequestBody) (*http.Response, bool, error) {
	var payload io.Reader
	contentLength := int64(-1)
	if newBody != nil {
		body, size, err := newBody()
		if err != nil {
			return nil, false, err
		}
		defer func() { _ = body.Close() }()
		payload, contentLength = body, size
	}

	req, resp, err := c.executeRequest(ctx, path, http.MethodPost, payload, contentLength)
	if err != nil {
		return nil, ctx.Err() == nil && isTransientNetworkError(err), err
	}

	retryable := resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= http.StatusInternalServerError
	if err := c.backfillResponseError(req, resp); err != nil {
		return nil, retryable, err
	}
	return resp, false, nil
}

func isTransientNetworkError(err error) bool {
	var netErr net.Error
	return (errors.As(err, &netErr) && netErr.Timeout()) ||
		errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.EPIPE) ||
		errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrUnexpectedEOF)
}

func bytesRequestBody(b []byte) backfillRequestBody {
	return func() (io.ReadCloser, int64, error) {
		return io.NopCloser(bytes.NewReader(b)), int64(len(b)), nil
	}
}

func bucketObjectRequestBody(ctx context.Context, bkt objstore.BucketReader, name string, size int64) backfillRequestBody {
	return func() (io.ReadCloser, int64, error) {
		r, err := bkt.Get(ctx, name)
		if err != nil {
			return nil, 0, errors.Wrapf(err, "failed to read %q", name)
		}
		return r, size, nil
	}
}
