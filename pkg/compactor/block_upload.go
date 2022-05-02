// SPDX-License-Identifier: AGPL-3.0-only

package compactor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"path"

	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/thanos-io/thanos/pkg/block/metadata"

	"github.com/grafana/mimir/pkg/storage/bucket"
)

// CreateBlockUpload requests the creation of a block upload session.
func (c *MultitenantCompactor) CreateBlockUpload(ctx context.Context, tenantID, blockID string) error {
	level.Info(c.logger).Log("msg", "creating block upload session", "user", tenantID, "block_id", blockID)
	// TODO: Verify that block hasn't already been ingested
	return nil
}

// UploadBlockFile uploads a block file.
func (c *MultitenantCompactor) UploadBlockFile(ctx context.Context, tenantID, blockID, pth string, r *http.Request) error {
	if path.Base(pth) == "meta.json" {
		return fmt.Errorf("meta.json is not allowed")
	}

	level.Info(c.logger).Log("msg", "uploading block file", "user", tenantID,
		"block_id", blockID, "path", pth, "size", r.ContentLength)

	dst := path.Join(blockID, pth)
	level.Info(c.logger).Log("msg", "uploading block file to bucket", "user", tenantID,
		"destination", dst)
	bkt := bucket.NewUserBucketClient(string(tenantID), c.bucketClient, c.cfgProvider)
	reader := bodyReader{
		r: r,
	}
	if err := bkt.Upload(ctx, dst, &reader); err != nil {
		return errors.Wrap(err, "failed uploading block file to bucket")
	}

	level.Info(c.logger).Log("msg", "finished uploading block file to bucket", "user",
		tenantID, "block_id", blockID, "path", pth)

	return nil
}

type bodyReader struct {
	r *http.Request
}

// ObjectSize implements thanos.ObjectSizer.
func (r *bodyReader) ObjectSize() (int64, error) {
	return r.r.ContentLength, nil
}

// Read implements io.Reader.
func (r *bodyReader) Read(b []byte) (int, error) {
	return r.r.Body.Read(b)
}

// CompleteBlockUpload completes a block upload session.
func (c *MultitenantCompactor) CompleteBlockUpload(ctx context.Context, tenantID, blockID string, r *http.Request) error {
	level.Info(c.logger).Log("msg", "completing block upload", "user", tenantID, "block_id", blockID, "content_length", r.ContentLength)
	dec := json.NewDecoder(r.Body)

	var meta metadata.Meta
	if err := dec.Decode(&meta); err != nil {
		return errors.Wrap(err, "failed to decode meta.json")
	}

	level.Info(c.logger).Log("msg", "processing request to complete block upload", "user",
		tenantID, "block_id", blockID, "files", len(meta.Thanos.Files))
	bkt := bucket.NewUserBucketClient(tenantID, c.bucketClient, c.cfgProvider)

	// Write meta.json, so the block is considered complete
	dst := path.Join(blockID, "meta.json")
	level.Info(c.logger).Log("msg", "writing meta.json in bucket", "dst", dst)
	buf := bytes.NewBuffer(nil)
	enc := json.NewEncoder(buf)
	if err := enc.Encode(meta); err != nil {
		return errors.Wrap(err, "failed to JSON encode meta.json")
	}
	if err := bkt.Upload(ctx, dst, buf); err != nil {
		return errors.Wrap(err, "failed uploading meta.json to bucket")
	}

	level.Info(c.logger).Log("msg", "successfully completed block upload")
	return nil
}
