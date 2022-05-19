// SPDX-License-Identifier: AGPL-3.0-only

package compactor

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"regexp"

	"github.com/go-kit/log/level"
	"github.com/gorilla/mux"
	"github.com/thanos-io/thanos/pkg/block/metadata"

	"github.com/grafana/dskit/tenant"

	"github.com/grafana/mimir/pkg/storage/bucket"
)

// CreateBlockUpload handles requests for creating block upload sessions.
func (c *MultitenantCompactor) CreateBlockUpload(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	blockID := vars["block"]
	if blockID == "" {
		http.Error(w, "missing block ID", http.StatusBadRequest)
		return
	}
	tenantID, _, err := tenant.ExtractTenantIDFromHTTPRequest(r)
	if err != nil {
		http.Error(w, "invalid tenant ID", http.StatusBadRequest)
		return
	}

	level.Debug(c.logger).Log("msg", "creating block upload session", "user", tenantID, "block_id", blockID)
	// TODO: Verify that block hasn't already been ingested

	w.WriteHeader(http.StatusOK)
}

// UploadBlockFile handles requests for uploading block files.
func (c *MultitenantCompactor) UploadBlockFile(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	blockID := vars["block"]
	if blockID == "" {
		http.Error(w, "missing block ID", http.StatusBadRequest)
		return
	}
	pth, err := url.QueryUnescape(vars["path"])
	if err != nil {
		http.Error(w, fmt.Sprintf("malformed file path: %q", vars["path"]), http.StatusBadRequest)
		return
	}
	if pth == "" {
		http.Error(w, "missing file path", http.StatusBadRequest)
		return
	}

	tenantID, ctx, err := tenant.ExtractTenantIDFromHTTPRequest(r)
	if err != nil {
		http.Error(w, "invalid tenant ID", http.StatusBadRequest)
		return
	}

	if path.Base(pth) == "meta.json" {
		http.Error(w, "meta.json is not allowed", http.StatusBadRequest)
		return
	}

	rePath := regexp.MustCompile(`^(index|chunks/\d{6})$`)
	if !rePath.MatchString(pth) {
		http.Error(w, fmt.Sprintf("invalid path: %q", pth), http.StatusBadRequest)
		return
	}

	dst := path.Join(blockID, pth)

	if r.Body == nil || r.ContentLength == 0 {
		http.Error(w, "file cannot be empty", http.StatusBadRequest)
		return
	}

	level.Debug(c.logger).Log("msg", "uploading block file to bucket", "user", tenantID,
		"destination", dst, "size", r.ContentLength)
	bkt := bucket.NewUserBucketClient(string(tenantID), c.bucketClient, c.cfgProvider)
	reader := bodyReader{
		r: r,
	}
	if err := bkt.Upload(ctx, dst, reader); err != nil {
		level.Error(c.logger).Log("msg", "failed uploading block file to bucket",
			"user", tenantID, "destination", dst, "err", err)
		http.Error(w, "failed uploading block file to bucket", http.StatusBadGateway)
		return
	}

	level.Debug(c.logger).Log("msg", "finished uploading block file to bucket",
		"user", tenantID, "block_id", blockID, "path", pth)

	w.WriteHeader(http.StatusOK)
}

// CompleteBlockUpload handles a request to complete a block upload session.
func (c *MultitenantCompactor) CompleteBlockUpload(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	blockID := vars["block"]
	if blockID == "" {
		http.Error(w, "missing block ID", http.StatusBadRequest)
		return
	}

	tenantID, ctx, err := tenant.ExtractTenantIDFromHTTPRequest(r)
	if err != nil {
		http.Error(w, "invalid tenant ID", http.StatusBadRequest)
		return
	}

	level.Debug(c.logger).Log("msg", "received request to complete block upload", "user", tenantID,
		"block_id", blockID, "content_length", r.ContentLength)
	dec := json.NewDecoder(r.Body)
	var meta metadata.Meta
	if err := dec.Decode(&meta); err != nil {
		http.Error(w, "malformed request body", http.StatusBadRequest)
		return
	}

	level.Debug(c.logger).Log("msg", "completing block upload", "user",
		tenantID, "block_id", blockID, "files", len(meta.Thanos.Files))
	bkt := bucket.NewUserBucketClient(tenantID, c.bucketClient, c.cfgProvider)

	// Write meta.json, so the block is considered complete
	dst := path.Join(blockID, "meta.json")
	level.Debug(c.logger).Log("msg", "writing meta.json in bucket", "dst", dst)
	buf := bytes.NewBuffer(nil)
	enc := json.NewEncoder(buf)
	if err := enc.Encode(meta); err != nil {
		level.Error(c.logger).Log("msg", "failed to encode meta.json", "user", tenantID,
			"block_id", blockID, "err", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	if err := bkt.Upload(ctx, dst, buf); err != nil {
		level.Error(c.logger).Log("msg", "failed uploading meta.json to bucket", "user", tenantID,
			"dst", dst, "err", err)
		http.Error(w, "failed uploading meta.json to bucket", http.StatusBadGateway)
		return
	}

	level.Debug(c.logger).Log("msg", "successfully completed block upload")

	w.WriteHeader(http.StatusOK)
}

type bodyReader struct {
	r *http.Request
}

// ObjectSize implements thanos.ObjectSizer.
func (r bodyReader) ObjectSize() (int64, error) {
	return r.r.ContentLength, nil
}

// Read implements io.Reader.
func (r bodyReader) Read(b []byte) (int, error) {
	return r.r.Body.Read(b)
}
