// SPDX-License-Identifier: AGPL-3.0-only

package compactor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gorilla/mux"
	"github.com/grafana/mimir/pkg/util"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/objstore"

	"github.com/grafana/dskit/tenant"
	"github.com/grafana/regexp"

	"github.com/grafana/mimir/pkg/storage/bucket"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	util_log "github.com/grafana/mimir/pkg/util/log"
)

const tmpMetaFilename = "uploading-" + block.MetaFilename

var rePath = regexp.MustCompile(`^(index|chunks/\d{6})$`)

// HandleBlockUpload handles requests for starting or completing block uploads.
//
// The query parameter uploadComplete (true or false, default false) controls whether the
// upload should be completed or not.
//
// Starting the uploading of a block means to upload meta file and verify that the upload can go ahead.
// In practice this means to check that the (complete) block isn't already in block storage,
// and that meta file is valid.
func (c *MultitenantCompactor) HandleBlockUpload(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	blockID := vars["block"]
	if blockID == "" {
		http.Error(w, "missing block ID", http.StatusBadRequest)
		return
	}
	bULID, err := ulid.Parse(blockID)
	if err != nil {
		http.Error(w, "invalid block ID", http.StatusBadRequest)
		return
	}
	tenantID, ctx, err := tenant.ExtractTenantIDFromHTTPRequest(r)
	if err != nil {
		http.Error(w, "invalid tenant ID", http.StatusBadRequest)
		return
	}

	logger := util_log.WithContext(ctx, c.logger)
	logger = log.With(logger, "block", blockID)

	userBkt := bucket.NewUserBucketClient(tenantID, c.bucketClient, c.cfgProvider)
	if err := checkForComplete(ctx, logger, bULID, userBkt); err != nil {
		var httpErr httpError
		if errors.As(err, &httpErr) {
			level.Warn(logger).Log("msg", httpErr.message, "err", err)
			http.Error(w, httpErr.message, httpErr.statusCode)
			return
		}

		level.Error(logger).Log("msg", "an unexpected error occurred", "err", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
	}

	if r.URL.Query().Get("uploadComplete") == "true" {
		err = c.completeBlockUpload(ctx, r, logger, userBkt, tenantID, bULID)
	} else {
		err = c.createBlockUpload(ctx, r, logger, userBkt, tenantID, bULID)
	}
	if err != nil {
		var httpErr httpError
		if errors.As(err, &httpErr) {
			level.Warn(logger).Log("msg", httpErr.message, "err", err)
			http.Error(w, httpErr.message, httpErr.statusCode)
			return
		}

		level.Error(logger).Log("msg", "an unexpected error occurred", "err", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// checkForComplete checks for a complete block with same ID. If one exists, an error is returned.
func checkForComplete(ctx context.Context, logger log.Logger, blockID ulid.ULID, userBkt objstore.Bucket) error {
	exists, err := userBkt.Exists(ctx, path.Join(blockID.String(), block.MetaFilename))
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("failed to check existence of %s in object storage", block.MetaFilename))
	}
	if exists {
		level.Debug(logger).Log("msg", "complete block already exists in object storage")
		return httpError{
			message:    "block already exists in object storage",
			statusCode: http.StatusConflict,
		}
	}

	return nil
}

func (c *MultitenantCompactor) createBlockUpload(ctx context.Context, r *http.Request,
	logger log.Logger, userBkt objstore.Bucket, tenantID string, blockID ulid.ULID) error {
	level.Debug(logger).Log("msg", "starting block upload")

	meta, err := decodeMeta(r.Body, "request body")
	if err != nil {
		return httpError{
			message:    "malformed request body",
			statusCode: http.StatusBadRequest,
		}
	}

	if err := c.sanitizeMeta(logger, tenantID, blockID, &meta); err != nil {
		return err
	}

	return c.uploadMeta(ctx, logger, meta, blockID, tmpMetaFilename, userBkt)
}

// UploadBlockFile handles requests for uploading block files.
//
// It takes the mandatory query parameter "path", specifying the file's destination path.
func (c *MultitenantCompactor) UploadBlockFile(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	blockID := vars["block"]
	if blockID == "" {
		http.Error(w, "missing block ID", http.StatusBadRequest)
		return
	}
	bULID, err := ulid.Parse(blockID)
	if err != nil {
		http.Error(w, "invalid block ID", http.StatusBadRequest)
		return
	}
	pth := r.URL.Query().Get("path")
	if pth == "" {
		http.Error(w, "missing or invalid file path", http.StatusBadRequest)
		return
	}

	tenantID, ctx, err := tenant.ExtractTenantIDFromHTTPRequest(r)
	if err != nil {
		http.Error(w, "invalid tenant ID", http.StatusBadRequest)
		return
	}

	logger := util_log.WithContext(ctx, c.logger)
	logger = log.With(logger, "block", blockID)

	if path.Base(pth) == block.MetaFilename {
		http.Error(w, fmt.Sprintf("%s is not allowed", block.MetaFilename), http.StatusBadRequest)
		return
	}

	if !rePath.MatchString(pth) {
		http.Error(w, fmt.Sprintf("invalid path: %q", pth), http.StatusBadRequest)
		return
	}

	if r.ContentLength == 0 {
		http.Error(w, "file cannot be empty", http.StatusBadRequest)
		return
	}

	userBkt := bucket.NewUserBucketClient(tenantID, c.bucketClient, c.cfgProvider)

	if err := checkForComplete(ctx, logger, bULID, userBkt); err != nil {
		var httpErr httpError
		if errors.As(err, &httpErr) {
			level.Warn(logger).Log("msg", httpErr.message, "err", err)
			http.Error(w, httpErr.message, httpErr.statusCode)
			return
		}

		level.Error(logger).Log("msg", "an unexpected error occurred", "err", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
	}

	metaPath := path.Join(blockID, tmpMetaFilename)
	exists, err := userBkt.Exists(ctx, metaPath)
	if err != nil {
		level.Error(logger).Log("msg", fmt.Sprintf("failed to check existence of %s in object storage", tmpMetaFilename),
			"err", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	if !exists {
		http.Error(w, fmt.Sprintf("upload of block %s not started yet", blockID), http.StatusNotFound)
		return
	}

	// TODO: Verify that upload path and length correspond to file index

	dst := path.Join(blockID, pth)

	level.Debug(logger).Log("msg", "uploading block file to bucket", "destination", dst,
		"size", r.ContentLength)
	reader := bodyReader{
		r: r,
	}
	if err := userBkt.Upload(ctx, dst, reader); err != nil {
		level.Error(logger).Log("msg", "failed uploading block file to bucket",
			"destination", dst, "err", err)
		// We don't know what caused the error; it could be the client's fault (e.g. killed
		// connection), but internal server error is the safe choice here.
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	level.Debug(logger).Log("msg", "finished uploading block file to bucket",
		"path", pth)

	w.WriteHeader(http.StatusOK)
}

func decodeMeta(r io.Reader, name string) (metadata.Meta, error) {
	dec := json.NewDecoder(r)
	var meta metadata.Meta
	if err := dec.Decode(&meta); err != nil {
		return meta, errors.Wrap(err, fmt.Sprintf("failed decoding %s", name))
	}

	return meta, nil
}

func (c *MultitenantCompactor) completeBlockUpload(ctx context.Context, r *http.Request,
	logger log.Logger, userBkt objstore.Bucket, tenantID string, blockID ulid.ULID) error {
	level.Debug(logger).Log("msg", "received request to complete block upload", "content_length", r.ContentLength)

	tmpMetaPath := path.Join(blockID.String(), tmpMetaFilename)
	rdr, err := userBkt.Get(ctx, tmpMetaPath)
	if err != nil {
		if userBkt.IsObjNotFoundErr(err) {
			return httpError{
				message:    fmt.Sprintf("upload of block %s not started yet", blockID),
				statusCode: http.StatusNotFound,
			}
		}
		return errors.Wrap(err, fmt.Sprintf("failed to download %s from object storage", tmpMetaFilename))
	}
	defer func() {
		_ = rdr.Close()
	}()

	meta, err := decodeMeta(rdr, tmpMetaFilename)
	if err != nil {
		return err
	}

	level.Debug(logger).Log("msg", "completing block upload", "files", len(meta.Thanos.Files))

	// Upload meta file so block is considered complete
	if err := c.uploadMeta(ctx, logger, meta, blockID, block.MetaFilename, userBkt); err != nil {
		return err
	}

	if err := userBkt.Delete(ctx, path.Join(blockID.String(), tmpMetaFilename)); err != nil {
		level.Warn(logger).Log("msg", fmt.Sprintf(
			"failed to delete %s from block in object storage", tmpMetaFilename), "err", err)
		return nil
	}

	level.Debug(logger).Log("msg", "successfully completed block upload")
	return nil
}

func (c *MultitenantCompactor) sanitizeMeta(logger log.Logger, tenantID string, blockID ulid.ULID,
	meta *metadata.Meta) error {
	meta.ULID = blockID

	var rejLbls []string
	for l, v := range meta.Thanos.Labels {
		switch l {
		// Preserve these labels
		case mimir_tsdb.CompactorShardIDExternalLabel:
		// Remove unused labels
		case mimir_tsdb.DeprecatedTenantIDExternalLabel, mimir_tsdb.DeprecatedIngesterIDExternalLabel, mimir_tsdb.DeprecatedShardIDExternalLabel:
			level.Debug(logger).Log("msg", fmt.Sprintf("removing unused external label from %s", block.MetaFilename),
				"label", l, "value", v)
			delete(meta.Thanos.Labels, l)
		default:
			rejLbls = append(rejLbls, l)
		}
	}

	if len(rejLbls) > 0 {
		level.Warn(logger).Log("msg", fmt.Sprintf("rejecting unsupported external label(s) in %s", block.MetaFilename),
			"labels", strings.Join(rejLbls, ","))
		return httpError{
			message:    fmt.Sprintf("unsupported external label(s): %s", strings.Join(rejLbls, ",")),
			statusCode: http.StatusBadRequest,
		}
	}

	var rejFilesInvalidPath []string
	var rejFilesUnspecSize []string
	for _, f := range meta.Thanos.Files {
		if !rePath.MatchString(f.RelPath) {
			rejFilesInvalidPath = append(rejFilesInvalidPath, f.RelPath)
			continue
		}

		if f.RelPath == block.MetaFilename {
			continue
		}

		if f.SizeBytes <= 0 {
			rejFilesUnspecSize = append(rejFilesUnspecSize, f.RelPath)
		}
	}
	if len(rejFilesInvalidPath) > 0 {
		level.Warn(logger).Log("msg", fmt.Sprintf("rejecting files with invalid paths in %s", block.MetaFilename),
			"files", strings.Join(rejFilesInvalidPath, ","))
		return httpError{
			message:    fmt.Sprintf("file(s) with invalid paths: %s", strings.Join(rejFilesInvalidPath, ",")),
			statusCode: http.StatusBadRequest,
		}
	}
	if len(rejFilesUnspecSize) > 0 {
		level.Warn(logger).Log("msg", fmt.Sprintf("rejecting files with unspecified sizes in %s", block.MetaFilename),
			"files", strings.Join(rejFilesUnspecSize, ","))
		return httpError{
			message:    fmt.Sprintf("file(s) with unspecified sizes: %s", strings.Join(rejFilesUnspecSize, ",")),
			statusCode: http.StatusBadRequest,
		}
	}

	// validate minTime/maxTime
	// basic sanity check
	if meta.MinTime < 0 || meta.MaxTime < 0 || meta.MaxTime < meta.MinTime {
		level.Warn(logger).Log("msg", "invalid minTime/maxTime in meta.json", "minTime", meta.MinTime,
			"maxTime", meta.MaxTime)
		return httpError{
			message:    fmt.Sprintf("invalid minTime/maxTime in %s: minTime=%d, maxTime=%d", block.MetaFilename, meta.MinTime, meta.MaxTime),
			statusCode: http.StatusBadRequest,
		}
	}
	// validate that times are in the past
	nowUTC := time.Now().UTC()
	if time.UnixMilli(meta.MinTime).UTC().After(nowUTC) || time.UnixMilli(meta.MaxTime).UTC().After(nowUTC) {
		level.Warn(logger).Log("msg", "chunk times greater than the present", "minTime", meta.MinTime,
			"maxTime", meta.MaxTime)
		return httpError{
			message:    fmt.Sprintf("chunk times greater than the present: minTime=%d, maxTime=%d", meta.MinTime, meta.MaxTime),
			statusCode: http.StatusBadRequest,
		}
	}
	// validate data is within the retention period
	durationSinceMinTime := time.Since(time.UnixMilli(meta.MinTime).UTC())
	if durationSinceMinTime > c.storageCfg.TSDB.Retention {
		age := util.FormatTimeMillis(durationSinceMinTime.Milliseconds())
		level.Warn(logger).Log("msg", "chunk age older than retention period", "age", age)
		return httpError{
			message:    fmt.Sprintf("chunk age (%s) older than retention period", age),
			statusCode: http.StatusBadRequest,
		}
	}

	// Mark block source
	meta.Thanos.Source = "upload"

	return nil
}

func (c *MultitenantCompactor) uploadMeta(ctx context.Context, logger log.Logger, meta metadata.Meta,
	blockID ulid.ULID, name string, userBkt objstore.Bucket) error {
	dst := path.Join(blockID.String(), name)
	level.Debug(logger).Log("msg", fmt.Sprintf("uploading %s to bucket", name), "dst", dst)
	buf := bytes.NewBuffer(nil)
	enc := json.NewEncoder(buf)
	if err := enc.Encode(meta); err != nil {
		return errors.Wrap(err, "failed to encode block metadata")
	}
	if err := userBkt.Upload(ctx, dst, buf); err != nil {
		return errors.Wrapf(err, "failed uploading %s to bucket", name)
	}

	return nil
}

type httpError struct {
	message    string
	statusCode int
}

func (e httpError) Error() string {
	return e.message
}

type bodyReader struct {
	r *http.Request
}

// ObjectSize implements thanos.ObjectSizer.
func (r bodyReader) ObjectSize() (int64, error) {
	if r.r.ContentLength < 0 {
		return 0, fmt.Errorf("unknown size")
	}

	return r.r.ContentLength, nil
}

// Read implements io.Reader.
func (r bodyReader) Read(b []byte) (int, error) {
	return r.r.Body.Read(b)
}
