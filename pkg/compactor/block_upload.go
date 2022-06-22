// SPDX-License-Identifier: AGPL-3.0-only

package compactor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gorilla/mux"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/objstore"

	"github.com/grafana/dskit/runutil"
	"github.com/grafana/dskit/tenant"
	"github.com/grafana/regexp"

	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/storage/sharding"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/util"
	util_log "github.com/grafana/mimir/pkg/util/log"
)

// Name of file where we store a block's meta file while it's being uploaded.
const uploadingMetaFilename = "uploading-" + block.MetaFilename

var rePath = regexp.MustCompile(`^(index|chunks/\d{6})$`)

// HandleBlockUpload handles requests for starting or completing block uploads.
//
// The query parameter uploadComplete (true or false, default false) controls whether the
// upload should be completed or not.
//
// Starting the uploading of a block means to upload a meta file and verify that the upload can
// go ahead. In practice this means to check that the (complete) block isn't already in block
// storage, and that the meta file is valid.
func (c *MultitenantCompactor) HandleBlockUpload(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	blockID := vars["block"]
	bULID, err := ulid.Parse(blockID)
	if err != nil {
		http.Error(w, "invalid block ID", http.StatusBadRequest)
		return
	}
	ctx := r.Context()
	tenantID, err := tenant.TenantID(ctx)
	if err != nil {
		http.Error(w, "invalid tenant ID", http.StatusBadRequest)
		return
	}
	if !c.cfgProvider.CompactorBlockUploadEnabled(tenantID) {
		http.Error(w, "block upload is disabled", http.StatusBadRequest)
		return
	}

	logger := log.With(util_log.WithContext(ctx, c.logger), "block", blockID)

	shouldComplete := r.URL.Query().Get("uploadComplete") == "true"
	var op string
	if shouldComplete {
		op = "complete block upload"
	} else {
		op = "start block upload"
	}

	userBkt := bucket.NewUserBucketClient(tenantID, c.bucketClient, c.cfgProvider)
	if err := checkForCompleteBlock(ctx, bULID, userBkt); err != nil {
		writeBlockUploadError(err, op, "while checking for complete block", logger, w)
		return
	}

	if shouldComplete {
		err = c.completeBlockUpload(ctx, r, logger, userBkt, tenantID, bULID)
	} else {
		err = c.createBlockUpload(ctx, r, logger, userBkt, tenantID, bULID)
	}
	if err != nil {
		writeBlockUploadError(err, op, "", logger, w)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func writeBlockUploadError(err error, op, extra string, logger log.Logger, w http.ResponseWriter) {
	var httpErr httpError
	if errors.As(err, &httpErr) {
		level.Warn(logger).Log("msg", httpErr.message, "operation", op)
		http.Error(w, httpErr.message, httpErr.statusCode)
		return
	}

	if extra != "" {
		extra = " " + extra
	}
	level.Error(logger).Log("msg", fmt.Sprintf("an unexpected error occurred%s", extra), "operation", op,
		"err", err)
	http.Error(w, "internal server error", http.StatusInternalServerError)
}

// checkForCompleteBlock checks for a complete block with same ID. If one exists, an error is returned.
func checkForCompleteBlock(ctx context.Context, blockID ulid.ULID, userBkt objstore.Bucket) error {
	exists, err := userBkt.Exists(ctx, path.Join(blockID.String(), block.MetaFilename))
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("failed to check existence of %s in object storage", block.MetaFilename))
	}
	if exists {
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

	if msg := c.sanitizeMeta(logger, blockID, &meta); msg != "" {
		return httpError{
			message:    msg,
			statusCode: http.StatusBadRequest,
		}
	}

	// validate data is within the retention period
	retention := c.cfgProvider.CompactorBlocksRetentionPeriod(tenantID)
	if retention > 0 {
		threshold := time.Now().Add(-retention)
		if time.UnixMilli(meta.MaxTime).Before(threshold) {
			maxTimeStr := util.FormatTimeMillis(meta.MaxTime)
			return httpError{
				message:    fmt.Sprintf("block max time (%s) older than retention period", maxTimeStr),
				statusCode: http.StatusUnprocessableEntity,
			}
		}
	}

	return c.uploadMeta(ctx, logger, meta, blockID, uploadingMetaFilename, userBkt)
}

// UploadBlockFile handles requests for uploading block files.
//
// It takes the mandatory query parameter "path", specifying the file's destination path.
func (c *MultitenantCompactor) UploadBlockFile(w http.ResponseWriter, r *http.Request) {
	const op = "block file upload"

	vars := mux.Vars(r)
	blockID := vars["block"]
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

	ctx := r.Context()
	tenantID, err := tenant.TenantID(ctx)
	if err != nil {
		http.Error(w, "invalid tenant ID", http.StatusBadRequest)
		return
	}
	if !c.cfgProvider.CompactorBlockUploadEnabled(tenantID) {
		http.Error(w, "block upload is disabled", http.StatusBadRequest)
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

	if err := checkForCompleteBlock(ctx, bULID, userBkt); err != nil {
		writeBlockUploadError(err, op, "while checking for complete block", logger, w)
		return
	}

	metaPath := path.Join(blockID, uploadingMetaFilename)
	exists, err := userBkt.Exists(ctx, metaPath)
	if err != nil {
		level.Error(logger).Log("msg", "failed to check existence in object storage",
			"path", metaPath, "operation", op, "err", err)
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
			"operation", op, "destination", dst, "err", err)
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

	uploadingMetaPath := path.Join(blockID.String(), uploadingMetaFilename)
	rdr, err := userBkt.Get(ctx, uploadingMetaPath)
	if err != nil {
		if userBkt.IsObjNotFoundErr(err) {
			return httpError{
				message:    fmt.Sprintf("upload of block %s not started yet", blockID),
				statusCode: http.StatusNotFound,
			}
		}
		return errors.Wrap(err, fmt.Sprintf("failed to download %s from object storage", uploadingMetaFilename))
	}
	defer func() {
		_ = rdr.Close()
	}()

	meta, err := decodeMeta(rdr, uploadingMetaFilename)
	if err != nil {
		return err
	}

	level.Debug(logger).Log("msg", "completing block upload", "files", len(meta.Thanos.Files))

	if err := c.validateBlock(ctx, blockID, tenantID, userBkt, meta); err != nil {
		return err
	}

	// Upload meta file so block is considered complete
	if err := c.uploadMeta(ctx, logger, meta, blockID, block.MetaFilename, userBkt); err != nil {
		return err
	}

	if err := userBkt.Delete(ctx, uploadingMetaPath); err != nil {
		level.Warn(logger).Log("msg", fmt.Sprintf(
			"failed to delete %s from block in object storage", uploadingMetaFilename), "err", err)
		return nil
	}

	level.Debug(logger).Log("msg", "successfully completed block upload")
	return nil
}

// sanitizeMeta sanitizes and validates a metadata.Meta object. If a validation error occurs, an error
// message gets returned, otherwise an empty string.
func (c *MultitenantCompactor) sanitizeMeta(logger log.Logger, blockID ulid.ULID, meta *metadata.Meta) string {
	meta.ULID = blockID

	for l, v := range meta.Thanos.Labels {
		switch l {
		// Preserve this label
		case mimir_tsdb.CompactorShardIDExternalLabel:
			if v == "" {
				level.Debug(logger).Log("msg", "removing empty external label",
					"label", l)
				delete(meta.Thanos.Labels, l)
				continue
			}

			if _, _, err := sharding.ParseShardIDLabelValue(v); err != nil {
				return fmt.Sprintf("invalid %s external label: %q",
					mimir_tsdb.CompactorShardIDExternalLabel, v)
			}
		// Remove unused labels
		case mimir_tsdb.DeprecatedTenantIDExternalLabel, mimir_tsdb.DeprecatedIngesterIDExternalLabel, mimir_tsdb.DeprecatedShardIDExternalLabel:
			level.Debug(logger).Log("msg", "removing unused external label",
				"label", l, "value", v)
			delete(meta.Thanos.Labels, l)
		default:
			return fmt.Sprintf("unsupported external label: %s", l)
		}
	}

	meta.Compaction.Parents = nil
	meta.Compaction.Sources = []ulid.ULID{blockID}

	for _, f := range meta.Thanos.Files {
		if f.RelPath == block.MetaFilename {
			continue
		}

		if !rePath.MatchString(f.RelPath) {
			return fmt.Sprintf("file with invalid path: %s", f.RelPath)
		}

		if f.SizeBytes <= 0 {
			return fmt.Sprintf("file with invalid size: %s", f.RelPath)
		}
	}

	if meta.Version != metadata.TSDBVersion1 {
		return fmt.Sprintf("version must be %d", metadata.TSDBVersion1)
	}

	// validate minTime/maxTime
	// basic sanity check
	if meta.MinTime < 0 || meta.MaxTime < 0 || meta.MaxTime < meta.MinTime {
		return fmt.Sprintf("invalid minTime/maxTime: minTime=%d, maxTime=%d",
			meta.MinTime, meta.MaxTime)
	}
	// validate that times are in the past
	now := time.Now()
	if meta.MinTime > now.UnixMilli() || meta.MaxTime > now.UnixMilli() {
		return fmt.Sprintf("block time(s) greater than the present: minTime=%d, maxTime=%d",
			meta.MinTime, meta.MaxTime)
	}

	// Mark block source
	meta.Thanos.Source = "upload"

	return ""
}

func (c *MultitenantCompactor) uploadMeta(ctx context.Context, logger log.Logger, meta metadata.Meta,
	blockID ulid.ULID, name string, userBkt objstore.Bucket) error {
	dst := path.Join(blockID.String(), name)
	level.Debug(logger).Log("msg", fmt.Sprintf("uploading %s to bucket", name), "dst", dst)
	buf := bytes.NewBuffer(nil)
	if err := json.NewEncoder(buf).Encode(meta); err != nil {
		return errors.Wrap(err, "failed to encode block metadata")
	}
	if err := userBkt.Upload(ctx, dst, buf); err != nil {
		return errors.Wrapf(err, "failed uploading %s to bucket", name)
	}

	return nil
}

func (c *MultitenantCompactor) validateBlock(ctx context.Context, blockID ulid.ULID, tenantID string,
	userBkt objstore.Bucket, meta metadata.Meta) error {
	blockDir, err := os.MkdirTemp("", "")
	if err != nil {
		return errors.Wrap(err, "failed to create temp dir")
	}
	defer func() {
		if err := os.RemoveAll(blockDir); err != nil {
			level.Warn(c.logger).Log("msg", "failed to remove temp dir", "path", blockDir, "err", err)
		}
	}()

	if err := userBkt.Iter(ctx, blockID.String(), func(pth string) error {
		if strings.HasSuffix(pth, ".lock") || pth == "meta.json" {
			return nil
		}

		r, err := userBkt.Get(ctx, pth)
		if err != nil {
			// TODO: Return error indicating bad gateway
			return errBadGateway{message: fmt.Sprintf("failed to get object %q from object storage", pth), err: err}
		}

		f, err := os.Create(filepath.Join(blockDir, pth))
		if err != nil {
			return errors.Wrap(err, "failed creating temp file")
		}
		defer func() {
			_ = f.Close()
		}()
		if _, err := io.Copy(f, r); err != nil {
			return errors.Wrap(err, "failed writing to temp file")
		}
		if err := f.Close(); err != nil {
			return errors.Wrap(err, "failed writing to temp file")
		}

		return nil
	}); err != nil {
		return errors.Wrapf(err, "failed to iterate block %s", blockID.String())
	}

	idxFi, err := analyzeBlockFile(blockDir, "index")
	if err != nil {
		return err
	}
	meta.Thanos.Files = []metadata.File{
		idxFi,
		{
			// Size not stated for meta.json
			RelPath: "meta.json",
		},
	}

	meta.Thanos.SegmentFiles = []string{}
	chunksDir := filepath.Join(blockDir, "chunks")
	ces, err := os.ReadDir(chunksDir)
	if err != nil {
		return errors.Wrapf(err, "failed to read directory %s", chunksDir)
	}
	for _, e := range ces {
		meta.Thanos.SegmentFiles = append(meta.Thanos.SegmentFiles, e.Name())
		fi, err := analyzeBlockFile(blockDir, filepath.Join("chunks", e.Name()))
		if err != nil {
			return err
		}

		meta.Thanos.Files = append(meta.Thanos.Files, fi)
	}

	if err := c.scanBlock(blockDir, meta); err != nil {
		return err
	}

	return nil
}

func analyzeBlockFile(relPath, blockDir string) (metadata.File, error) {
	fi, err := os.Stat(filepath.Join(blockDir, relPath))
	if err != nil {
		return metadata.File{}, errors.Wrapf(err, "failed to stat %s", filepath.Join(blockDir, relPath))
	}
	return metadata.File{
		RelPath:   relPath,
		SizeBytes: fi.Size(),
	}, nil
}

func (c *MultitenantCompactor) scanBlock(blockDir string, meta metadata.Meta) error {
	// TODO: Count samples (check with Peter)

	var cr *chunks.Reader
	cr, err := chunks.NewDirReader(filepath.Join(blockDir, block.ChunksDirname), nil)
	if err != nil {
		return errors.Wrap(err, "open chunks dir")
	}
	defer runutil.CloseWithErrCapture(&err, cr, "closing chunks reader")

	r, err := index.NewFileReader(filepath.Join(blockDir, block.IndexFilename))
	if err != nil {
		return errors.Wrap(err, "open index file")
	}
	defer runutil.CloseWithErrCapture(&err, r, "gather index issue file reader")

	ps, err := r.Postings(index.AllPostingsKey())
	if err != nil {
		return errors.Wrap(err, "get all postings")
	}

	shard, shardCount, err := sharding.ParseShardIDLabelValue(meta.Thanos.Labels[mimir_tsdb.CompactorShardIDExternalLabel])
	if err != nil {
		return errors.Wrap(err, "parse shard ID label value")
	}

	var (
		lastLset labels.Labels
		lset     labels.Labels

		outOfOrderLabels int
	)
	// Per series.
	for ps.Next() {
		lastLset = append(lastLset[:0], lset...)

		id := ps.At()

		var chnks []chunks.Meta
		if err := r.Series(id, &lset, &chnks); err != nil {
			return errors.Wrap(err, "read series")
		}
		if len(lset) == 0 {
			return errors.Errorf("empty label set detected for series %d", id)
		}
		if lastLset != nil && labels.Compare(lastLset, lset) >= 0 {
			return errors.Errorf("series %v out of order; previous: %v", lset, lastLset)
		}
		l0 := lset[0]
		for _, l := range lset[1:] {
			if l.Name < l0.Name {
				outOfOrderLabels++
				level.Warn(c.logger).Log("msg",
					"out-of-order label set: known bug in Prometheus 2.8.0 and below",
					"labelset", lset.String(),
					"series", fmt.Sprintf("%d", id),
				)
			}
			l0 = l
		}
		if len(chnks) == 0 {
			return errors.Errorf("empty chunks for series %d", id)
		}

		if lset.Hash()%shardCount != shard {
			return errBadRequest{message: fmt.Sprintf("series sharded incorrectly: %s", lset.String())}
		}

		ooo := 0
		// Per chunk in series.
		for i, chnk := range chnks {
			if i == 0 {
				continue
			}

			prev := chnks[i-1]

			// Chunk order within block.
			if chnk.MinTime > prev.MaxTime {
				continue
			}

			if chnk.MinTime == prev.MinTime && chnk.MaxTime == prev.MaxTime {
				// TODO(bplotka): Calc and check checksum from chunks itself.
				// The chunks can overlap 1:1 in time, but does not have same data.
				// We assume same data for simplicity, but it can be a symptom of error.
				continue
			}

			// Chunks partly overlaps or out of order.
			level.Debug(c.logger).Log("msg", "found out of order chunks",
				"prev_ref", prev.Ref, "next_ref", chnk.Ref,
				"prev_min_time", timestamp.Time(prev.MinTime).UTC().Format(time.RFC3339Nano),
				"prev_max_time", timestamp.Time(prev.MaxTime).UTC().Format(time.RFC3339Nano),
				"next_min_time", timestamp.Time(chnk.MinTime).UTC().Format(time.RFC3339Nano),
				"next_max_time", timestamp.Time(chnk.MaxTime).UTC().Format(time.RFC3339Nano),
				"labels", lset, "prev_chunk_index", i-1, "next_chunk_index", i, "chunks_for_series", len(chnks))

			ooo++
		}

		/*
			if ooo > 0 {
				// TODO: Should this be an error?
					stats.OutOfOrderSeries++
					stats.OutOfOrderChunks += ooo
			}
		*/

		if err := c.verifyChunks(cr, lset, chnks); err != nil {
			return err
		}
	}
	if ps.Err() != nil {
		return errors.Wrap(err, "failed iterating over postings")
	}

	if outOfOrderLabels > 0 {
		return errBadRequest{message: fmt.Sprintf("block has %d out of order labels", outOfOrderLabels)}
	}

	return nil
}

type errBadRequest struct {
	message string
}

func (e errBadRequest) Error() string {
	return e.message
}

type errBadGateway struct {
	message string
	err     error
}

func (e errBadGateway) Error() string {
	return e.message
}

func (e errBadGateway) Unwrap() error {
	return e.err
}

func (c *MultitenantCompactor) verifyChunks(cr *chunks.Reader, lset labels.Labels, chnks []chunks.Meta) error {
	for _, cm := range chnks {
		ch, err := cr.Chunk(cm.Ref)
		if err != nil {
			return errors.Wrapf(err, "failed to read chunk %d", cm.Ref)
		}

		samples := 0
		firstSample := true
		prevTS := int64(-1)

		it := ch.Iterator(nil)
		for it.Err() == nil && it.Next() {
			samples++
			ts, _ := it.At()

			if firstSample {
				firstSample = false
				if ts != cm.MinTime {
					// TODO: Error?
					level.Warn(c.logger).Log("ref", cm.Ref, "msg", "timestamp of the first sample doesn't match chunk MinTime",
						"sampleTimestamp", formatTimestamp(ts), "chunkMinTime", formatTimestamp(cm.MinTime))
				}
			} else if ts <= prevTS {
				// TODO: Error?
				level.Warn(c.logger).Log("ref", cm.Ref, "msg", "found sample with timestamp not strictly higher than previous timestamp",
					"previous", formatTimestamp(prevTS), "sampleTimestamp", formatTimestamp(ts))
			}

			prevTS = ts
		}
		if e := it.Err(); e != nil {
			return errors.Wrapf(err, "failed to failed to iterate over samples of chunk %d", cm.Ref)
		}
		if samples == 0 {
			// TODO: Error?
			level.Warn(c.logger).Log("ref", cm.Ref, "msg", "no samples found in the chunk")
		} else if prevTS != cm.MaxTime {
			// TODO: Error?
			level.Warn(c.logger).Log("ref", cm.Ref, "msg", "timestamp of the last sample doesn't match chunk MaxTime",
				"sampleTimestamp", formatTimestamp(prevTS), "chunkMaxTime", formatTimestamp(cm.MaxTime))
		}
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

func formatTimestamp(ts int64) string {
	return fmt.Sprintf("%d (%s)", ts, timestamp.Time(ts).UTC().Format(time.RFC3339Nano))
}
