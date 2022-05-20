// SPDX-License-Identifier: AGPL-3.0-only

package compactor

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
	"strings"
	"time"

	"github.com/go-kit/log/level"
	"github.com/gofrs/uuid"
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
)

// CreateBlockUpload handles requests for creating block upload sessions.
func (c *MultitenantCompactor) CreateBlockUpload(w http.ResponseWriter, r *http.Request) {
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

	level.Debug(c.logger).Log("msg", "creating block upload session", "user", tenantID, "block_id", blockID)

	bkt := bucket.NewUserBucketClient(string(tenantID), c.bucketClient, c.cfgProvider)
	exists := false
	if err := bkt.Iter(ctx, blockID, func(pth string) error {
		exists = true
		return nil
	}); err != nil {
		level.Error(c.logger).Log("msg", "failed to iterate over block files", "user", tenantID,
			"block_id", blockID, "err", err)
		http.Error(w, "failed iterating over block files in object storage", http.StatusBadGateway)
		return
	}
	if exists {
		level.Debug(c.logger).Log("msg", "block already exists in object storage", "user", tenantID,
			"block_id", blockID)
		http.Error(w, "block already exists in object storage", http.StatusConflict)
		return
	}

	rnd, err := uuid.NewV4()
	if err != nil {
		level.Error(c.logger).Log("msg", "failed to generate UUID", "err", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	lockName := fmt.Sprintf("%s.lock", rnd)
	if err := bkt.Upload(ctx, lockName, bytes.NewBuffer(nil)); err != nil {
		level.Error(c.logger).Log("msg", "failed to upload lock file to block dir", "user", tenantID,
			"block_id", blockID, "err", err)
		http.Error(w, "failed uploading lock file to block dir", http.StatusBadGateway)
		return
	}
	if err := bkt.Iter(ctx, blockID, func(pth string) error {
		exists = pth != lockName
		return nil
	}); err != nil {
		level.Error(c.logger).Log("msg", "failed to iterate over block files", "user", tenantID,
			"block_id", blockID, "err", err)
		http.Error(w, "failed iterating over block files in object storage", http.StatusBadGateway)
		return
	}
	if exists {
		level.Debug(c.logger).Log("msg", "another file exists for block in object storage", "user", tenantID,
			"block_id", blockID)
		http.Error(w, "another file exists for block in object storage", http.StatusConflict)
		return
	}

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

	if r.Body == nil || r.ContentLength == 0 {
		http.Error(w, "file cannot be empty", http.StatusBadRequest)
		return
	}

	bkt := bucket.NewUserBucketClient(string(tenantID), c.bucketClient, c.cfgProvider)

	exists := false
	if err := bkt.Iter(ctx, blockID, func(pth string) error {
		exists = strings.HasSuffix(pth, ".lock")
		return nil
	}); err != nil {
		level.Error(c.logger).Log("msg", "failed to iterate over block files", "user", tenantID,
			"block_id", blockID, "err", err)
		http.Error(w, "failed iterating over block files in object storage", http.StatusBadGateway)
		return
	}
	if !exists {
		level.Debug(c.logger).Log("msg", "no lock file exists for block in object storage, refusing file upload",
			"user", tenantID, "block_id", blockID)
		http.Error(w, "block upload has not yet been initiated", http.StatusBadRequest)
		return
	}

	dst := path.Join(blockID, pth)

	level.Debug(c.logger).Log("msg", "uploading block file to bucket", "user", tenantID,
		"destination", dst, "size", r.ContentLength)
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

	bkt := bucket.NewUserBucketClient(tenantID, c.bucketClient, c.cfgProvider)

	exists := false
	if err := bkt.Iter(ctx, blockID, func(pth string) error {
		exists = strings.HasSuffix(pth, ".lock")
		return nil
	}); err != nil {
		level.Error(c.logger).Log("msg", "failed to iterate over block files", "user", tenantID,
			"block_id", blockID, "err", err)
		http.Error(w, "failed iterating over block files in object storage", http.StatusBadGateway)
		return
	}
	if !exists {
		level.Debug(c.logger).Log("msg", "no lock file exists for block in object storage, refusing to complete block",
			"user", tenantID, "block_id", blockID)
		http.Error(w, "block upload has not yet been initiated", http.StatusBadRequest)
		return
	}

	level.Debug(c.logger).Log("msg", "completing block upload", "user",
		tenantID, "block_id", blockID, "files", len(meta.Thanos.Files))

	if err := c.sanitizeMeta(blockID, tenantID, &meta); err != nil {
		level.Error(c.logger).Log("msg", "failed to upload meta.json", "user", tenantID,
			"block_id", blockID, "err", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	if err := c.validateBlock(ctx, w, blockID, tenantID, bkt, meta); err != nil {
		level.Error(c.logger).Log("msg", "failed validating block", "user", tenantID,
			"block_id", blockID, "err", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	// Upload meta.json so block is considered complete
	if err := c.uploadMeta(ctx, w, meta, blockID, tenantID, bkt); err != nil {
		var eBadGW errBadGateway
		if errors.As(err, &eBadGW) {
			level.Error(c.logger).Log("msg", eBadGW.message, "user", tenantID,
				"block_id", blockID, "err", err)
			http.Error(w, eBadGW.message, http.StatusBadGateway)
			return
		}
		var eBadReq errBadRequest
		if errors.As(err, &eBadReq) {
			level.Warn(c.logger).Log("msg", eBadReq.message, "user", tenantID,
				"block_id", blockID)
			http.Error(w, eBadReq.message, http.StatusBadRequest)
			return
		}

		level.Error(c.logger).Log("msg", "failed to upload meta.json", "user", tenantID,
			"block_id", blockID, "err", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	var lockFiles []string
	if err := bkt.Iter(ctx, blockID, func(pth string) error {
		if strings.HasSuffix(pth, ".lock") {
			lockFiles = append(lockFiles, pth)
		}
		return nil
	}); err != nil {
		level.Error(c.logger).Log("msg", "failed to iterate over block files", "user", tenantID,
			"block_id", blockID, "err", err)
		http.Error(w, "failed iterating over block files in object storage", http.StatusBadGateway)
		return
	}
	failed := false
	for _, lf := range lockFiles {
		if err := bkt.Delete(ctx, lf); err != nil {
			level.Error(c.logger).Log("msg", "failed to delete lock file from block in object storage", "user", tenantID,
				"block_id", blockID, "err", err)
			failed = true
		}
	}
	if failed {
		http.Error(w, "failed deleting lock file(s) from object storage", http.StatusBadGateway)
		return
	}

	level.Debug(c.logger).Log("msg", "successfully completed block upload")

	w.WriteHeader(http.StatusOK)
}

func (c *MultitenantCompactor) validateBlock(ctx context.Context, w http.ResponseWriter, blockID, tenantID string,
	bkt objstore.Bucket, meta metadata.Meta) error {
	blockDir, err := os.MkdirTemp("", "")
	if err != nil {
		return errors.Wrap(err, "failed to create temp dir")
	}
	defer func() {
		if err := os.RemoveAll(blockDir); err != nil {
			level.Warn(c.logger).Log("msg", "failed to remove temp dir", "path", blockDir, "err", err)
		}
	}()

	if err := bkt.Iter(ctx, blockID, func(pth string) error {
		if strings.HasSuffix(pth, ".lock") || pth == "meta.json" {
			return nil
		}

		r, err := bkt.Get(ctx, pth)
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
		return errors.Wrapf(err, "failed to iterate block %s", blockID)
	}

	// Write meta.json
	f, err := os.Create(filepath.Join(blockDir, "meta.json"))
	if err != nil {
		return errors.Wrap(err, "failed to create temporary meta.json")
	}
	defer func() {
		_ = f.Close()
	}()

	/*
		metaJSON, err := json.Marshal(meta)
		if err != nil {
			return errors.Wrap(err, "failed JSON encoding block metadata")
		}
		if _, err := f.Write(metaJSON); err != nil {
			return errors.Wrap(err, "failed writing to temporary meta.json")
		}
		if err := f.Close(); err != nil {
			return errors.Wrap(err, "failed writing to temporary meta.json")
		}
	*/

	if err := c.verifyBlock(blockDir, meta); err != nil {
		return err
	}

	return nil
}

func (c *MultitenantCompactor) verifyBlock(blockDir string, meta metadata.Meta) error {
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

		if ooo > 0 {
			// TODO: Should this be an error?
			/*
				stats.OutOfOrderSeries++
				stats.OutOfOrderChunks += ooo
			*/
		}

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

func (c *MultitenantCompactor) sanitizeMeta(tenantID, blockID string, meta *metadata.Meta) error {
	if meta.Thanos.Labels == nil {
		meta.Thanos.Labels = map[string]string{}
	}
	updated := false

	metaULID := meta.ULID
	if metaULID.String() != blockID {
		level.Warn(c.logger).Log("msg", "updating meta.json block ID", "old_value", metaULID.String(),
			"new_value", blockID)
		var err error
		meta.ULID, err = ulid.Parse(blockID)
		if err != nil {
			return errors.Wrapf(err, "couldn't parse block ID %q", blockID)
		}
		updated = true
	}

	metaTenantID := meta.Thanos.Labels[mimir_tsdb.TenantIDExternalLabel]
	if metaTenantID != tenantID {
		level.Warn(c.logger).Log("msg", "updating meta.json tenant label", "block_id", blockID,
			"old_value", metaTenantID, "new_value", tenantID)
		updated = true
		meta.Thanos.Labels[mimir_tsdb.TenantIDExternalLabel] = tenantID
	}

	for l, v := range meta.Thanos.Labels {
		switch l {
		case mimir_tsdb.TenantIDExternalLabel, mimir_tsdb.IngesterIDExternalLabel, mimir_tsdb.CompactorShardIDExternalLabel:
		default:
			level.Warn(c.logger).Log("msg", "removing unknown meta.json label", "block_id", blockID, "label", l, "value", v)
			updated = true
			delete(meta.Thanos.Labels, l)
		}
	}

	if !updated {
		level.Info(c.logger).Log("msg", "no changes to meta.json required", "block_id", blockID)
	}

	// TODO: List files in bucket and update file list in meta.json
	return nil
}

func (c *MultitenantCompactor) uploadMeta(ctx context.Context, w http.ResponseWriter, meta metadata.Meta, blockID, tenantID string,
	bkt objstore.Bucket) error {
	dst := path.Join(blockID, "meta.json")
	level.Debug(c.logger).Log("msg", "uploading meta.json to bucket", "dst", dst)
	buf := bytes.NewBuffer(nil)
	enc := json.NewEncoder(buf)
	if err := enc.Encode(meta); err != nil {
		return errors.Wrap(err, "failed to encode meta.json")
	}
	if err := bkt.Upload(ctx, dst, buf); err != nil {
		return errBadGateway{message: "failed uploading meta.json to bucket", err: err}
	}

	return nil
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

func formatTimestamp(ts int64) string {
	return fmt.Sprintf("%d (%s)", ts, timestamp.Time(ts).UTC().Format(time.RFC3339Nano))
}
