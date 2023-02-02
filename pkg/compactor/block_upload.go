// SPDX-License-Identifier: AGPL-3.0-only

package compactor

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gorilla/mux"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/thanos-io/objstore"

	"github.com/grafana/dskit/tenant"
	"github.com/grafana/regexp"

	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/storage/sharding"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/storage/tsdb/metadata"
	"github.com/grafana/mimir/pkg/util"
	util_log "github.com/grafana/mimir/pkg/util/log"
)

// Name of file where we store a block's meta file while it's being uploaded.
const (
	uploadingMetaFilename = "uploading-meta.json"
	validationFilename    = "validation.json"
)

type BlockUploadConfig struct {
	ValidationType              int           `yaml:"-"`
	ValidationHeartbeatInterval time.Duration `yaml:"validation_heartbeat_interval" category:"experimental"`
	ValidationHeartbeatTimeout  time.Duration `yaml:"validation_heartbeat_timeout" category:"experimental"`
}

func (cfg *BlockUploadConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet, logger log.Logger) {
	f.DurationVar(&cfg.ValidationHeartbeatInterval, prefix+".validation-heartbeat-interval", 1*time.Minute, "Duration of time between heartbeats of an in-progress block upload validation. <= 0 to disable heartbeating.")
	f.DurationVar(&cfg.ValidationHeartbeatTimeout, prefix+".validation-heartbeat-timeout", 5*time.Minute, "Maximum duration of time to wait for a block upload validation to complete. <= 0 to disable.")
}

const (
	AsynchronousValidation = iota
	SynchronousValidation  // for unit testing
)

var rePath = regexp.MustCompile(`^(index|chunks/\d{6})$`)

// StartBlockUpload handles request for starting block upload.
//
// Starting the uploading of a block means to upload a meta file and verify that the upload can
// go ahead. In practice this means to check that the (complete) block isn't already in block
// storage, and that the meta file is valid.
func (c *MultitenantCompactor) StartBlockUpload(w http.ResponseWriter, r *http.Request) {
	blockID, tenantID, err := c.parseBlockUploadParameters(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	blockLogger := log.With(util_log.WithContext(ctx, c.logger), "block", blockID)

	const op = "start block upload"

	userBkt := bucket.NewUserBucketClient(tenantID, c.bucketClient, c.cfgProvider)
	if _, _, err := c.checkBlockState(ctx, userBkt, blockID, false); err != nil {
		writeBlockUploadError(err, op, "while checking for complete block", blockLogger, w)
		return
	}

	if err := c.createBlockUpload(ctx, r, blockLogger, userBkt, tenantID, blockID); err != nil {
		writeBlockUploadError(err, op, "", blockLogger, w)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// FinishBlockUpload handles request for finishing block upload.
//
// Finishing block upload performs block validation, and if all checks pass, marks block as finished
// by uploading meta.json file.
func (c *MultitenantCompactor) FinishBlockUpload(w http.ResponseWriter, r *http.Request) {
	blockID, tenantID, err := c.parseBlockUploadParameters(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	blockLogger := log.With(util_log.WithContext(ctx, c.logger), "block", blockID)

	const op = "complete block upload"

	userBkt := bucket.NewUserBucketClient(tenantID, c.bucketClient, c.cfgProvider)
	m, _, err := c.checkBlockState(ctx, userBkt, blockID, true)
	if err != nil {
		writeBlockUploadError(err, op, "while checking for complete block", blockLogger, w)
		return
	}

	// This should not happen, as checkBlockState with requireUploadInProgress=true returns nil error
	// only if uploading-meta.json file exists.
	if m == nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	// create validation file to signal that block validation has started
	if err := c.uploadValidation(ctx, blockLogger, blockID, userBkt); err != nil {
		writeBlockUploadError(err, op, "while creating validation file", blockLogger, w)
		return
	}

	switch c.compactorCfg.BlockUpload.ValidationType {
	case AsynchronousValidation:
		go c.validateAndCompleteBlockUpload(blockLogger, userBkt, blockID, *m)
	case SynchronousValidation:
		c.validateAndCompleteBlockUpload(blockLogger, userBkt, blockID, *m)
	default:
		http.Error(w, "invalid block validation configured", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// parseBlockUploadParameters parses common parameters from the request: block ID, tenant and checks if tenant has uploads enabled.
func (c *MultitenantCompactor) parseBlockUploadParameters(r *http.Request) (ulid.ULID, string, error) {
	blockID, err := ulid.Parse(mux.Vars(r)["block"])
	if err != nil {
		return ulid.ULID{}, "", errors.New("invalid block ID")
	}

	ctx := r.Context()
	tenantID, err := tenant.TenantID(ctx)
	if err != nil {
		return ulid.ULID{}, "", errors.New("invalid tenant ID")
	}

	if !c.cfgProvider.CompactorBlockUploadEnabled(tenantID) {
		return ulid.ULID{}, "", errors.New("block upload is disabled")
	}

	return blockID, tenantID, nil
}

func writeBlockUploadError(err error, op, extra string, blockLogger log.Logger, w http.ResponseWriter) {
	var httpErr httpError
	if errors.As(err, &httpErr) {
		level.Warn(blockLogger).Log("msg", httpErr.message, "operation", op)
		http.Error(w, httpErr.message, httpErr.statusCode)
		return
	}

	if extra != "" {
		extra = " " + extra
	}
	level.Error(blockLogger).Log("msg", fmt.Sprintf("an unexpected error occurred%s", extra), "operation", op, "err", err)
	http.Error(w, "internal server error", http.StatusInternalServerError)
}

func (c *MultitenantCompactor) createBlockUpload(ctx context.Context, r *http.Request,
	blockLogger log.Logger, userBkt objstore.Bucket, tenantID string, blockID ulid.ULID) error {
	level.Debug(blockLogger).Log("msg", "starting block upload")

	var meta metadata.Meta
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(&meta); err != nil {
		return httpError{
			message:    "malformed request body",
			statusCode: http.StatusBadRequest,
		}
	}

	if msg := c.sanitizeMeta(blockLogger, blockID, &meta); msg != "" {
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

	return c.uploadMeta(ctx, blockLogger, meta, blockID, uploadingMetaFilename, userBkt)
}

// UploadBlockFile handles requests for uploading block files.

// It takes the mandatory query parameter "path", specifying the file's destination path.
func (c *MultitenantCompactor) UploadBlockFile(w http.ResponseWriter, r *http.Request) {
	blockID, tenantID, err := c.parseBlockUploadParameters(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	pth := r.URL.Query().Get("path")
	if pth == "" {
		http.Error(w, "missing or invalid file path", http.StatusBadRequest)
		return
	}

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

	const op = "block file upload"

	ctx := r.Context()
	blockLogger := log.With(util_log.WithContext(ctx, c.logger), "block", blockID)

	userBkt := bucket.NewUserBucketClient(tenantID, c.bucketClient, c.cfgProvider)

	m, _, err := c.checkBlockState(ctx, userBkt, blockID, true)
	if err != nil {
		writeBlockUploadError(err, op, "while checking for complete block", blockLogger, w)
		return
	}

	// This should not happen.
	if m == nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	// Check if file was specified in meta.json, and if it has expected size.
	found := false
	for _, f := range m.Thanos.Files {
		if pth == f.RelPath {
			found = true

			if r.ContentLength != f.SizeBytes {
				http.Error(w, fmt.Sprintf("file size doesn't match %s", block.MetaFilename), http.StatusBadRequest)
				return
			}
		}
	}
	if !found {
		http.Error(w, "unexpected file", http.StatusBadRequest)
		return
	}

	dst := path.Join(blockID.String(), pth)

	level.Debug(blockLogger).Log("msg", "uploading block file to bucket", "destination", dst, "size", r.ContentLength)
	reader := bodyReader{r: r}
	if err := userBkt.Upload(ctx, dst, reader); err != nil {
		level.Error(blockLogger).Log("msg", "failed uploading block file to bucket", "operation", op, "destination", dst, "err", err)
		// We don't know what caused the error; it could be the client's fault (e.g. killed
		// connection), but internal server error is the safe choice here.
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	level.Debug(blockLogger).Log("msg", "finished uploading block file to bucket", "path", pth)

	w.WriteHeader(http.StatusOK)
}

func (c *MultitenantCompactor) validateAndCompleteBlockUpload(blockLogger log.Logger, userBkt objstore.Bucket, blockID ulid.ULID, meta metadata.Meta) {
	level.Debug(blockLogger).Log("msg", "completing block upload", "files", len(meta.Thanos.Files))

	ch := make(chan struct{})
	var wg sync.WaitGroup

	var ctx context.Context
	var cancel context.CancelFunc

	if c.compactorCfg.BlockUpload.ValidationHeartbeatTimeout > 0 {
		ctx, cancel = context.WithTimeout(context.Background(), c.compactorCfg.BlockUpload.ValidationHeartbeatTimeout)
	} else {
		ctx, cancel = context.WithCancel(context.Background())
	}
	defer cancel()

	if c.compactorCfg.BlockUpload.ValidationHeartbeatInterval > 0 {
		// start go routine that updates validation file timestamp once per minute
		wg.Add(1)
		go c.periodicValidationUpdater(ctx, blockLogger, blockID, userBkt, ch, &wg, cancel)
	}

	if err := c.validateBlock(ctx, blockID, userBkt, meta); err != nil {
		level.Error(blockLogger).Log("msg", "error while validating block", "err", err)
		close(ch)
		wg.Wait()
		if !errors.Is(err, context.Canceled) {
			err := c.uploadValidationWithError(ctx, blockID, userBkt, err.Error())
			if err != nil {
				level.Error(blockLogger).Log("msg", "error updating validation file after failed block validation", "err", err)
			}
		}
		return
	}

	close(ch)
	// use waitgroup to ensure validation ts update is complete
	wg.Wait()

	// Upload meta file so block is considered complete
	if err := c.uploadMeta(ctx, blockLogger, meta, blockID, block.MetaFilename, userBkt); err != nil {
		level.Error(blockLogger).Log("msg", "error uploading block metadata file", "err", err)
		if !errors.Is(err, context.Canceled) {
			err := c.uploadValidationWithError(ctx, blockID, userBkt, err.Error())
			if err != nil {
				level.Error(blockLogger).Log("msg", "error updating validation file after upload of metadata file failed", "err", err)
			}
		}
		return
	}

	if err := userBkt.Delete(ctx, path.Join(blockID.String(), uploadingMetaFilename)); err != nil {
		level.Warn(blockLogger).Log("msg", fmt.Sprintf(
			"failed to delete %s from block in object storage", uploadingMetaFilename), "err", err)
		return
	}

	if err := userBkt.Delete(ctx, path.Join(blockID.String(), validationFilename)); err != nil {
		level.Warn(blockLogger).Log("msg", fmt.Sprintf(
			"failed to delete %s from block in object storage", validationFilename), "err", err)
		return
	}

	level.Debug(blockLogger).Log("msg", "successfully completed block upload")
}

// sanitizeMeta sanitizes and validates a metadata.Meta object. If a validation error occurs, an error
// message gets returned, otherwise an empty string.
func (c *MultitenantCompactor) sanitizeMeta(blockLogger log.Logger, blockID ulid.ULID, meta *metadata.Meta) string {
	meta.ULID = blockID
	for l, v := range meta.Thanos.Labels {
		switch l {
		// Preserve this label
		case mimir_tsdb.CompactorShardIDExternalLabel:
			if v == "" {
				level.Debug(blockLogger).Log("msg", "removing empty external label",
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
			level.Debug(blockLogger).Log("msg", "removing unused external label",
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

func (c *MultitenantCompactor) uploadMeta(ctx context.Context, blockLogger log.Logger, meta metadata.Meta,
	blockID ulid.ULID, name string, userBkt objstore.Bucket) error {
	dst := path.Join(blockID.String(), name)
	level.Debug(blockLogger).Log("msg", fmt.Sprintf("uploading %s to bucket", name), "dst", dst)
	buf := bytes.NewBuffer(nil)
	if err := json.NewEncoder(buf).Encode(meta); err != nil {
		return errors.Wrap(err, "failed to encode block metadata")
	}
	if err := userBkt.Upload(ctx, dst, buf); err != nil {
		return errors.Wrapf(err, "failed uploading %s to bucket", name)
	}

	return nil
}

func (c *MultitenantCompactor) createTemporaryBlockDirectory() (dir string, err error) {
	blockDir, err := os.MkdirTemp(c.compactorCfg.DataDir, "upload")
	if err != nil {
		level.Error(c.logger).Log("msg", "failed to create temporary block directory", "err", err)
		return "", errors.New("failed to create temporary block directory")
	}

	level.Debug(c.logger).Log("msg", "created temporary block directory", "dir", blockDir)
	return blockDir, nil
}

func (c *MultitenantCompactor) removeTemporaryBlockDirectory(blockDir string) {
	level.Debug(c.logger).Log("msg", "removing temporary block directory", "dir", blockDir)
	if err := os.RemoveAll(blockDir); err != nil {
		level.Warn(c.logger).Log("msg", "failed to remove temporary block directory", "path", blockDir, "err", err)
	}
}

func (c *MultitenantCompactor) prepareBlockForValidation(ctx context.Context, userBkt objstore.Bucket, blockID ulid.ULID) (string, error) {
	blockDir, err := c.createTemporaryBlockDirectory()
	if err != nil {
		return "", err
	}

	// download the block to local storage
	level.Debug(c.logger).Log("msg", "downloading block from bucket", "block", blockID.String())
	err = objstore.DownloadDir(ctx, c.logger, userBkt, blockID.String(), blockID.String(), blockDir)
	if err != nil {
		c.removeTemporaryBlockDirectory(blockDir)
		return "", errors.Wrap(err, "failed to download block")
	}

	// rename the temporary meta file name to the expected one locally so that the block can be inspected
	err = os.Rename(filepath.Join(blockDir, uploadingMetaFilename), filepath.Join(blockDir, block.MetaFilename))
	if err != nil {
		level.Warn(c.logger).Log("msg", "could not rename temporary metadata file", "block", blockID.String(), "err", err)
		c.removeTemporaryBlockDirectory(blockDir)
		return "", errors.New("failed renaming while preparing block for validation")
	}

	return blockDir, nil
}

func (c *MultitenantCompactor) validateBlock(ctx context.Context, blockID ulid.ULID,
	userBkt objstore.Bucket, meta metadata.Meta) error {
	blockDir, err := c.prepareBlockForValidation(ctx, userBkt, blockID)
	if err != nil {
		return err
	}
	defer c.removeTemporaryBlockDirectory(blockDir)

	blockMetadata, err := metadata.ReadFromDir(blockDir)
	if err != nil {
		return errors.Wrap(err, "error reading block metadata file")
	}

	// check that all files listed in the metadata are present and the correct size
	for _, f := range blockMetadata.Thanos.Files {
		fi, err := os.Stat(filepath.Join(blockDir, filepath.FromSlash(f.RelPath)))
		if err != nil {
			return errors.Wrapf(err, "failed to stat %s", f.RelPath)
		}

		if !fi.Mode().IsRegular() {
			return errors.Errorf("not a file: %s", f.RelPath)
		}

		if f.RelPath != block.MetaFilename && fi.Size() != f.SizeBytes {
			return errors.Errorf("file size mismatch for %s", f.RelPath)
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

type validationFile struct {
	LastUpdate int64  // UnixMillis of last update time.
	Error      string // Error message if validation failed.
}

type BlockUploadStateResult struct {
	State string `json:"result"`
	Error string `json:"error,omitempty"`
}

type blockUploadState int

const (
	blockStateUnknown         blockUploadState = iota // unknown, default value
	blockIsComplete                                   // meta.json file exists
	blockUploadNotStarted                             // meta.json doesn't exist, uploading-meta.json doesn't exist
	blockUploadInProgress                             // meta.json doesn't exist, but uploading-meta.json does
	blockValidationInProgress                         // meta.json doesn't exist, uploading-meta.json exists, validation.json exists and is recent
	blockValidationFailed
	blockValidationStale
)

func (c *MultitenantCompactor) GetBlockUploadStateHandler(w http.ResponseWriter, r *http.Request) {
	blockID, tenantID, err := c.parseBlockUploadParameters(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	userBkt := bucket.NewUserBucketClient(tenantID, c.bucketClient, c.cfgProvider)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	s, _, v, err := c.getBlockUploadState(r.Context(), userBkt, blockID)
	if err != nil {
		writeBlockUploadError(err, "get block state", "", log.With(util_log.WithContext(r.Context(), c.logger), "block", blockID), w)
		return
	}

	res := BlockUploadStateResult{}

	switch s {
	case blockIsComplete:
		res.State = "complete"
	case blockUploadNotStarted:
		http.Error(w, "block doesn't exist", http.StatusNotFound)
		return
	case blockValidationStale:
		fallthrough
	case blockUploadInProgress:
		res.State = "uploading"
	case blockValidationInProgress:
		res.State = "validating"
	case blockValidationFailed:
		res.State = "failed"
		res.Error = v.Error
	}

	util.WriteJSONResponse(w, res)
}

// checkBlockState checks blocks state and returns various HTTP status codes for individual states if block
// upload cannot start, finish or file cannot be uploaded to the block.
func (c *MultitenantCompactor) checkBlockState(ctx context.Context, userBkt objstore.Bucket, blockID ulid.ULID, requireUploadInProgress bool) (*metadata.Meta, *validationFile, error) {
	s, m, v, err := c.getBlockUploadState(ctx, userBkt, blockID)
	if err != nil {
		return m, v, err
	}

	switch s {
	case blockIsComplete:
		return m, v, httpError{message: "block already exists", statusCode: http.StatusConflict}
	case blockValidationInProgress:
		return m, v, httpError{message: "block validation in progress", statusCode: http.StatusBadRequest}
	case blockUploadNotStarted:
		if requireUploadInProgress {
			return m, v, httpError{message: "block upload not started", statusCode: http.StatusNotFound}
		}
		return m, v, nil
	case blockValidationStale:
		// if validation is stale, we treat block as being in "upload in progress" state, and validation can start again.
		fallthrough
	case blockUploadInProgress:
		return m, v, nil
	case blockValidationFailed:
		return m, v, httpError{message: "block validation failed", statusCode: http.StatusBadRequest}
	}

	return m, v, httpError{message: "unknown block upload state", statusCode: http.StatusInternalServerError}
}

// getBlockUploadState returns state of the block upload, and meta and validation objects, if they exist.
func (c *MultitenantCompactor) getBlockUploadState(ctx context.Context, userBkt objstore.Bucket, blockID ulid.ULID) (blockUploadState, *metadata.Meta, *validationFile, error) {
	exists, err := userBkt.Exists(ctx, path.Join(blockID.String(), block.MetaFilename))
	if err != nil {
		return blockStateUnknown, nil, nil, err
	}
	if exists {
		return blockIsComplete, nil, nil, nil
	}

	meta, err := c.loadUploadingMeta(ctx, userBkt, blockID)
	if err != nil {
		return blockStateUnknown, nil, nil, err
	}
	// If neither meta.json nor uploading-meta.json file exist, we say that the block doesn't exist.
	if meta == nil {
		return blockUploadNotStarted, nil, nil, err
	}

	v, err := c.loadValidation(ctx, userBkt, blockID)
	if err != nil {
		return blockStateUnknown, meta, nil, err
	}
	if v == nil {
		return blockUploadInProgress, meta, nil, err
	}
	if v.Error != "" {
		return blockValidationFailed, meta, v, err
	}
	if time.Since(time.UnixMilli(v.LastUpdate)) < c.compactorCfg.BlockUpload.ValidationHeartbeatTimeout {
		return blockValidationInProgress, meta, v, nil
	}
	return blockValidationStale, meta, v, nil
}

func (c *MultitenantCompactor) loadUploadingMeta(ctx context.Context, userBkt objstore.Bucket, blockID ulid.ULID) (*metadata.Meta, error) {
	r, err := userBkt.Get(ctx, path.Join(blockID.String(), uploadingMetaFilename))
	if err != nil {
		if userBkt.IsObjNotFoundErr(err) {
			return nil, nil
		}
		return nil, err
	}
	defer func() { _ = r.Close() }()

	v := &metadata.Meta{}
	err = json.NewDecoder(r).Decode(v)
	if err != nil {
		return nil, err
	}

	return v, nil
}

func (c *MultitenantCompactor) loadValidation(ctx context.Context, userBkt objstore.Bucket, blockID ulid.ULID) (*validationFile, error) {
	r, err := userBkt.Get(ctx, path.Join(blockID.String(), validationFilename))
	if err != nil {
		if userBkt.IsObjNotFoundErr(err) {
			return nil, nil
		}
		return nil, err
	}
	defer func() { _ = r.Close() }()

	v := &validationFile{}
	err = json.NewDecoder(r).Decode(v)
	if err != nil {
		return nil, err
	}

	return v, nil
}

func (c *MultitenantCompactor) uploadValidationWithError(ctx context.Context, blockID ulid.ULID,
	userBkt objstore.Bucket, errorStr string) error {
	val := validationFile{
		LastUpdate: time.Now().UnixMilli(),
		Error:      errorStr,
	}
	dst := path.Join(blockID.String(), validationFilename)
	if err := marshalAndUploadToBucket(ctx, userBkt, dst, val); err != nil {
		return errors.Wrapf(err, "failed uploading %s to bucket", validationFilename)
	}
	return nil
}

func (c *MultitenantCompactor) uploadValidation(ctx context.Context, blockLogger log.Logger, blockID ulid.ULID,
	userBkt objstore.Bucket) error {
	return c.uploadValidationWithError(ctx, blockID, userBkt, "")
}

func (c *MultitenantCompactor) periodicValidationUpdater(ctx context.Context, blockLogger log.Logger, blockID ulid.ULID,
	userBkt objstore.Bucket, ch chan struct{}, wg *sync.WaitGroup, cancelFn func()) {
	defer wg.Done()
	ticker := time.NewTicker(c.compactorCfg.BlockUpload.ValidationHeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ch:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := c.uploadValidation(ctx, blockLogger, blockID, userBkt); err != nil {
				level.Warn(blockLogger).Log("msg", "error during periodic update of validation file", "err", err)
				cancelFn()
				return
			}
		}
	}
}

func marshalAndUploadToBucket(ctx context.Context, bkt objstore.Bucket, pth string, val interface{}) error {
	buf, err := json.Marshal(val)
	if err != nil {
		return err
	}
	if err := bkt.Upload(ctx, pth, bytes.NewReader(buf)); err != nil {
		return err
	}
	return nil
}
