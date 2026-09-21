// SPDX-License-Identifier: AGPL-3.0-only

package backfill

import (
	"errors"
	"flag"
	"fmt"
	"net/http"
	"path"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/compactor/blockupload"
	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/util"
	util_log "github.com/grafana/mimir/pkg/util/log"
)

const (
	jobPathVar       = "job"
	blocksPathPrefix = "data"

	// TODO: make this configurable and/or revisit it once backfill validation is implemented.
	maxBlockRange = 24 * time.Hour
)

type Config struct {
	Storage bucket.Config `yaml:"storage"`
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	cfg.Storage.RegisterFlagsWithPrefixAndDefaultDirectory("backfill.storage.", "backfillblocks", f)
}

func (cfg *Config) Validate() error {
	return cfg.Storage.Validate()
}

var errInvalidJobID = errors.New("invalid backfill job ID")

type startResult struct {
	JobID string `json:"job"`
}

type API struct {
	bucketClient objstore.Bucket
	logger       log.Logger
	upload       *blockupload.BlockUploader
}

func NewAPI(limits blockupload.Limits, bucketClient objstore.Bucket, logger log.Logger, registerer prometheus.Registerer) *API {
	logger = log.With(logger, "component", "backfill")

	return &API{
		bucketClient: bucketClient,
		logger:       logger,
		upload: blockupload.New(blockupload.Config{
			MaxBlockRange: maxBlockRange,
		}, limits, prometheus.WrapRegistererWithPrefix("cortex_backfill_upload_", registerer)),
	}
}

// TODO: lock the tenant's backfill.
func (a *API) Start(w http.ResponseWriter, r *http.Request) {
	jobID := uuid.New().String()
	level.Info(a.operationLogger(r, "start backfill", jobID)).Log("msg", "started backfill operation")

	util.WriteJSONResponse(w, startResult{JobID: jobID})
}

// TODO: hand the operation over to the cloud job flow.
func (a *API) Finish(w http.ResponseWriter, r *http.Request) {
	a.logOperation(w, r, "finish backfill", "upload reported complete")
}

// TODO: stop the in-progress operation.
func (a *API) Cancel(w http.ResponseWriter, r *http.Request) {
	a.logOperation(w, r, "cancel backfill", "backfill cancelled")
}

// TODO
func (a *API) Reset(w http.ResponseWriter, r *http.Request) {
	a.logOperation(w, r, "reset backfill", "backfill reset")
}

// TODO: report the operation state.
func (a *API) Status(w http.ResponseWriter, r *http.Request) {
	if _, err := parseJobID(r); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	http.Error(w, "backfill status is not implemented yet", http.StatusNotImplemented)
}

func (a *API) StartBlockUpload(w http.ResponseWriter, r *http.Request) {
	if bkt, logger, ok := a.acceptBlockRequest(w, r); ok {
		a.upload.StartBlockUpload(w, r, bkt, logger)
	}
}

func (a *API) UploadBlockFile(w http.ResponseWriter, r *http.Request) {
	if bkt, logger, ok := a.acceptBlockRequest(w, r); ok {
		a.upload.UploadBlockFile(w, r, bkt, logger)
	}
}

// FinishBlockUpload does not validate the block. The backfill operation validates it once every
// block of the job has arrived.
func (a *API) FinishBlockUpload(w http.ResponseWriter, r *http.Request) {
	if bkt, logger, ok := a.acceptBlockRequest(w, r); ok {
		a.upload.FinishBlockUploadWithoutValidation(w, r, bkt, logger)
	}
}

// TODO: authorize the request, confirm the job exists and is owned by the tenant.
//
// acceptBlockRequest names the job a block belongs to: it logs under the job and keeps the job's
// blocks together under their own prefix.
func (a *API) acceptBlockRequest(w http.ResponseWriter, r *http.Request) (objstore.Bucket, log.Logger, bool) {
	jobID, err := parseJobID(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return nil, nil, false
	}

	bkt := bucket.NewPrefixedBucketClient(a.bucketClient, path.Join(blocksPathPrefix, jobID))
	return bkt, log.With(a.logger, "backfill_job", jobID), true
}

func (a *API) logOperation(w http.ResponseWriter, r *http.Request, operation, msg string) {
	jobID, err := parseJobID(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	level.Info(a.operationLogger(r, operation, jobID)).Log("msg", msg)

	w.WriteHeader(http.StatusOK)
}

func (a *API) operationLogger(r *http.Request, operation, jobID string) log.Logger {
	return log.With(
		util_log.WithContext(r.Context(), a.logger),
		"operation", operation,
		"backfill_job", jobID,
	)
}

func parseJobID(r *http.Request) (string, error) {
	parsed, err := uuid.Parse(mux.Vars(r)[jobPathVar])
	if err != nil {
		return "", fmt.Errorf("%w: %w", errInvalidJobID, err)
	}

	return parsed.String(), nil
}
