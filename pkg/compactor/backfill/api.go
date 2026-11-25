// SPDX-License-Identifier: AGPL-3.0-only

package backfill

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/grafana/dskit/tenant"
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

	BucketPrefix = bucket.MimirInternalsPrefix + "/backfill"

	// TODO: make this configurable and/or revisit it once backfill validation is implemented.
	maxBlockRange = 24 * time.Hour
)

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
		bucketClient: bucket.NewPrefixedBucketClient(bucketClient, BucketPrefix),
		logger:       logger,
		upload: blockupload.New(blockupload.Config{
			MaxBlockRange: maxBlockRange,
		}, limits, prometheus.WrapRegistererWithPrefix("cortex_backfill_upload_", registerer)),
	}
}

// Start starts a backfill for the tenant, which has at most one backfill at a time.
func (a *API) Start(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	tenantID, err := tenant.TenantID(ctx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// TODO: two concurrent starts can both pass this check, since object storage has no conditional writes
	active, err := a.hasActiveBackfill(ctx, tenantID)
	if err != nil {
		level.Error(a.logger).Log("msg", "failed to check for an active backfill", "user", tenantID, "err", err)
		http.Error(w, "failed to check for an active backfill", http.StatusInternalServerError)
		return
	}
	if active {
		http.Error(w, "a backfill is already in progress", http.StatusConflict)
		return
	}

	jobID := uuid.New()
	// The marker is written before any data, so the compactor never takes the backfill's data as orphaned
	if err := WriteMarker(ctx, a.bucketClient, tenantID, jobID, PhaseBackfill, Marker{}); err != nil {
		level.Error(a.logger).Log("msg", "failed to start backfill", "user", tenantID, "err", err)
		http.Error(w, "failed to start backfill", http.StatusInternalServerError)
		return
	}
	level.Info(a.operationLogger(r, "start backfill", jobID)).Log("msg", "started backfill operation")

	util.WriteJSONResponse(w, startResult{JobID: jobID.String()})
}

// hasActiveBackfill reports whether the tenant has a backfill that isn't fully cleaned up
func (a *API) hasActiveBackfill(ctx context.Context, tenantID string) (bool, error) {
	ids, err := ListBackfills(ctx, a.bucketClient, tenantID)
	if err != nil {
		return false, err
	}
	for _, id := range ids {
		for _, phase := range []string{PhaseBackfill, PhaseCleanup} {
			exists, err := a.bucketClient.Exists(ctx, PhaseMarkerPath(tenantID, id, phase))
			if err != nil {
				return false, err
			}
			if exists {
				return true, nil
			}
		}
	}
	return false, nil
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

// TODO: authorize the request.
//
// acceptBlockRequest names the job a block belongs to: it logs under the job and keeps the job's
// blocks together under their own prefix. Only blocks of the tenant's backfills that are still
// uploading are accepted.
func (a *API) acceptBlockRequest(w http.ResponseWriter, r *http.Request) (objstore.Bucket, log.Logger, bool) {
	jobID, err := parseJobID(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return nil, nil, false
	}
	ctx := r.Context()
	tenantID, err := tenant.TenantID(ctx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return nil, nil, false
	}
	logger := log.With(a.logger, "user", tenantID, "backfill_job", jobID)

	started, err := a.bucketClient.Exists(ctx, PhaseMarkerPath(tenantID, jobID, PhaseBackfill))
	if err != nil {
		level.Error(logger).Log("msg", "failed to check backfill marker", "err", err)
		http.Error(w, "failed to check backfill", http.StatusInternalServerError)
		return nil, nil, false
	}
	if !started {
		http.Error(w, "backfill not found", http.StatusNotFound)
		return nil, nil, false
	}
	cleaning, err := a.bucketClient.Exists(ctx, PhaseMarkerPath(tenantID, jobID, PhaseCleanup))
	if err != nil {
		level.Error(logger).Log("msg", "failed to check cleanup marker", "err", err)
		http.Error(w, "failed to check backfill", http.StatusInternalServerError)
		return nil, nil, false
	}
	if cleaning {
		http.Error(w, "backfill is being cleaned up", http.StatusConflict)
		return nil, nil, false
	}

	return bucket.NewPrefixedBucketClient(a.bucketClient, DataPrefix(tenantID, jobID)), logger, true
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

func (a *API) operationLogger(r *http.Request, operation string, jobID uuid.UUID) log.Logger {
	return log.With(
		util_log.WithContext(r.Context(), a.logger),
		"operation", operation,
		"backfill_job", jobID,
	)
}

func parseJobID(r *http.Request) (uuid.UUID, error) {
	parsed, err := uuid.Parse(mux.Vars(r)[jobPathVar])
	if err != nil {
		return uuid.UUID{}, fmt.Errorf("%w: %w", errInvalidJobID, err)
	}

	return parsed, nil
}
