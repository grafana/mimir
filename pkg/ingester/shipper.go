// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/shipper/shipper.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package ingester

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/runutil"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/tsdb/fileutil"
	"github.com/thanos-io/objstore"

	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
)

// ShipperMetrics holds the Shipper metrics. Mimir runs 1 Shipper for each tenant but
// the metrics instance is shared across all tenants.
type ShipperMetrics struct {
	uploads                  prometheus.Counter
	uploadFailures           prometheus.Counter
	lastSuccessfulUploadTime prometheus.Gauge
}

func NewShipperMetrics(reg prometheus.Registerer, component string) *ShipperMetrics {
	return &ShipperMetrics{
		uploads: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: fmt.Sprintf("cortex_%s_shipper_uploads_total", component),
			Help: "Total number of uploaded TSDB blocks",
		}),
		uploadFailures: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: fmt.Sprintf("cortex_%s_shipper_upload_failures_total", component),
			Help: "Total number of TSDB block upload failures",
		}),
		lastSuccessfulUploadTime: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: fmt.Sprintf("cortex_%s_shipper_last_successful_upload_timestamp_seconds", component),
			Help: "Unix timestamp (in seconds) of the last successful TSDB block uploaded to the object storage.",
		}),
	}
}

type ShipperConfigProvider interface {
	OutOfOrderBlocksExternalLabelEnabled(userID string) bool
}

// Shipper watches a directory for matching files and directories and uploads
// them to a remote data store.
// Shipper implements BlocksUploader interface.
type Shipper struct {
	logger      log.Logger
	cfgProvider ShipperConfigProvider
	userID      string
	dir         string
	metrics     *ShipperMetrics
	bucket      objstore.Bucket
	source      block.SourceType
}

// NewShipper creates a new uploader that detects new TSDB blocks in dir and uploads them to
// remote if necessary. It attaches the Thanos metadata section in each meta JSON file.
// If uploadCompacted is enabled, it also uploads compacted blocks which are already in filesystem.
func NewShipper(
	logger log.Logger,
	cfgProvider ShipperConfigProvider,
	userID string,
	metrics *ShipperMetrics,
	dir string,
	bucket objstore.Bucket,
	source block.SourceType,
) *Shipper {
	if logger == nil {
		logger = log.NewNopLogger()
	}

	return &Shipper{
		logger:      logger,
		cfgProvider: cfgProvider,
		userID:      userID,
		dir:         dir,
		bucket:      bucket,
		metrics:     metrics,
		source:      source,
	}
}

// Sync performs a single synchronization, which ensures all non-compacted local blocks have been uploaded
// to the object bucket once.
//
// It is not concurrency-safe, however it is compactor-safe (running concurrently with compactor is ok).
func (s *Shipper) Sync(ctx context.Context) (shipped int, err error) {
	shippedBlocks, err := ReadShippedBlocks(s.dir)
	if err != nil {
		// If we encounter any error, proceed with an new list of shipped blocks.
		// The meta file will be overridden later. Note that the meta file is only
		// used to avoid unnecessary bucket.Exists call, which are properly handled
		// by the system if their occur anyway.
		level.Warn(s.logger).Log("msg", "reading meta file failed, will override it", "err", err)

		// Reset the shipped blocks slice, so we can rebuild it only with blocks that still exist locally.
		shippedBlocks = map[ulid.ULID]time.Time{}
	}

	meta := ShipperMeta{Version: ShipperMetaVersion1, Shipped: map[ulid.ULID]model.Time{}}
	var uploadErrs int

	metas, err := s.blockMetasFromOldest()
	if err != nil {
		return 0, err
	}
	for _, m := range metas {
		// Do not sync a block if we already shipped or ignored it. If it's no longer found in the bucket,
		// it was generally removed by the compaction process.
		if shippedTime, shipped := shippedBlocks[m.ULID]; shipped {
			meta.Shipped[m.ULID] = model.TimeFromUnixNano(shippedTime.UnixNano())
			continue
		}

		if m.Stats.NumSamples == 0 {
			// Ignore empty blocks.
			level.Debug(s.logger).Log("msg", "ignoring empty block", "block", m.ULID)
			continue
		}

		// We only ship of the first compacted block level as normal flow.
		if m.Compaction.Level > 1 {
			continue
		}

		// Check against bucket if the meta file for this block exists.
		ok, err := s.bucket.Exists(ctx, path.Join(m.ULID.String(), block.MetaFilename))
		if err != nil {
			return 0, errors.Wrap(err, "check exists")
		}
		if ok {
			// We decide to be conservative here and assume it was just recently uploaded.
			// It would be very rare to have blocks uploaded but not tracked in the Shipper meta file.
			// This could happen if process crashed while uploading the block.
			meta.Shipped[m.ULID] = model.Now()
			shipped++ // the last upload must have failed, report the block as if it was shipped successfully now
			continue
		}

		level.Info(s.logger).Log("msg", "uploading new block to long-term storage", "block", m.ULID)
		if err := s.upload(ctx, m); err != nil {
			// No error returned, just log line. This is because we want other blocks to be shipped even
			// though this one failed. It will be retried on second Sync iteration.
			level.Error(s.logger).Log("msg", "uploading new block to long-term storage failed", "block", m.ULID, "err", err)
			uploadErrs++
			continue
		}
		level.Info(s.logger).Log("msg", "finished uploading new block to long-term storage", "block", m.ULID)

		meta.Shipped[m.ULID] = model.Now()
		shipped++
		s.metrics.uploads.Inc()
		s.metrics.lastSuccessfulUploadTime.SetToCurrentTime()
	}

	if err := WriteShipperMetaFile(s.logger, s.dir, meta); err != nil {
		level.Warn(s.logger).Log("msg", "updating meta file failed", "err", err)
	}

	if uploadErrs > 0 {
		s.metrics.uploadFailures.Add(float64(uploadErrs))
		return shipped, errors.Errorf("failed to sync %v blocks", uploadErrs)
	}

	return shipped, nil
}

// upload method uploads the block to blocks storage. Block is uploaded with updated meta.json file with extra details.
// This updated version of meta.json is however not persisted locally on the disk, to avoid race condition when TSDB
// library could actually unload the block if it found meta.json file missing.
func (s *Shipper) upload(ctx context.Context, meta *block.Meta) error {
	blockDir := filepath.Join(s.dir, meta.ULID.String())

	meta.Thanos.Source = s.source
	meta.Thanos.SegmentFiles = block.GetSegmentFiles(blockDir)

	if meta.Compaction.FromOutOfOrder() && s.cfgProvider.OutOfOrderBlocksExternalLabelEnabled(s.userID) {
		// At this point the OOO data was already ingested and compacted, so there's no point in checking for the OOO feature flag
		meta.Thanos.Labels[mimir_tsdb.OutOfOrderExternalLabel] = mimir_tsdb.OutOfOrderExternalLabelValue
	}

	// Upload block with custom metadata.
	return block.Upload(ctx, s.logger, s.bucket, blockDir, meta)
}

// blockMetasFromOldest returns the block meta of each block found in dir
// sorted by minTime asc.
func (s *Shipper) blockMetasFromOldest() (metas []*block.Meta, _ error) {
	fis, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, errors.Wrap(err, "read dir")
	}
	names := make([]string, 0, len(fis))
	for _, fi := range fis {
		names = append(names, fi.Name())
	}
	for _, n := range names {
		if _, ok := block.IsBlockDir(n); !ok {
			continue
		}
		dir := filepath.Join(s.dir, n)

		fi, err := os.Stat(dir)
		if err != nil {
			return nil, errors.Wrapf(err, "stat block %v", dir)
		}
		if !fi.IsDir() {
			continue
		}
		m, err := block.ReadMetaFromDir(dir)
		if err != nil {
			return nil, errors.Wrapf(err, "read metadata for block %v", dir)
		}
		metas = append(metas, m)
	}
	sort.Slice(metas, func(i, j int) bool {
		return metas[i].BlockMeta.MinTime < metas[j].BlockMeta.MinTime
	})
	return metas, nil
}

// ReadShippedBlocks returns all the shipped blocks in this directory with their shipped time.
func ReadShippedBlocks(dir string) (map[ulid.ULID]time.Time, error) {
	meta, err := readShipperMetaFile(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// If the meta file doesn't exist it means the Shipper hasn't run yet.
			meta = ShipperMeta{}
		} else {
			return nil, err
		}
	}

	shippedBlocks := make(map[ulid.ULID]time.Time, len(meta.Shipped))
	for blockID, shippedTime := range meta.Shipped {
		shippedBlocks[blockID] = shippedTime.Time()
	}
	return shippedBlocks, nil
}

// ShipperMeta defines the format mimir.Shipper.json file that the Shipper places in the data directory.
type ShipperMeta struct {
	Version int                      `json:"version"`
	Shipped map[ulid.ULID]model.Time `json:"shipped"`
}

const (
	// ShipperMetaFilename is the known JSON filename for meta information.
	ShipperMetaFilename = "mimir.Shipper.json"

	// ShipperMetaVersion1 represents 1 version of meta.
	ShipperMetaVersion1 = 1
)

// WriteShipperMetaFile writes the given meta into <dir>/mimir.Shipper.json.
func WriteShipperMetaFile(logger log.Logger, dir string, meta ShipperMeta) error {
	path := filepath.Join(dir, ShipperMetaFilename)
	tmp := path + ".tmp"

	f, err := os.Create(tmp)
	if err != nil {
		return err
	}

	enc := json.NewEncoder(f)
	enc.SetIndent("", "\t")

	if err := enc.Encode(meta); err != nil {
		runutil.CloseWithLogOnErr(logger, f, "write meta file close")
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}

	err = renameFile(logger, tmp, path)
	if err != nil {
		return errors.Wrap(err, "writing mimir shipped meta file")
	}

	return nil
}

// readShipperMetaFile reads the given meta from <dir>/mimir.Shipper.json.
func readShipperMetaFile(dir string) (ShipperMeta, error) {
	fpath := filepath.Join(dir, filepath.Clean(ShipperMetaFilename))
	b, err := os.ReadFile(fpath)
	if err != nil {
		return ShipperMeta{}, errors.Wrapf(err, "failed to read %s", fpath)
	}

	var m ShipperMeta
	if err := json.Unmarshal(b, &m); err != nil {
		return ShipperMeta{}, errors.Wrapf(err, "failed to parse %s as JSON: %q", fpath, string(b))
	}
	if m.Version != ShipperMetaVersion1 {
		return ShipperMeta{}, errors.Errorf("unexpected meta file version %d", m.Version)
	}

	return m, nil
}

func renameFile(logger log.Logger, from, to string) error {
	if err := os.RemoveAll(to); err != nil {
		return err
	}
	if err := os.Rename(from, to); err != nil {
		return err
	}

	// Directory was renamed; sync parent dir to persist rename.
	pdir, err := fileutil.OpenDir(filepath.Dir(to))
	if err != nil {
		return err
	}

	if err = fileutil.Fdatasync(pdir); err != nil {
		runutil.CloseWithLogOnErr(logger, pdir, "rename file dir close")
		return err
	}
	return pdir.Close()
}
