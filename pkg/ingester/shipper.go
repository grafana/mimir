// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/shipper/shipper.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package ingester

import (
	"context"
	"encoding/json"
	"os"
	"path"
	"path/filepath"
	"sort"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/runutil"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/tsdb/fileutil"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/storage/tsdb/metadata"
)

type metrics struct {
	dirSyncs                 prometheus.Counter
	dirSyncFailures          prometheus.Counter
	uploads                  prometheus.Counter
	uploadFailures           prometheus.Counter
	lastSuccessfulUploadTime prometheus.Gauge
}

func newMetrics(reg prometheus.Registerer) *metrics {
	var m metrics

	m.dirSyncs = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "thanos_shipper_dir_syncs_total",
		Help: "Total number of dir syncs",
	})
	m.dirSyncFailures = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "thanos_shipper_dir_sync_failures_total",
		Help: "Total number of failed dir syncs",
	})
	m.uploads = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "thanos_shipper_uploads_total",
		Help: "Total number of uploaded blocks",
	})
	m.uploadFailures = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "thanos_shipper_upload_failures_total",
		Help: "Total number of block upload failures",
	})
	m.lastSuccessfulUploadTime = promauto.With(reg).NewGauge(prometheus.GaugeOpts{
		Name: "thanos_shipper_last_successful_upload_time",
		Help: "Unix timestamp (in seconds) of the last successful TSDB block uploaded to the bucket.",
	})

	return &m
}

// Shipper watches a directory for matching files and directories and uploads
// them to a remote data store.
// Shipper implements BlocksUploader interface.
type Shipper struct {
	logger  log.Logger
	dir     string
	metrics *metrics
	bucket  objstore.Bucket
	source  metadata.SourceType
}

// NewShipper creates a new uploader that detects new TSDB blocks in dir and uploads them to
// remote if necessary. It attaches the Thanos metadata section in each meta JSON file.
// If uploadCompacted is enabled, it also uploads compacted blocks which are already in filesystem.
func NewShipper(
	logger log.Logger,
	r prometheus.Registerer,
	dir string,
	bucket objstore.Bucket,
	source metadata.SourceType,
) *Shipper {
	if logger == nil {
		logger = log.NewNopLogger()
	}

	return &Shipper{
		logger:  logger,
		dir:     dir,
		bucket:  bucket,
		metrics: newMetrics(r),
		source:  source,
	}
}

// Sync performs a single synchronization, which ensures all non-compacted local blocks have been uploaded
// to the object bucket once.
//
// It is not concurrency-safe, however it is compactor-safe (running concurrently with compactor is ok).
func (s *Shipper) Sync(ctx context.Context) (uploaded int, err error) {
	meta, err := readShipperMetaFile(s.dir)
	if err != nil {
		// If we encounter any error, proceed with an empty meta file and overwrite it later.
		// The meta file is only used to avoid unnecessary bucket.Exists call,
		// which are properly handled by the system if their occur anyway.
		if !os.IsNotExist(err) {
			level.Warn(s.logger).Log("msg", "reading meta file failed, will override it", "err", err)
		}
		meta = &shipperMeta{Version: shipperMetaVersion1}
	}

	// Build a map of blocks we already uploaded.
	hasUploaded := make(map[ulid.ULID]struct{}, len(meta.Uploaded))
	for _, id := range meta.Uploaded {
		hasUploaded[id] = struct{}{}
	}

	// Reset the uploaded slice, so we can rebuild it only with blocks that still exist locally.
	meta.Uploaded = nil

	var uploadErrs int

	metas, err := s.blockMetasFromOldest()
	if err != nil {
		return 0, err
	}
	for _, m := range metas {
		// Do not sync a block if we already uploaded or ignored it. If it's no longer found in the bucket,
		// it was generally removed by the compaction process.
		if _, uploaded := hasUploaded[m.ULID]; uploaded {
			meta.Uploaded = append(meta.Uploaded, m.ULID)
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
			meta.Uploaded = append(meta.Uploaded, m.ULID)
			uploaded++ // the last upload must have failed, report the block as if it was uploaded successfully now
			continue
		}

		if err := s.upload(ctx, m); err != nil {
			// No error returned, just log line. This is because we want other blocks to be uploaded even
			// though this one failed. It will be retried on second Sync iteration.
			level.Error(s.logger).Log("msg", "shipping failed", "block", m.ULID, "err", err)
			uploadErrs++
			continue
		}
		meta.Uploaded = append(meta.Uploaded, m.ULID)
		uploaded++
		s.metrics.uploads.Inc()
		s.metrics.lastSuccessfulUploadTime.SetToCurrentTime()
	}
	if err := writeShipperMetaFile(s.logger, s.dir, meta); err != nil {
		level.Warn(s.logger).Log("msg", "updating meta file failed", "err", err)
	}

	s.metrics.dirSyncs.Inc()
	if uploadErrs > 0 {
		s.metrics.uploadFailures.Add(float64(uploadErrs))
		return uploaded, errors.Errorf("failed to sync %v blocks", uploadErrs)
	}

	return uploaded, nil
}

// upload method uploads the block to blocks storage. Block is uploaded with updated meta.json file with extra details.
// This updated version of meta.json is however not persisted locally on the disk, to avoid race condition when TSDB
// library could actually unload the block if it found meta.json file missing.
func (s *Shipper) upload(ctx context.Context, meta *metadata.Meta) error {
	level.Info(s.logger).Log("msg", "upload new block", "id", meta.ULID)

	blockDir := filepath.Join(s.dir, meta.ULID.String())

	meta.Thanos.Source = s.source
	meta.Thanos.SegmentFiles = block.GetSegmentFiles(blockDir)

	// Upload block with custom metadata.
	return block.Upload(ctx, s.logger, s.bucket, blockDir, meta)
}

// blockMetasFromOldest returns the block meta of each block found in dir
// sorted by minTime asc.
func (s *Shipper) blockMetasFromOldest() (metas []*metadata.Meta, _ error) {
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
		m, err := metadata.ReadFromDir(dir)
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

func readShippedBlocks(dir string) (map[ulid.ULID]struct{}, error) {
	meta, err := readShipperMetaFile(dir)
	if errors.Is(err, os.ErrNotExist) {
		// If the meta file doesn't exist it means the shipper hasn't run yet.
		meta = &shipperMeta{}
	} else if err != nil {
		return nil, err
	}

	// Build a map.
	shippedBlocks := make(map[ulid.ULID]struct{}, len(meta.Uploaded))
	for _, blockID := range meta.Uploaded {
		shippedBlocks[blockID] = struct{}{}
	}

	return shippedBlocks, nil
}

// shipperMeta defines the format thanos.shipper.json file that the shipper places in the data directory.
type shipperMeta struct {
	Version  int         `json:"version"`
	Uploaded []ulid.ULID `json:"uploaded"`
}

const (
	// shipperMetaFilename is the known JSON filename for meta information.
	shipperMetaFilename = "thanos.shipper.json"

	// shipperMetaVersion1 represents 1 version of meta.
	shipperMetaVersion1 = 1
)

// writeShipperMetaFile writes the given meta into <dir>/thanos.shipper.json.
func writeShipperMetaFile(logger log.Logger, dir string, meta *shipperMeta) error {
	// Make any changes to the file appear atomic.
	path := filepath.Join(dir, shipperMetaFilename)
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
	return renameFile(logger, tmp, path)
}

// readShipperMetaFile reads the given meta from <dir>/thanos.shipper.json.
func readShipperMetaFile(dir string) (*shipperMeta, error) {
	fpath := filepath.Join(dir, filepath.Clean(shipperMetaFilename))
	b, err := os.ReadFile(fpath)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to read %s", fpath)
	}

	var m shipperMeta
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, errors.Wrapf(err, "failed to parse %s as JSON: %q", fpath, string(b))
	}
	if m.Version != shipperMetaVersion1 {
		return nil, errors.Errorf("unexpected meta file version %d", m.Version)
	}

	return &m, nil
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
