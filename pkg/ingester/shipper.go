// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/shipper/shipper.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package ingester

import (
	"context"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"sort"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/shipper"
)

type metrics struct {
	dirSyncs        prometheus.Counter
	dirSyncFailures prometheus.Counter
	uploads         prometheus.Counter
	uploadFailures  prometheus.Counter
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
	labels  func() labels.Labels
	source  metadata.SourceType

	hashFunc metadata.HashFunc
}

// NewShipper creates a new uploader that detects new TSDB blocks in dir and uploads them to
// remote if necessary. It attaches the Thanos metadata section in each meta JSON file.
// If uploadCompacted is enabled, it also uploads compacted blocks which are already in filesystem.
func NewShipper(
	logger log.Logger,
	r prometheus.Registerer,
	dir string,
	bucket objstore.Bucket,
	lbls func() labels.Labels,
	source metadata.SourceType,
	hashFunc metadata.HashFunc,
) *Shipper {
	if logger == nil {
		logger = log.NewNopLogger()
	}
	if lbls == nil {
		lbls = func() labels.Labels { return nil }
	}

	return &Shipper{
		logger:   logger,
		dir:      dir,
		bucket:   bucket,
		labels:   lbls,
		metrics:  newMetrics(r),
		source:   source,
		hashFunc: hashFunc,
	}
}

// Sync performs a single synchronization, which ensures all non-compacted local blocks have been uploaded
// to the object bucket once.
//
// If uploaded.
//
// It is not concurrency-safe, however it is compactor-safe (running concurrently with compactor is ok).
func (s *Shipper) Sync(ctx context.Context) (uploaded int, err error) {
	meta, err := shipper.ReadMetaFile(s.dir)
	if err != nil {
		// If we encounter any error, proceed with an empty meta file and overwrite it later.
		// The meta file is only used to avoid unnecessary bucket.Exists call,
		// which are properly handled by the system if their occur anyway.
		if !os.IsNotExist(err) {
			level.Warn(s.logger).Log("msg", "reading meta file failed, will override it", "err", err)
		}
		meta = &shipper.Meta{Version: shipper.MetaVersion1}
	}

	// Build a map of blocks we already uploaded.
	hasUploaded := make(map[ulid.ULID]struct{}, len(meta.Uploaded))
	for _, id := range meta.Uploaded {
		hasUploaded[id] = struct{}{}
	}

	// Reset the uploaded slice so we can rebuild it only with blocks that still exist locally.
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
	}
	if err := shipper.WriteMetaFile(s.logger, s.dir, meta); err != nil {
		level.Warn(s.logger).Log("msg", "updating meta file failed", "err", err)
	}

	s.metrics.dirSyncs.Inc()
	if uploadErrs > 0 {
		s.metrics.uploadFailures.Add(float64(uploadErrs))
		return uploaded, errors.Errorf("failed to sync %v blocks", uploadErrs)
	}

	return uploaded, nil
}

// sync uploads the block if not exists in remote storage.
// TODO(khyatisoneji): Double check if block does not have deletion-mark.json for some reason, otherwise log it or return error.
func (s *Shipper) upload(ctx context.Context, meta *metadata.Meta) error {
	level.Info(s.logger).Log("msg", "upload new block", "id", meta.ULID)

	// We hard-link the files into a temporary upload directory so we are not affected
	// by other operations happening against the TSDB directory.
	updir := filepath.Join(s.dir, "thanos", "upload", meta.ULID.String())

	// Remove updir just in case.
	if err := os.RemoveAll(updir); err != nil {
		return errors.Wrap(err, "clean upload directory")
	}
	if err := os.MkdirAll(updir, 0750); err != nil {
		return errors.Wrap(err, "create upload dir")
	}
	defer func() {
		if err := os.RemoveAll(updir); err != nil {
			level.Error(s.logger).Log("msg", "failed to clean upload directory", "err", err)
		}
	}()

	dir := filepath.Join(s.dir, meta.ULID.String())
	if err := hardlinkBlock(dir, updir); err != nil {
		return errors.Wrap(err, "hard link block")
	}
	// Attach current labels and write a new meta file with Thanos extensions.
	if lset := s.labels(); lset != nil {
		meta.Thanos.Labels = lset.Map()
	}
	meta.Thanos.Source = s.source
	meta.Thanos.SegmentFiles = block.GetSegmentFiles(updir)
	if err := meta.WriteToDir(s.logger, updir); err != nil {
		return errors.Wrap(err, "write meta file")
	}
	return block.Upload(ctx, s.logger, s.bucket, updir, s.hashFunc)
}

// blockMetasFromOldest returns the block meta of each block found in dir
// sorted by minTime asc.
func (s *Shipper) blockMetasFromOldest() (metas []*metadata.Meta, _ error) {
	fis, err := ioutil.ReadDir(s.dir)
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

func hardlinkBlock(src, dst string) error {
	chunkDir := filepath.Join(dst, block.ChunksDirname)

	if err := os.MkdirAll(chunkDir, 0750); err != nil {
		return errors.Wrap(err, "create chunks dir")
	}

	fis, err := ioutil.ReadDir(filepath.Join(src, block.ChunksDirname))
	if err != nil {
		return errors.Wrap(err, "read chunk dir")
	}
	files := make([]string, 0, len(fis))
	for _, fi := range fis {
		files = append(files, fi.Name())
	}
	for i, fn := range files {
		files[i] = filepath.Join(block.ChunksDirname, fn)
	}
	files = append(files, block.MetaFilename, block.IndexFilename)

	for _, fn := range files {
		if err := os.Link(filepath.Join(src, fn), filepath.Join(dst, fn)); err != nil {
			return errors.Wrapf(err, "hard link file %s", fn)
		}
	}
	return nil
}

func readShippedBlocks(dir string) (map[ulid.ULID]struct{}, error) {
	shipperMeta, err := shipper.ReadMetaFile(dir)
	if errors.Is(err, os.ErrNotExist) {
		// If the meta file doesn't exist it means the shipper hasn't run yet.
		shipperMeta = &shipper.Meta{}
	} else if err != nil {
		return nil, err
	}

	// Build a map.
	shippedBlocks := make(map[ulid.ULID]struct{}, len(shipperMeta.Uploaded))
	for _, blockID := range shipperMeta.Uploaded {
		shippedBlocks[blockID] = struct{}{}
	}

	return shippedBlocks, nil
}
