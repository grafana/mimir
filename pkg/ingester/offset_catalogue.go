// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/runutil"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/storage/ingest/kmeta"
	"github.com/grafana/mimir/pkg/util/atomicfs"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

type offsetCatalogueMetrics struct {
	syncs            prometheus.Counter
	syncFailures     prometheus.Counter
	lastSyncTime     prometheus.Gauge
	lastSyncedOffset *prometheus.GaugeVec
}

func newOffsetCatalogueMetrics(reg prometheus.Registerer) *offsetCatalogueMetrics {
	return &offsetCatalogueMetrics{
		syncs: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_ingester_offset_catalogue_syncs_total",
			Help: "Total number of successful offset catalogue syncs.",
		}),
		syncFailures: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_ingester_offset_catalogue_sync_failures_total",
			Help: "Total number of offset catalogue sync failures.",
		}),
		lastSyncTime: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "cortex_ingester_offset_catalogue_last_sync_timestamp_seconds",
			Help: "Unix timestamp (in seconds) of the last successful offset catalogue sync.",
		}),
		lastSyncedOffset: promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Name: "cortex_ingester_offset_catalogue_last_synced_offset",
			Help: "The last-seen offset used as the high-water mark in the most recent successful offset catalogue sync.",
		}, []string{"write_compartment"}),
	}
}

const (
	offsetCatalogueVersion = 2
)

var (
	errOffsetCatalogueSyncEmptyOffsets  = errors.New("no offsets to sync")
	errOffsetCatalogueSyncVersionTooOld = errors.New("offset catalogue version is too old")
)

type offsetWatermark struct {
	Partition int32 `json:"p"`
	Offset    int64 `json:"o"`
}

func (o offsetWatermark) String() string {
	return fmt.Sprintf("%d/%d", o.Partition, o.Offset)
}

type offsetCatalogueData struct {
	Version   int   `json:"version"`
	UpdatedAt int64 `json:"updated_at"`

	// Data holds block -> cluster -> offset-watermark mapping.
	Data map[string]map[int]offsetWatermark `json:"data"`
}

type offsetCatalogue struct {
	logger    log.Logger
	metrics   *offsetCatalogueMetrics
	dir       string
	userID    string
	partition int32

	// cachedData is a pointer to the latest version of catalogue data.
	cachedData atomic.Pointer[offsetCatalogueData]
}

func newOffsetCatalogue(logger log.Logger, metrics *offsetCatalogueMetrics, dir, userID string, partition int32) *offsetCatalogue {
	return &offsetCatalogue{
		logger:    logger,
		metrics:   metrics,
		dir:       dir,
		userID:    userID,
		partition: partition,
	}
}

const offsetCatalogueFilename = "offset-catalogue.json"

func (c *offsetCatalogue) Sync(ctx context.Context, offsetHWs kmeta.PartitionOffsets) (err error) {
	spanLogger, ctx := spanlogger.New(ctx, c.logger, tracer, "Ingester.OffsetCatalogue.Sync")
	defer spanLogger.Finish()

	defer func() {
		if err != nil && !errors.Is(err, context.Canceled) {
			c.metrics.syncFailures.Inc()
		}
	}()

	oldData, err := readOffsetCatalogueFromFile(c.dir)
	if err != nil {
		// Missing or outdated catalogues are rebuilt from the blocks on disk.
		// Corrupted catalogues are always overwritten with a fresh copy.
		if !errors.Is(err, os.ErrNotExist) && !errors.Is(err, errOffsetCatalogueSyncVersionTooOld) {
			level.Warn(spanLogger).Log("msg", "reading offset catalogue failed, will override", "err", err)
		}
		oldData.Data = map[string]map[int]offsetWatermark{}
	}

	blocks := make(map[string]struct{})
	for id, err := range listBlocks(c.dir) {
		if err != nil {
			return err
		}
		blocks[id.String()] = struct{}{}
	}

	offsets := make(map[int]offsetWatermark, offsetHWs.NumKafkaClusters())
	for clusterID, offset := range offsetHWs {
		// Ignore empty offsets, e.g. if Sync runs before partition reader init is finished, or when a read compartment partition is empty.
		if offset == -1 {
			continue
		}
		offsets[clusterID] = offsetWatermark{Partition: c.partition, Offset: offset}
	}

	if len(offsets) == 0 {
		return errOffsetCatalogueSyncEmptyOffsets
	}

	data := offsetCatalogueData{
		Version:   offsetCatalogueVersion,
		UpdatedAt: time.Now().Unix(),
		Data:      make(map[string]map[int]offsetWatermark, len(blocks)),
	}
	for id := range blocks {
		if err := ctx.Err(); err != nil {
			return err
		}

		if oldOffsets, ok := oldData.Data[id]; ok {
			// If block already exists in the previous catalogue keep it.
			// Make sure to add offsets for the missing clusters. This can happen when the previous Sync saw an empty read compartment.
			if len(oldOffsets) < len(offsets) {
				for clusterID, offset := range offsets {
					if _, ok := oldOffsets[clusterID]; !ok {
						oldOffsets[clusterID] = offset
					}
				}
			}
			data.Data[id] = oldOffsets
			continue
		}
		// If block wasn't found in the catalogue (e.g. block existed before start),
		// or block's watermark offset wasn't captured, fallback to the most recent offsetHW.
		// This is conservative: if block was found on disk, its data came from offset lower than current offsetHW.
		data.Data[id] = offsets
	}

	if err := writeOffsetCatalogueToFile(c.dir, data); err != nil {
		return err
	}

	c.cachedData.Store(&data)

	c.metrics.syncs.Inc()
	c.metrics.lastSyncTime.SetToCurrentTime()
	for clusterID, offset := range offsets {
		// Last synced offset is per-write-compartment cluster.
		c.metrics.lastSyncedOffset.WithLabelValues(strconv.Itoa(clusterID)).Set(float64(offset.Offset))
	}

	return nil
}

func (c *offsetCatalogue) Data() offsetCatalogueData {
	// cachedData is replaced on successful sync; it's safe to return the current copy.
	if data := c.cachedData.Load(); data != nil {
		return *data
	}
	// Return a dummy version of data, to simplify how the method is used.
	return offsetCatalogueData{
		Version: offsetCatalogueVersion,
		Data:    make(map[string]map[int]offsetWatermark),
	}
}

// offsetWatermarksCommitted returns true if the committed offset of every Kafka cluster reached the block's watermark in that cluster.
func offsetWatermarksCommitted(watermarks map[int]offsetWatermark, committed kmeta.PartitionOffsets) bool {
	if len(watermarks) == 0 {
		return false
	}
	for clusterID, wm := range watermarks {
		if wm.Offset > committed.ForKafkaCluster(clusterID) {
			return false
		}
	}
	return true
}

func readOffsetCatalogueFromFile(dir string) (_ offsetCatalogueData, retErr error) {
	filePath := filepath.Join(dir, offsetCatalogueFilename)
	b, err := os.ReadFile(filePath)
	if err != nil {
		return offsetCatalogueData{}, fmt.Errorf("read %s: %w", filePath, err)
	}

	defer func() {
		if retErr == nil {
			return
		}
		// Quick check if the version of the file was too old.
		header := struct {
			Version int `json:"version"`
		}{}
		if err := json.Unmarshal(b, &header); err == nil && header.Version < offsetCatalogueVersion {
			retErr = fmt.Errorf("%w: expect version %d, got %d", errOffsetCatalogueSyncVersionTooOld, offsetCatalogueVersion, header.Version)
		}
	}()

	var data offsetCatalogueData
	if err := json.Unmarshal(b, &data); err != nil {
		return offsetCatalogueData{}, fmt.Errorf("parse json %s: %w", filePath, err)
	}
	if data.Version != offsetCatalogueVersion {
		return offsetCatalogueData{}, fmt.Errorf("expected version %d got %d", offsetCatalogueVersion, data.Version)
	}
	return data, nil
}

func writeOffsetCatalogueToFile(dir string, data offsetCatalogueData) (err error) {
	filePath := filepath.Join(dir, offsetCatalogueFilename)

	f, err := atomicfs.Create(filePath)
	if err != nil {
		return fmt.Errorf("create %s: %w", filePath, err)
	}
	defer runutil.CloseWithErrCapture(&err, f, "write offset catalogue to file %s", filePath)

	enc := json.NewEncoder(f)
	enc.SetIndent("", "\t")

	if err := enc.Encode(data); err != nil {
		return fmt.Errorf("encode data to file %s: %w", filePath, err)
	}
	return nil
}
