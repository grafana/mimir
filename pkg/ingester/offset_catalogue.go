// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/runutil"

	"github.com/grafana/mimir/pkg/util/atomicfs"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

const (
	offsetCatalogueVersion = 1
)

type offsetWatermark struct {
	Partition int32 `json:"partition"`
	Offset    int64 `json:"offset"`
}

func (o offsetWatermark) String() string {
	return fmt.Sprintf("%d/%d", o.Partition, o.Offset)
}

type offsetCatalogueData struct {
	Version   int                        `json:"version"`
	UpdatedAt int64                      `json:"updated_at"`
	Data      map[string]offsetWatermark `json:"data"`
}

type offsetCatalogue struct {
	logger    log.Logger
	dir       string
	userID    string
	partition int32
}

func newOffsetCatalogue(logger log.Logger, dir, userID string, partition int32) *offsetCatalogue {
	return &offsetCatalogue{
		logger:    logger,
		dir:       dir,
		userID:    userID,
		partition: partition,
	}
}

const offsetCatalogueFilename = "offset-catalogue.json"

func (c *offsetCatalogue) Sync(ctx context.Context, offsetHW int64) error {
	spanLogger, ctx := spanlogger.New(ctx, c.logger, tracer, "Ingester.OffsetCatalogue.Sync")
	defer spanLogger.Finish()

	oldData, err := readOffsetCatalogueFromFile(c.dir)
	if errors.Is(err, os.ErrNotExist) {
		// The catalogue file may not exist if that's the first sync of this new tenant.
		oldData.Data = map[string]offsetWatermark{}
	} else if err != nil {
		return fmt.Errorf("read offset catalogue: %w", err)
	}

	blocks := make(map[string]struct{})
	for id, err := range listBlocks(c.dir) {
		if err != nil {
			return err
		}
		blocks[id.String()] = struct{}{}
	}

	data := offsetCatalogueData{
		Version:   offsetCatalogueVersion,
		UpdatedAt: time.Now().Unix(),
		Data:      make(map[string]offsetWatermark, len(blocks)),
	}
	for id := range blocks {
		if mark, ok := oldData.Data[id]; ok {
			// If block already exists in the previous catalogue, keep it.
			data.Data[id] = mark
			continue
		}
		// If block wasn't found in the catalogue (e.g. block existed before start),
		// or block's watermark offset wasn't captured, fallback to the most recent offsetHW.
		// This is conservative: if block was found on disk, its data came from offset lower than current offsetHW.
		data.Data[id] = offsetWatermark{
			Partition: c.partition,
			Offset:    offsetHW,
		}
	}

	return writeOffsetCatalogueToFile(c.dir, data)
}

func readOffsetCatalogueFromFile(dir string) (offsetCatalogueData, error) {
	filePath := filepath.Join(dir, offsetCatalogueFilename)
	b, err := os.ReadFile(filePath)
	if err != nil {
		return offsetCatalogueData{}, fmt.Errorf("read %s: %w", filePath, err)
	}

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
