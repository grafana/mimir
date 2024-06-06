package blockbuilder

import (
	"context"
	"os"
	"path/filepath"
	"time"

	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/concurrency"

	"github.com/grafana/mimir/pkg/ingester"
	"github.com/grafana/mimir/pkg/storage/bucket"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
)

type shipper interface {
	Sync(ctx context.Context) (int, error)
}

func (b *BlockBuilder) addShippers(userIDs []string) {
	b.shipperMtx.Lock()
	defer b.shipperMtx.Unlock()

	for _, userID := range userIDs {
		if b.shippers[userID] != nil {
			continue
		}

		// TODO(codesome): consider moving shipper inside utils or its own package.
		b.shippers[userID] = ingester.NewShipper(
			b.logger,
			b.limits,
			userID,
			b.shipperMetrics,
			b.shipperUserDir(userID),
			bucket.NewUserBucketClient(userID, b.bucket, b.limits),
			block.ReceiveSource,
		)
	}
}

func (b *BlockBuilder) shipperRootDir() string {
	return filepath.Join(b.cfg.BlocksStorageConfig.TSDB.Dir, "shipper")
}

func (b *BlockBuilder) shipperUserDir(userID string) string {
	return filepath.Join(b.shipperRootDir(), userID)
}

func (b *BlockBuilder) shipBlocksLoop(ctx context.Context) error {
	// We aggressively look to ship blocks because BlockBuilder is intended to be stateless.
	shipTicker := time.NewTicker(time.Minute)
	defer shipTicker.Stop()

	for {
		select {
		case <-shipTicker.C:
			b.shipBlocks(ctx)

		case <-ctx.Done():
			return nil
		}
	}
}

func (b *BlockBuilder) shipBlocks(ctx context.Context) {
	b.shipperMtx.RLock()
	userIDs := make([]string, 0, len(b.shippers))
	shipperCopy := make(map[string]shipper, len(b.shippers))
	for k, v := range b.shippers {
		shipperCopy[k] = v
		userIDs = append(userIDs, k)
	}
	b.shipperMtx.RUnlock()

	// Number of concurrent workers is limited in order to avoid to concurrently sync a lot
	// of tenants in a large cluster.
	_ = concurrency.ForEachUser(ctx, userIDs, b.cfg.BlocksStorageConfig.TSDB.ShipConcurrency, func(ctx context.Context, userID string) error {
		sh := shipperCopy[userID]
		if sh == nil {
			return nil
		}

		uploaded, err := sh.Sync(ctx)
		if err != nil {
			level.Warn(b.logger).Log("msg", "Shipper failed to synchronize TSDB blocks with the storage", "user", userID, "uploaded", uploaded, "err", err)
		} else {
			level.Debug(b.logger).Log("msg", "Shipper successfully synchronized TSDB blocks with storage", "user", userID, "uploaded", uploaded)
		}

		// Delete all shipped blocks.
		dir := b.shipperUserDir(userID)
		shippedBlocks, err := ingester.ReadShippedBlocks(dir)
		if err != nil {
			level.Error(b.logger).Log("msg", "failed to read shipped blocks", "user", userID, "err", err)
			return nil
		}
		// Remove the shipped blocks.
		for blockID := range shippedBlocks {
			if err := os.RemoveAll(filepath.Join(dir, blockID.String())); err != nil {
				level.Error(b.logger).Log("msg", "failed to remove shipped block", "user", userID, "block", blockID, "err", err)
			}
		}

		return nil
	})
}
