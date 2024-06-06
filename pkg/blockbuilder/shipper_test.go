package blockbuilder

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/ingester"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
)

func TestAddShippers(t *testing.T) {
	b := BlockBuilder{shippers: make(map[string]shipper)}

	b.addShippers([]string{"user1", "user2"})
	require.Equal(t, 2, len(b.shippers))
	require.NotNil(t, b.shippers["user1"])
	require.NotNil(t, b.shippers["user2"])

	// Adding the same users again should not override it and
	// nil shipper should be overridden.
	u1, u2 := b.shippers["user1"], b.shippers["user2"]
	b.shippers["user4"] = nil
	b.addShippers([]string{"user3", "user2", "user4"})
	require.Equal(t, 4, len(b.shippers))
	require.NotNil(t, b.shippers["user3"])
	require.NotNil(t, b.shippers["user4"])
	require.Equal(t, u1, b.shippers["user1"])
	require.Equal(t, u2, b.shippers["user2"])
}

func TestBlockDeletionAfterShipping(t *testing.T) {
	dbDir := t.TempDir()
	b := BlockBuilder{
		logger: log.NewNopLogger(),
		cfg: Config{
			BlocksStorageConfig: mimir_tsdb.BlocksStorageConfig{
				TSDB: mimir_tsdb.TSDBConfig{
					Dir:             dbDir,
					ShipConcurrency: 1,
				},
			},
		},
		shippers: make(map[string]shipper),
	}

	userID := "user1"
	ms := &mockShipper{t: t, dir: b.shipperUserDir(userID)}
	b.shippers[userID] = ms

	blockID, err := block.CreateBlock(
		context.Background(), b.shipperUserDir(userID),
		[]labels.Labels{
			labels.FromStrings("a", "b1"),
			labels.FromStrings("a", "b2"),
			labels.FromStrings("a", "b3"),
		},
		10, 100, 200, nil,
	)
	require.NoError(t, err)

	blockDir := filepath.Join(b.shipperUserDir(userID), blockID.String())
	ms.addBlock(blockID)

	// Block should exist before shipping.
	_, err = os.Stat(blockDir)
	require.NoError(t, err)

	require.Equal(t, 0, ms.synced)
	b.shipBlocks(context.Background())
	require.Equal(t, 1, ms.synced)

	// Block deleted after shipping.
	_, err = os.Stat(blockDir)
	require.True(t, errors.Is(err, os.ErrNotExist), "Block still exists after shipping")
}

type mockShipper struct {
	t        *testing.T
	blockIDs []ulid.ULID
	dir      string
	synced   int
}

func (s *mockShipper) addBlock(id ulid.ULID) {
	s.blockIDs = append(s.blockIDs, id)
}

func (s *mockShipper) Sync(ctx context.Context) (int, error) {
	s.synced++
	sm := ingester.ShipperMeta{
		Version: ingester.ShipperMetaVersion1,
		Shipped: map[ulid.ULID]model.Time{},
	}
	for _, id := range s.blockIDs {
		sm.Shipped[id] = model.Time(time.Now().UnixMilli())
	}
	err := ingester.WriteShipperMetaFile(log.NewNopLogger(), s.dir, sm)
	require.NoError(s.t, err)
	return len(s.blockIDs), nil
}
