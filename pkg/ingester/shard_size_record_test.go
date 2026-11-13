// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/common/promslog"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/require"
)

func TestUserTSDB_ObserveShardSize(t *testing.T) {
	dir := t.TempDir()
	open := func() *userTSDB {
		db, err := tsdb.Open(dir, promslog.NewNopLogger(), nil, tsdb.DefaultOptions(), nil)
		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })
		return &userTSDB{userID: "test", db: db}
	}
	t0 := time.UnixMilli(1_000_000)

	u := open()
	require.Zero(t, u.shardSizeUnchangedSince(), "not checked yet")
	u.observeShardSize(3, t0, log.NewNopLogger())
	require.Equal(t, t0.UnixMilli(), u.shardSizeUnchangedSince(), "nothing kept: the shard size counts as changed now")
	u.observeShardSize(3, t0.Add(time.Hour), log.NewNopLogger())
	require.Equal(t, t0.UnixMilli(), u.shardSizeUnchangedSince(), "unchanged")

	t1 := t0.Add(2 * time.Hour)
	u.observeShardSize(4, t1, log.NewNopLogger())
	require.Equal(t, t1.UnixMilli(), u.shardSizeUnchangedSince(), "the shard grew")
	require.NoError(t, u.db.Close())

	// A restart keeps the time when the shard size is unchanged.
	u = open()
	u.observeShardSize(4, t1.Add(time.Hour), log.NewNopLogger())
	require.Equal(t, t1.UnixMilli(), u.shardSizeUnchangedSince(), "kept across the restart")
	require.NoError(t, u.db.Close())

	// The shard grew while the ingester was down.
	u = open()
	t2 := t1.Add(3 * time.Hour)
	u.observeShardSize(5, t2, log.NewNopLogger())
	require.Equal(t, t2.UnixMilli(), u.shardSizeUnchangedSince(), "changed while down: counts as changed at the restart")
	rec, err := readShardSizeRecord(dir)
	require.NoError(t, err)
	require.Equal(t, shardSizeRecord{Version: shardSizeRecordVersion1, ShardSize: 5, SinceMs: t2.UnixMilli()}, rec)
	require.NoError(t, u.db.Close())

	// A file that can't be read counts as nothing kept.
	require.NoError(t, os.WriteFile(filepath.Join(dir, shardSizeRecordFilename), []byte("{"), 0o644))
	u = open()
	t3 := t2.Add(time.Hour)
	u.observeShardSize(5, t3, log.NewNopLogger())
	require.Equal(t, t3.UnixMilli(), u.shardSizeUnchangedSince())
}
