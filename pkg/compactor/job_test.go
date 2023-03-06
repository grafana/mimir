package compactor

import (
	"testing"

	"github.com/oklog/ulid"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/tsdb/metadata"
)

func TestJob_MinCompactionLevel(t *testing.T) {
	job := NewJob("user-1", "group-1", nil, 0, true, 2, "shard-1")
	require.NoError(t, job.AppendMeta(&metadata.Meta{BlockMeta: tsdb.BlockMeta{ULID: ulid.MustNew(1, nil), Compaction: tsdb.BlockMetaCompaction{Level: 2}}}))
	assert.Equal(t, 2, job.MinCompactionLevel())

	require.NoError(t, job.AppendMeta(&metadata.Meta{BlockMeta: tsdb.BlockMeta{ULID: ulid.MustNew(2, nil), Compaction: tsdb.BlockMetaCompaction{Level: 3}}}))
	assert.Equal(t, 2, job.MinCompactionLevel())

	require.NoError(t, job.AppendMeta(&metadata.Meta{BlockMeta: tsdb.BlockMeta{ULID: ulid.MustNew(3, nil), Compaction: tsdb.BlockMetaCompaction{Level: 1}}}))
	assert.Equal(t, 1, job.MinCompactionLevel())
}
