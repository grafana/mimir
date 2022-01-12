package gcp

import (
	"context"
	"encoding/binary"
	"encoding/hex"

	"cloud.google.com/go/bigtable"
	"github.com/grafana/mimir/pkg/chunk"
	"github.com/grafana/mimir/pkg/chunk/gcp"
	ot "github.com/opentracing/opentracing-go"
	"github.com/sirupsen/logrus"

	chunkTool "github.com/grafana/mimir/cmd/cortextool/pkg/chunk"
)

const (
	columnFamily = "f"
)

// keysFn returns the row and column keys for the given hash and range keys.
type keysFn func(hashValue string, rangeValue []byte) (rowKey, columnKey string)

// hashPrefix calculates a 64bit hash of the input string and hex-encodes
// the result, taking care to zero pad etc.
func hashPrefix(input string) string {
	prefix := hashAdd(hashNew(), input)
	var encodedUint64 [8]byte
	binary.LittleEndian.PutUint64(encodedUint64[:], prefix)
	var hexEncoded [16]byte
	hex.Encode(hexEncoded[:], encodedUint64[:])
	return string(hexEncoded[:])
}

// storageIndexDeleter implements chunk.IndexDeleter for GCP.
type storageIndexDeleter struct {
	cfg    gcp.Config
	client *bigtable.Client
	keysFn keysFn
}

// NewStorageIndexDeleter returns a new v2 StorageClient.
func NewStorageIndexDeleter(ctx context.Context, cfg gcp.Config) (chunkTool.Deleter, error) {
	client, err := bigtable.NewClient(ctx, cfg.Project, cfg.Instance)
	if err != nil {
		return nil, err
	}
	return newstorageIndexDeleter(cfg, client), nil
}

func newstorageIndexDeleter(cfg gcp.Config, client *bigtable.Client) *storageIndexDeleter {
	return &storageIndexDeleter{
		cfg:    cfg,
		client: client,
		keysFn: func(hashValue string, rangeValue []byte) (string, string) {

			// We hash the row key and prepend it back to the key for better distribution.
			// We preserve the existing key to make migrations and o11y easier.
			if cfg.DistributeKeys {
				hashValue = hashPrefix(hashValue) + "-" + hashValue
			}

			return hashValue, string(rangeValue)
		},
	}
}

func (s *storageIndexDeleter) DeleteEntry(ctx context.Context, entry chunk.IndexEntry, deleteSeries bool) error {
	sp, ctx := ot.StartSpanFromContext(ctx, "DeleteEntry")
	defer sp.Finish()

	table := s.client.Open(entry.TableName)
	rowKey, columnKey := s.keysFn(entry.HashValue, entry.RangeValue)

	mut := bigtable.NewMutation()
	if deleteSeries {
		mut.DeleteRow()
	} else {
		mut.DeleteCellsInColumn(columnFamily, columnKey)
	}

	err := table.Apply(ctx, rowKey, mut)
	if err != nil {
		return err
	}
	return nil
}

func (s *storageIndexDeleter) DeleteSeries(ctx context.Context, series chunk.IndexQuery) ([]error, error) {
	sp, ctx := ot.StartSpanFromContext(ctx, "DeleteSeries")
	defer sp.Finish()

	table := s.client.Open(series.TableName)
	rowKey, _ := s.keysFn(series.HashValue, []byte{})

	mut := bigtable.NewMutation()
	mut.DeleteRow()

	muts := []*bigtable.Mutation{mut}
	rowKeys := []string{rowKey}

	logrus.Infof("deleting series from bigtable, rowkey: %v, table: %v", rowKey, series.TableName)

	err := table.ReadRows(ctx, bigtable.PrefixRange(rowKey+":"), func(row bigtable.Row) bool {
		mut := bigtable.NewMutation()
		mut.DeleteRow()
		rowKeys = append(rowKeys, row.Key())
		logrus.Infof("deleting series from bigtable, rowkey: %v, table: %v", row.Key(), series.TableName)
		return true
	})

	if err != nil {
		return nil, err
	}

	return table.ApplyBulk(ctx, rowKeys, muts)
}
