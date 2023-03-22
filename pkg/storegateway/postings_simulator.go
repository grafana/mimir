package storegateway

import (
	"context"
	"os"

	"github.com/go-kit/log"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/filesystem"

	"github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/storegateway/indexheader"
	"github.com/grafana/mimir/pkg/util/pool"
)

const (
	bucketLocation      = "/users/dimitar/proba/postings-shortcut/thanos-bucket"
	indexHeaderLocation = "/users/dimitar/proba/postings-shortcut/local"
	tenantId            = "10428"
)

var (
	blockULID = ulid.MustParse("01GW1P25XTPFDB3FYJWWC4JVV3")
)

func RunPostingsSimulator() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logger := log.NewLogfmtLogger(os.Stdout)
	reg := prometheus.NewRegistry()

	setupBlock(ctx, logger, reg)
}

func setupBlock(ctx context.Context, logger log.Logger, reg *prometheus.Registry) *bucketBlock {
	completeBucket, err := filesystem.NewBucket(bucketLocation)
	noErr(err)

	userBucket := objstore.NewPrefixedBucket(completeBucket, tenantId)
	indexHeaderReader, err := indexheader.NewStreamBinaryReader(
		ctx,
		logger,
		userBucket,
		indexHeaderLocation,
		blockULID,
		tsdb.DefaultPostingOffsetInMemorySampling,
		indexheader.NewStreamBinaryReaderMetrics(reg),
		indexheader.Config{MaxIdleFileHandles: 1},
	)
	noErr(err)
	metaFetcher, err := block.NewMetaFetcher(logger, 1, objstore.WithNoopInstr(userBucket), indexHeaderLocation, reg, nil)
	noErr(err)
	blockMetas, errs, err := metaFetcher.Fetch(ctx)
	noErr(err)
	for _, err = range errs {
		noErr(err)
	}
	block, err := newBucketBlock(
		ctx,
		tenantId,
		logger,
		NewBucketStoreMetrics(reg),
		blockMetas[blockULID],
		userBucket,
		indexHeaderLocation+"/"+blockULID.String(),
		noopCache{},
		pool.NoopBytes{},
		indexHeaderReader,
		newGapBasedPartitioners(tsdb.DefaultPartitionerMaxGapSize, reg),
	)
	noErr(err)
	return block
}

func noErr(err error) {
	if err != nil {
		panic(err)
	}
}
