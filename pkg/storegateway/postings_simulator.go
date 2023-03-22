package storegateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"

	"github.com/go-kit/log"
	"github.com/grafana/regexp"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/filesystem"

	"github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/storegateway/indexheader"
	"github.com/grafana/mimir/pkg/util/pool"
	"github.com/grafana/mimir/tools/query-step-alignment-analysis/query_stat"
)

const (
	bucketLocation      = "/users/dimitar/proba/postings-shortcut/thanos-bucket"
	indexHeaderLocation = "/users/dimitar/proba/postings-shortcut/local"
	queriesDump         = "/users/dimitar/proba/postings-shortcut/thanos-bucket/ops-21-mar-2023-query-dump.json"
	tenantId            = "10428"
)

var (
	blockULID = ulid.MustParse("01GW1P25XTPFDB3FYJWWC4JVV3")

	queryPathPrefix  = `/prometheus/api/v1/query`
	labelValuesRegex = regexp.MustCompile(`/prometheus/api/v1/label/(?P<lname>\w+)/values`)
	labelNamesPath   = `/prometheus/api/v1/labels`
	seriesPath       = `/prometheus/api/v1/series`
	remoteReadPath   = `/prometheus/api/v1/read`
	metadataPath     = `/prometheus/api/v1/metadata`
)

func RunPostingsSimulator() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go listenForSignals(ctx, cancel)

	logger := log.NewLogfmtLogger(os.Stdout)
	reg := prometheus.NewRegistry()

	block := setupBlock(ctx, logger, reg)
	defer block.Close()

	indexReader := block.indexReader()
	defer indexReader.Close()

	queriesStream, err := os.OpenFile(queriesDump, os.O_RDONLY, 0)
	noErr(err)
	defer queriesStream.Close()

	queryDecoder := json.NewDecoder(queriesStream)

	q := &query_stat.QueryStat{}
	for i := 0; ; i++ {
		if ctx.Err() != nil {
			break
		}
		fmt.Println("at ", i)
		err = queryDecoder.Decode(q)
		if errors.Is(err, io.EOF) {
			break
		}

		vectorSelectors := parseQuery(q)

		for _, selector := range vectorSelectors {
			//expandedPostings, expandedExtraPostings, postingsStats :=
			postings(ctx, selector, indexReader)

		}
	}
}

func listenForSignals(ctx context.Context, cancel context.CancelFunc) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	select {
	case <-ctx.Done():
		return
	case <-c:
		cancel()
	}
}

type postingStats struct {
	fetchedRegularPostingsBytes  int
	fetchedShortcutPostingsBytes int
}

func postings(ctx context.Context, selector *parser.VectorSelector, indexr *bucketIndexReader) (expandedPostings index.Postings, expandedPostingsWithShortcut index.Postings, stats postingStats) {
	return
}

func parseQuery(q *query_stat.QueryStat) []*parser.VectorSelector {
	switch labelValsSubMatch := labelValuesRegex.FindStringSubmatch(q.RequestPath); {
	case q.RequestPath == metadataPath:
		return nil
	case q.RequestPath == remoteReadPath:
		return nil // this isn't exposed in the query logs, hopefully they aren't too many requests
	case len(labelValsSubMatch) > 0:
		return nil // TODO dimitarvdimitrov implement this too to predict what we can do if we also optimize label values calls
	case strings.HasPrefix(q.RequestPath, queryPathPrefix):
		return parseQueryStr(q.Query)
	case q.RequestPath == labelNamesPath || q.RequestPath == seriesPath:
		if q.Match == "" {
			return nil
		}
		return parseQueryStr(q.Match)
	default:
		panic("cannot classify path " + q.RequestPath + fmt.Sprintf(" %#v", q))
	}
}

func parseQueryStr(q string) []*parser.VectorSelector {
	expr, err := parser.ParseExpr(q)
	if err != nil {
		return nil // some queries will be invalid, so we skip them
	}
	var selectors []*parser.VectorSelector
	parser.Inspect(expr, func(node parser.Node, path []parser.Node) error {
		if n, ok := node.(*parser.VectorSelector); ok {
			selectors = append(selectors, n)
			//fmt.Println(n.String())
		}
		return nil
	})

	return selectors
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
