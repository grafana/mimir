package storegateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/regexp"
	"github.com/oklog/ulid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/filesystem"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/storegateway/indexcache"
	"github.com/grafana/mimir/pkg/storegateway/indexheader"
	"github.com/grafana/mimir/pkg/util/pool"
	"github.com/grafana/mimir/tools/query-step-alignment-analysis/query_stat"
)

const (
	bucketLocation      = "/users/dimitar/proba/postings-shortcut/thanos-bucket"
	indexHeaderLocation = "/users/dimitar/proba/postings-shortcut/local"
	queriesDump         = "/users/dimitar/proba/postings-shortcut/ops-21-mar-2023-query-dump.json"
	resultsLocation     = "/users/dimitar/proba/postings-shortcut/results.txt"
	tenantID            = "10428"
	concurrency         = 8
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

type stats struct {
	fetchedRegularPostings, fetchedShortcutPostings *atomic.Uint64
}

func (s stats) String() string {
	return fmt.Sprintf("regular %d shortcut %d", s.fetchedRegularPostings.Load(), s.fetchedShortcutPostings.Load())
}

func (s stats) reset() {
	s.fetchedRegularPostings.Store(0)
	s.fetchedShortcutPostings.Store(0)
}

func RunPostingsSimulator() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go listenForSignals(ctx, cancel)

	go func() {
		// expose pprof
		fmt.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	logger := log.NewLogfmtLogger(os.Stdout)
	reg := prometheus.NewRegistry()

	block := setupBlock(ctx, logger, reg)
	defer block.Close()

	indexReader := block.indexReader()
	defer indexReader.Close()

	queriesFile, err := os.OpenFile(queriesDump, os.O_RDONLY, 0)
	noErr(err)
	defer queriesFile.Close()

	resultsFile, err := os.OpenFile(resultsLocation, os.O_TRUNC|os.O_WRONLY|os.O_CREATE, 0660)
	noErr(err)
	defer resultsFile.Close()

	queriesChan := make(chan query_stat.QueryStat)
	defer close(queriesChan)
	go processQueries(queriesChan, indexReader, io.MultiWriter(resultsFile, os.Stdout))

	queryDecoder := json.NewDecoder(queriesFile)

	q := &query_stat.QueryStat{}
	for {
		*q = query_stat.QueryStat{}
		if ctx.Err() != nil {
			break
		}
		err = queryDecoder.Decode(q)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			//fmt.Println("invalid query at offset ", queryDecoder.InputOffset())
			continue
		}

		timeWouldSkipStoreGateways := func(t time.Time) bool {
			return t.After(q.Timestamp.Add(-12 * time.Hour))
		}

		if !q.InstantQueryTime.IsZero() && timeWouldSkipStoreGateways(q.InstantQueryTime) {
			continue // this was an instant query which would have only touched ingesters, skip
		}

		if (!q.Start.IsZero() && timeWouldSkipStoreGateways(q.Start)) &&
			(!q.End.IsZero() && timeWouldSkipStoreGateways(q.End)) {
			continue // this was a range query that doesn't
		}

		queriesChan <- *q
	}
}

func processQueries(queries <-chan query_stat.QueryStat, indexr *bucketIndexReader, resultsDest io.Writer) {
	var (
		i                int
		currentMinute    int64
		statistics       = stats{atomic.NewUint64(0), atomic.NewUint64(0)}
		wg               = &sync.WaitGroup{}
		fannedOutQueries = make(chan query_stat.QueryStat)
		ctx, cancel      = context.WithCancel(context.Background())
	)
	defer cancel()

	for q := range queries {
		if q.Timestamp.UnixNano()/int64(time.Minute) != currentMinute {
			close(fannedOutQueries)
			wg.Wait()
			fmt.Fprintln(resultsDest, "at ", i, q.Timestamp.UTC().String(), statistics)
			statistics.reset()

			wg.Add(concurrency)
			fannedOutQueries = make(chan query_stat.QueryStat)
			for i := 0; i < concurrency; i++ {
				go processQueriesSingle(ctx, wg, fannedOutQueries, indexr, statistics)
			}
			currentMinute = q.Timestamp.UnixNano() / int64(time.Minute)
		}

		fannedOutQueries <- q
		i++
	}
}

func processQueriesSingle(ctx context.Context, wg *sync.WaitGroup, fannedOutQueries <-chan query_stat.QueryStat, indexr *bucketIndexReader, statistics stats) {
	defer wg.Done()
	for q := range fannedOutQueries {
		vectorSelectors := parseQuery(q)
		for _, selector := range vectorSelectors {
			matchers := selector.LabelMatchers
			if selector.Name != "" {
				matchers = append(matchers, parser.MustLabelMatcher(labels.MatchEqual, labels.MetricName, selector.Name))
			}
			_, _, postingsStats := postings(ctx, matchers, indexr)
			statistics.fetchedRegularPostings.Add(uint64(postingsStats.postingsTouchedSizeSum))
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

func postings(ctx context.Context, matchers []*labels.Matcher, indexr *bucketIndexReader) (expandedPostings []storage.SeriesRef, expandedPostingsWithShortcut []storage.SeriesRef, stats *queryStats) {
	safeStats := newSafeQueryStats()
	expanded, err := indexr.expandedPostings(ctx, matchers, safeStats)
	noErr(err)
	return expanded, nil, safeStats.export()
}

func parseQuery(q query_stat.QueryStat) []*parser.VectorSelector {
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
		}
		return nil
	})

	return selectors
}

func setupBlock(ctx context.Context, logger log.Logger, reg *prometheus.Registry) *bucketBlock {
	completeBucket, err := filesystem.NewBucket(bucketLocation)
	noErr(err)

	userBucket := objstore.NewPrefixedBucket(completeBucket, tenantID)
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

	indexCache, err := indexcache.NewInMemoryIndexCacheWithConfig(logger, reg, indexcache.InMemoryIndexCacheConfig{
		MaxSize:     1024 * 1024 * 1024,
		MaxItemSize: 125 * 1024 * 1024,
	})
	noErr(err)
	block, err := newBucketBlock(
		ctx,
		tenantID,
		logger,
		NewBucketStoreMetrics(reg),
		blockMetas[blockULID],
		userBucket,
		indexHeaderLocation+"/"+blockULID.String(),
		indexCache,
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
