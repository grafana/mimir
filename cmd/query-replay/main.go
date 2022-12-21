package main

import (
	"bufio"
	"context"
	"fmt"
	"math/big"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"go.uber.org/atomic"
	"golang.org/x/time/rate"

	"github.com/grafana/mimir/pkg/continuoustest"
)

const (
	queriesPath       = "/Users/dimitar/Documents/proba/goldman-sachs-queries/first-100-range-queries.txt"
	queryEndpoint     = "http://localhost:8080/prometheus"
	tenantID          = "417760"
	concurrency       = 100
	requestsPerSecond = float64(10)
)

var logger = log.NewLogfmtLogger(os.Stdout)

func main() {
	reader, err := os.Open(queriesPath)
	if err != nil {
		panic(errors.Wrap(err, "open queries"))
	}
	defer reader.Close()

	stats := requestStats{
		sentQueries:   atomic.NewInt64(0),
		failedQueries: atomic.NewInt64(0),
	}
	done := &sync.WaitGroup{}
	done.Add(1 + concurrency)

	queriesChan := make(chan queryRequest)
	go produceQueries(reader, queriesChan, stats, done)
	for i := 0; i < concurrency; i++ {
		go sendQueries(queriesChan, stats, done)
	}
	go logStats(stats)

	done.Wait()
	logger.Log("msg", "sent all queries")
	printStats(stats, 0)
}

func logStats(stats requestStats) {
	prevSentQueries := stats.sentQueries.Load()
	for range time.Tick(time.Second) {
		printStats(stats, prevSentQueries)
		prevSentQueries = stats.sentQueries.Load()
	}
}

func printStats(stats requestStats, prevSentQueries int64) {
	sentQueries := stats.sentQueries.Load()
	logger.Log("msg", "stats", "failed_queries", stats.failedQueries.Load(), "sent_queries", sentQueries, "sent_since_last_log", sentQueries-prevSentQueries)
}

func sendQueries(queriesChan chan queryRequest, stats requestStats, done *sync.WaitGroup) {
	defer done.Done()

	endpoint := flagext.URLValue{}
	err := endpoint.Set(queryEndpoint)
	if err != nil {
		panic(err)
	}

	client, err := continuoustest.NewClient(continuoustest.ClientConfig{
		TenantID:          tenantID,
		WriteBaseEndpoint: endpoint,
		ReadBaseEndpoint:  endpoint,
		ReadTimeout:       time.Minute,
	}, logger)

	if err != nil {
		logger.Log("msg", "couldn't create prometheus client", "err", err)
		return
	}

	for r := range queriesChan {
		//logger.Log("msg", "sending query", "query", fmt.Sprintf("%v", r))

		_, err := client.QueryRange(context.Background(), r.query, time.UnixMilli(r.start), time.UnixMilli(r.end), r.step, continuoustest.WithResultsCacheEnabled(false))
		if err != nil {
			logger.Log("msg", "query failed", "err", err, "query", fmt.Sprintf("%v", r))
			stats.failedQueries.Inc()
			continue
		}

		//logger.Log("response", fmt.Sprintf("%v", m.Len()))
	}
}

// parses each new line as space delimited `float64_param_start float64_param_end float64_param_step param_query`
// where param_query is everything after the third space.
func produceQueries(reader *os.File, queriesChan chan queryRequest, stats requestStats, done *sync.WaitGroup) {
	defer done.Done()
	defer close(queriesChan)

	runID := strconv.FormatInt(time.Now().Unix(), 10)
	scanner := bufio.NewScanner(reader)
	rateLimit := rate.NewLimiter(rate.Limit(requestsPerSecond), int(requestsPerSecond))

	for scanner.Scan() {
		queryLine := scanner.Text()
		splitQueryLine := strings.SplitN(queryLine, " ", 4)
		if len(splitQueryLine) != 4 {
			logger.Log("msg", "query line contains unexpected number of fields; skipping", "line", queryLine)
			continue
		}
		start, err := parseScientificUnixMillis(splitQueryLine[0])
		if err != nil {
			logger.Log("msg", "couldn't parse query line; skipping", "err", err, "line", queryLine)
			continue
		}
		end, err := parseScientificUnixMillis(splitQueryLine[1])
		if err != nil {
			logger.Log("msg", "couldn't parse query line; skipping", "err", err, "line", queryLine)
			continue
		}
		step, err := parseStep(splitQueryLine[2])
		if err != nil {
			logger.Log("msg", "couldn't parse query line; skipping", "err", err, "line", queryLine)
			continue
		}

		query := appendNoCacheMatcher(splitQueryLine[3], runID)

		err = rateLimit.Wait(context.Background())
		if err != nil {
			logger.Log("msg", "limiter error", "err", err)
		}

		queriesChan <- queryRequest{
			start: start,
			end:   end,
			step:  step,
			query: query,
		}
		stats.sentQueries.Inc()
	}
}

// This assumes that all queries have the `{}` somewhere in them. This was valid for all of goldman sach's queries.
// Also, this assumes that there is exactly one `}` in the query, which was also valid for goldman sach's queries,
// except for 200 queries ran by mimir-continuous-test
func appendNoCacheMatcher(s string, nonce string) string {
	idx := strings.Index(s, "}")
	if idx == -1 {
		return s
	}
	return fmt.Sprintf(`%s,__no_cache__!="%s"%s`, s[:idx], nonce, s[idx:])
}

// Parses 1.67089685911E9, multiples by 1000, expects that the result it is a whole number
// returns a time with these unix milliseconds
func parseScientificUnixMillis(s string) (int64, error) {
	flt, _, err := big.ParseFloat(s, 10, 0, big.ToNearestEven)
	if err != nil {
		return 0, err
	}
	flt = flt.Mul(flt, big.NewFloat(10))
	i, _ := flt.Int64()
	return i, nil
}

func parseStep(s string) (time.Duration, error) {
	if step, err := strconv.ParseFloat(s, 64); err == nil {
		return time.Second * time.Duration(step), nil
	}

	stepD, err := model.ParseDuration(s)
	if err != nil {
		return 0, err
	}
	return time.Duration(stepD), nil
}

type queryRequest struct {
	start, end int64 // in millis
	step       time.Duration
	query      string
}

type requestStats struct {
	sentQueries   *atomic.Int64
	failedQueries *atomic.Int64
}
