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
	queriesPath    = "/Users/dimitar/Documents/proba/goldman-sachs-queries/range-queries.txt"
	queryEndpoint1 = "http://localhost:8080/prometheus"
	queryEndpoint2 = "http://localhost:8081/prometheus"
	tenantID       = "417760"

	rateRampUpDuration   = 20 * time.Minute
	rateRampUpInterval   = 10 * time.Second
	concurrency          = 600
	maxRequestsPerSecond = float64(250)
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
		totalSeries:   atomic.NewInt64(0),
	}
	done := &sync.WaitGroup{}
	done.Add(concurrency)

	queriesChan := make(chan queryRequest)
	go produceQueries(reader, queriesChan, stats, done)
	for i := 0; i < concurrency; i++ {
		if i%2 == 0 {
			go sendQueries(queriesChan, queryEndpoint1, stats, done)
		} else {
			go sendQueries(queriesChan, queryEndpoint2, stats, done)
		}
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
	logger.Log("t", time.Now().UTC().String(), "msg", "stats", "failed_queries", stats.failedQueries.Load(), "sent_queries", sentQueries, "sent_since_last_log", sentQueries-prevSentQueries, "total_series", stats.totalSeries.Load())
}

func sendQueries(queriesChan chan queryRequest, queryEndpoint string, stats requestStats, done *sync.WaitGroup) {
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

		m, err := client.QueryRange(context.Background(), r.query, time.UnixMilli(r.start), time.UnixMilli(r.end), r.step, continuoustest.WithResultsCacheEnabled(false))
		if err != nil {
			logger.Log("msg", "query failed", "err", err, "query", fmt.Sprintf("%v", r))
			if stats.failedQueries.Inc() > 1000 {
				return
			}
			continue
		}
		stats.totalSeries.Add(int64(m.Len()))
		_ = m
		//logger.Log("response", fmt.Sprintf("%v", m.Len()))
	}
}

// parses each new line as space delimited `float64_param_start float64_param_end float64_param_step param_query`
// where param_query is everything after the third space.
func produceQueries(reader *os.File, queriesChan chan queryRequest, stats requestStats, done *sync.WaitGroup) {
	defer close(queriesChan)

	runID := strconv.FormatInt(time.Now().Unix(), 10)
	scanner := bufio.NewScanner(reader)
	rateLimit := newRampingUpRateLimiter(rateRampUpDuration, rateRampUpInterval, 1, maxRequestsPerSecond)

	for scanner.Scan() {
		queryLine := scanner.Text()

		r, err := parseQueryRequest(queryLine, runID)
		if err != nil {
			logger.Log("msg", "couldn't parse query line; skipping", "err", err, "line", queryLine)
			continue
		}
		err = rateLimit.Wait()
		if err != nil {
			logger.Log("msg", "limiter error", "err", err)
		}

		queriesChan <- r
		stats.sentQueries.Inc()
	}
}

func parseQueryRequest(queryLine, nonce string) (queryRequest, error) {
	splitQueryLine := strings.SplitN(queryLine, " ", 4)
	if len(splitQueryLine) != 4 {
		return queryRequest{}, errors.New("query line contains unexpected number of fields")
	}
	start, err := parseScientificUnixMillis(splitQueryLine[0])
	if err != nil {
		return queryRequest{}, err
	}
	end, err := parseScientificUnixMillis(splitQueryLine[1])
	if err != nil {
		return queryRequest{}, err
	}
	step, err := parseStep(splitQueryLine[2])
	if err != nil {
		return queryRequest{}, err
	}
	query := appendNoCacheMatcher(splitQueryLine[3], nonce)

	return queryRequest{
		start: start,
		end:   end,
		step:  step,
		query: query,
	}, nil
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
	flt = flt.Mul(flt, big.NewFloat(1000))
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
	totalSeries   *atomic.Int64
}

type rampingUpRateLimiter struct {
	mtx *sync.RWMutex
	l   *rate.Limiter
}

func newRampingUpRateLimiter(rampUpDuration, rampUpInterval time.Duration, startRate, finalRate float64) *rampingUpRateLimiter {
	l := &rampingUpRateLimiter{
		l:   rate.NewLimiter(rate.Limit(startRate), int(startRate)),
		mtx: &sync.RWMutex{},
	}
	go l.rampUp(rampUpDuration, rampUpInterval, startRate, finalRate)
	return l
}

func (l *rampingUpRateLimiter) Wait() error {
	l.mtx.RLock()
	defer l.mtx.RUnlock()
	return l.l.Wait(context.Background())
}

func (l *rampingUpRateLimiter) rampUp(duration, interval time.Duration, startRate, finalRate float64) {
	maxRampUps := int(duration / interval)
	rampUpStep := (finalRate - startRate) / float64(maxRampUps)
	currentRampUps := 0
	currentRate := startRate

	for range time.Tick(interval) {
		currentRate += rampUpStep
		currentRampUps++
		if currentRampUps > maxRampUps {
			return
		}
		logger.Log("msg", "ramping up request rate to", "rate", currentRate)
		l.mtx.Lock()
		l.l = rate.NewLimiter(rate.Limit(currentRate), int(currentRate))
		l.mtx.Unlock()
	}
}
