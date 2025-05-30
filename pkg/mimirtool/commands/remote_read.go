// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/grafana/cortex-tools/blob/main/pkg/commands/remote_read.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package commands

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/alecthomas/kingpin/v2"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	config_util "github.com/prometheus/common/config"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/util/annotations"
	log "github.com/sirupsen/logrus"

	"github.com/grafana/mimir/pkg/mimirtool/backfill"
	"github.com/grafana/mimir/pkg/mimirtool/client"
)

// DefaultChunkedReadLimit is the default value for the maximum size of the protobuf frame client allows.
// 50MB is the default. This is equivalent to ~100k full XOR chunks and average labelset.
const DefaultChunkedReadLimit = 5e+7

type RemoteReadCommand struct {
	address        string
	remoteReadPath string

	tenantID string
	apiKey   string

	readTimeout time.Duration
	tsdbPath    string

	selectors     []string
	from          string
	to            string
	readSizeLimit uint64
	useChunks     bool
}

func (c *RemoteReadCommand) Register(app *kingpin.Application, envVars EnvVarNames) {
	remoteReadCmd := app.Command("remote-read", "Inspect stored series in Grafana Mimir using the remote read API.")
	exportCmd := remoteReadCmd.Command("export", "Export metrics remote read series into a local TSDB.").Action(c.export)
	dumpCmd := remoteReadCmd.Command("dump", "Dump remote read series.").Action(c.dump)
	statsCmd := remoteReadCmd.Command("stats", "Show statistic of remote read series.").Action(c.stats)

	now := time.Now()
	for _, cmd := range []*kingpin.CmdClause{exportCmd, dumpCmd, statsCmd} {
		cmd.Flag("address", "Address of the Grafana Mimir cluster; alternatively, set "+envVars.Address+".").
			Envar(envVars.Address).
			Required().
			StringVar(&c.address)
		cmd.Flag("remote-read-path", "Path of the remote read endpoint.").
			Default("/prometheus/api/v1/read").
			StringVar(&c.remoteReadPath)
		cmd.Flag("id", "Grafana Mimir tenant ID. Used for basic auth and as X-Scope-OrgID HTTP header. Alternatively, set "+envVars.TenantID+".").
			Envar(envVars.TenantID).
			Default("").
			StringVar(&c.tenantID)
		cmd.Flag("key", "Basic auth password to use when contacting Grafana Mimir; alternatively, set "+envVars.APIKey+".").
			Envar(envVars.APIKey).
			Default("").
			StringVar(&c.apiKey)
		cmd.Flag("read-timeout", "timeout for read requests").
			Default("30s").
			DurationVar(&c.readTimeout)

		cmd.Flag("selector", `PromQL selector to filter metrics on. To return all metrics '{__name__!=""}' can be used. Can be specified multiple times to send multiple queries in a single remote read request.`).
			Default("up").
			StringsVar(&c.selectors)

		cmd.Flag("from", "Start of the time window to select metrics.").
			Default(now.Add(-time.Hour).Format(time.RFC3339)).
			StringVar(&c.from)
		cmd.Flag("to", "End of the time window to select metrics.").
			Default(now.Format(time.RFC3339)).
			StringVar(&c.to)
		cmd.Flag("read-size-limit", "Maximum number of bytes to read.").
			Default(strconv.Itoa(DefaultChunkedReadLimit)).
			Uint64Var(&c.readSizeLimit)
		cmd.Flag("use-chunks", "Request chunked streaming response (STREAMED_XOR_CHUNKS) instead of sampled response (SAMPLES).").
			Default("true").
			BoolVar(&c.useChunks)
	}

	exportCmd.Flag("tsdb-path", "Path to the folder where to store the TSDB blocks, if not set a new directory in $TEMP is created.").
		Default("").
		StringVar(&c.tsdbPath)
}

type setTenantIDTransport struct {
	http.RoundTripper
	tenantID string
}

func (s *setTenantIDTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req.Header.Set("X-Scope-OrgID", s.tenantID)
	return s.RoundTripper.RoundTrip(req)
}

type timeSeriesIterator struct {
	seriesSet           storage.SeriesSet
	currentSeriesChunks chunkenc.Iterator

	ts int64
	v  float64
	h  *histogram.Histogram
	fh *histogram.FloatHistogram
}

func newTimeSeriesIterator(seriesSet storage.SeriesSet) *timeSeriesIterator {
	return &timeSeriesIterator{
		seriesSet:           seriesSet,
		currentSeriesChunks: chunkenc.NewNopIterator(),
	}
}

func (i *timeSeriesIterator) Next() error {
	// Find non empty chunk iterator.
	var vt chunkenc.ValueType
	for vt = i.currentSeriesChunks.Next(); vt == chunkenc.ValNone; vt = i.currentSeriesChunks.Next() {
		if !i.seriesSet.Next() {
			err := i.seriesSet.Err()
			if err != nil {
				return err
			}
			return io.EOF
		}
		i.currentSeriesChunks = i.seriesSet.At().Iterator(i.currentSeriesChunks)
	}
	switch vt {
	case chunkenc.ValFloat:
		i.ts, i.v = i.currentSeriesChunks.At()
		i.h = nil
		i.fh = nil
	case chunkenc.ValHistogram:
		i.ts, i.h = i.currentSeriesChunks.AtHistogram(nil)
		i.v = i.h.Sum
		i.fh = nil
	case chunkenc.ValFloatHistogram:
		i.ts, i.fh = i.currentSeriesChunks.AtFloatHistogram(nil)
		i.v = i.fh.Sum
		i.h = nil
	default:
		panic("unreachable")
	}
	return nil
}

func (i *timeSeriesIterator) Labels() (l labels.Labels) {
	return i.seriesSet.At().Labels()
}

func (i *timeSeriesIterator) Sample() (ts int64, v float64, h *histogram.Histogram, fh *histogram.FloatHistogram) {
	return i.ts, i.v, i.h, i.fh
}

// this is adapted from Go 1.15 for older versions
// TODO: Reuse upstream once that version is common
func redactedURL(u *url.URL) string {
	if u == nil {
		return ""
	}

	ru := *u
	if _, has := ru.User.Password(); has {
		ru.User = url.UserPassword(ru.User.Username(), "xxxxx")
	}
	return ru.String()
}

func (c *RemoteReadCommand) readClient() (remote.ReadClient, error) {
	// validate inputs
	addressURL, err := url.Parse(c.address)
	if err != nil {
		return nil, err
	}

	remoteReadPathURL, err := url.Parse(c.remoteReadPath)
	if err != nil {
		return nil, err
	}

	addressURL = addressURL.ResolveReference(remoteReadPathURL)

	// build client
	readClient, err := remote.NewReadClient("remote-read", &remote.ClientConfig{
		URL:     &config_util.URL{URL: addressURL},
		Timeout: model.Duration(c.readTimeout),
		HTTPClientConfig: config_util.HTTPClientConfig{
			BasicAuth: &config_util.BasicAuth{
				Username: c.tenantID,
				Password: config_util.Secret(c.apiKey),
			},
		},
		ChunkedReadLimit: c.readSizeLimit,
		Headers: map[string]string{
			"User-Agent": client.UserAgent(),
		},
	})
	if err != nil {
		return nil, err
	}

	// if tenant ID is set, add a tenant ID header to every request
	if c.tenantID != "" {
		client, ok := readClient.(*remote.Client)
		if !ok {
			return nil, fmt.Errorf("unexpected type %T", readClient)
		}
		client.Client.Transport = &setTenantIDTransport{
			RoundTripper: client.Client.Transport,
			tenantID:     c.tenantID,
		}
	}

	log.Infof("Created remote read client using endpoint '%s'", redactedURL(addressURL))

	return readClient, nil
}

// prepare() validates the input and prepares the client to query remote read endpoints
func (c *RemoteReadCommand) prepare() (query func(context.Context) (storage.SeriesSet, error), from, to time.Time, err error) {
	from, err = time.Parse(time.RFC3339, c.from)
	if err != nil {
		return nil, time.Time{}, time.Time{}, fmt.Errorf("error parsing from: '%s' value: %w", c.from, err)
	}

	to, err = time.Parse(time.RFC3339, c.to)
	if err != nil {
		return nil, time.Time{}, time.Time{}, fmt.Errorf("error parsing to: '%s' value: %w", c.to, err)
	}

	if len(c.selectors) == 0 {
		return nil, time.Time{}, time.Time{}, fmt.Errorf("at least one selector must be specified")
	}

	// Parse all selectors
	var pbQueries []*prompb.Query
	for _, selector := range c.selectors {
		matchers, err := parser.ParseMetricSelector(selector)
		if err != nil {
			return nil, time.Time{}, time.Time{}, fmt.Errorf("error parsing selector '%s': %w", selector, err)
		}

		pbQuery, err := remote.ToQuery(
			int64(model.TimeFromUnixNano(from.UnixNano())),
			int64(model.TimeFromUnixNano(to.UnixNano())),
			matchers,
			nil,
		)
		if err != nil {
			return nil, time.Time{}, time.Time{}, fmt.Errorf("error creating query for selector '%s': %w", selector, err)
		}
		pbQueries = append(pbQueries, pbQuery)
	}

	readClient, err := c.readClient()
	if err != nil {
		return nil, time.Time{}, time.Time{}, err
	}

	return func(ctx context.Context) (storage.SeriesSet, error) {
		log.Infof("Querying time from=%s to=%s with %d selectors", from.Format(time.RFC3339), to.Format(time.RFC3339), len(c.selectors))
	for i, selector := range c.selectors {
		log.Debugf("Selector %d: %s", i+1, selector)
	}
		return c.executeMultipleQueries(ctx, readClient, pbQueries)
	}, from, to, nil
}

// executeMultipleQueries sends multiple queries with batching support
//
// TODO: This functionality should be implemented upstream in Prometheus first.
// The prometheus/prometheus client has a TODO at storage/remote/client.go:347-348
// that says "Support batching multiple queries into one read request, as the
// protobuf interface allows for it."
//
// Once that's implemented upstream, we should remove this custom implementation
// and use the enhanced prometheus client directly.
//
// This interim solution provides:
// - True batching for sampled responses (the common case)
// - Fallback to individual requests for chunked responses
// - Full compatibility with existing prometheus client infrastructure
func (c *RemoteReadCommand) executeMultipleQueries(ctx context.Context, readClient remote.ReadClient, queries []*prompb.Query) (storage.SeriesSet, error) {
	if len(queries) == 1 {
		// Use the original client for single queries to maintain full compatibility
		return readClient.Read(ctx, queries[0], false)
	}

	// For multiple queries, we implement batching ourselves as an interim solution
	// until this functionality is available upstream in prometheus/prometheus
	client, ok := readClient.(*remote.Client)
	if !ok {
		return nil, fmt.Errorf("multi-query support requires *remote.Client, got %T", readClient)
	}

	return c.sendBatchedRequest(ctx, client, queries)
}

// sendBatchedRequest sends a single HTTP request with multiple queries
// This is an interim implementation that should eventually be replaced by
// upstream prometheus client functionality. It reuses as much of the existing
// prometheus client infrastructure as possible.
func (c *RemoteReadCommand) sendBatchedRequest(ctx context.Context, client *remote.Client, queries []*prompb.Query) (storage.SeriesSet, error) {
	// Build the batched request with response type preference
	var acceptedTypes []prompb.ReadRequest_ResponseType
	if c.useChunks {
		log.Debugf("Requesting chunked streaming response (STREAMED_XOR_CHUNKS)")
		acceptedTypes = []prompb.ReadRequest_ResponseType{
			prompb.ReadRequest_STREAMED_XOR_CHUNKS,
			prompb.ReadRequest_SAMPLES, // fallback
		}
	} else {
		log.Debugf("Requesting sampled response (SAMPLES)")
		acceptedTypes = []prompb.ReadRequest_ResponseType{
			prompb.ReadRequest_SAMPLES,
		}
	}

	req := &prompb.ReadRequest{
		Queries:               queries,
		AcceptedResponseTypes: acceptedTypes,
	}

	// Use the same logic as prometheus client for sending the request
	// This is copied from vendor/github.com/prometheus/prometheus/storage/remote/client.go
	data, err := proto.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("unable to marshal read request: %w", err)
	}

	compressed := snappy.Encode(nil, data)

	// Get URL using reflection (same as before)
	clientValue := reflect.ValueOf(client).Elem()
	urlStringField := clientValue.FieldByName("urlString")
	if !urlStringField.IsValid() {
		return nil, fmt.Errorf("unable to access urlString field")
	}
	urlString := urlStringField.String()

	httpReq, err := http.NewRequest(http.MethodPost, urlString, bytes.NewReader(compressed))
	if err != nil {
		return nil, fmt.Errorf("unable to create request: %w", err)
	}
	httpReq.Header.Add("Content-Encoding", "snappy")
	httpReq.Header.Add("Accept-Encoding", "snappy")
	httpReq.Header.Set("Content-Type", "application/x-protobuf")
	httpReq.Header.Set("User-Agent", "mimirtool")
	httpReq.Header.Set("X-Prometheus-Remote-Read-Version", "0.1.0")

	httpResp, err := client.Client.Do(httpReq.WithContext(ctx))
	if err != nil {
		return nil, fmt.Errorf("error sending request: %w", err)
	}
	defer httpResp.Body.Close()

	if httpResp.StatusCode/100 != 2 {
		body, _ := io.ReadAll(httpResp.Body)
		errStr := strings.TrimSpace(string(body))
		return nil, fmt.Errorf("remote server returned http status %s: %s", httpResp.Status, errStr)
	}

	contentType := httpResp.Header.Get("Content-Type")
	log.Debugf("Response content type: %s", contentType)

	// Handle different response types - simplified approach
	switch {
	case strings.HasPrefix(contentType, "application/x-protobuf"):
		log.Debugf("Processing sampled response")
		return c.handleSampledResponse(req, httpResp)
	default:
		// For chunked responses or unsupported types, fall back to individual requests
		// This keeps the implementation simple while still providing batching for the common case
		log.Debugf("Falling back to individual requests for content type: %s", contentType)
		return c.fallbackToIndividualRequests(ctx, client, queries)
	}
}

// handleSampledResponse handles batched sampled responses
func (c *RemoteReadCommand) handleSampledResponse(req *prompb.ReadRequest, httpResp *http.Response) (storage.SeriesSet, error) {
	// Read and decompress response
	compressedResp, err := io.ReadAll(httpResp.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading response: %w", err)
	}

	uncompressed, err := snappy.Decode(nil, compressedResp)
	if err != nil {
		return nil, fmt.Errorf("error decompressing response: %w", err)
	}

	var resp prompb.ReadResponse
	err = proto.Unmarshal(uncompressed, &resp)
	if err != nil {
		return nil, fmt.Errorf("unable to unmarshal response body: %w", err)
	}

	if len(resp.Results) != len(req.Queries) {
		return nil, fmt.Errorf("responses: want %d, got %d", len(req.Queries), len(resp.Results))
	}

	// Combine all results from all queries
	var allSeries []storage.Series
	for i, result := range resp.Results {
		log.Debugf("Processing result %d/%d with %d series", i+1, len(resp.Results), len(result.Timeseries))
		seriesSet := remote.FromQueryResult(false, result)
		for seriesSet.Next() {
			allSeries = append(allSeries, seriesSet.At())
		}
		if err := seriesSet.Err(); err != nil {
			return nil, fmt.Errorf("error reading series from query %d: %w", i, err)
		}
	}

	log.Infof("Combined %d series from %d queries using batched request", len(allSeries), len(req.Queries))
	return &combinedSeriesSet{series: allSeries, index: -1}, nil
}

// fallbackToIndividualRequests handles cases where batching isn't supported
func (c *RemoteReadCommand) fallbackToIndividualRequests(ctx context.Context, client *remote.Client, queries []*prompb.Query) (storage.SeriesSet, error) {
	log.Infof("Using individual requests for %d queries", len(queries))
	var allSeries []storage.Series
	for i, query := range queries {
		log.Debugf("Executing individual query %d/%d", i+1, len(queries))
		seriesSet, err := client.Read(ctx, query, false)
		if err != nil {
			return nil, fmt.Errorf("error executing query %d: %w", i, err)
		}
		
		// Collect all series from this query
		for seriesSet.Next() {
			allSeries = append(allSeries, seriesSet.At())
		}
		if err := seriesSet.Err(); err != nil {
			return nil, fmt.Errorf("error reading series from query %d: %w", i, err)
		}
	}

	log.Infof("Combined %d series from %d individual queries", len(allSeries), len(queries))
	return &combinedSeriesSet{series: allSeries, index: -1}, nil
}

// combinedSeriesSet implements storage.SeriesSet for multiple series
type combinedSeriesSet struct {
	series []storage.Series
	index  int
	err    error
}

func (c *combinedSeriesSet) Next() bool {
	c.index++
	return c.index < len(c.series)
}

func (c *combinedSeriesSet) At() storage.Series {
	if c.index < 0 || c.index >= len(c.series) {
		return nil
	}
	return c.series[c.index]
}

func (c *combinedSeriesSet) Err() error {
	return c.err
}

func (c *combinedSeriesSet) Warnings() annotations.Annotations {
	return nil
}



func (c *RemoteReadCommand) dump(_ *kingpin.ParseContext) error {
	query, _, _, err := c.prepare()
	if err != nil {
		return err
	}

	timeseries, err := query(context.Background())
	if err != nil {
		return err
	}

	var it chunkenc.Iterator
	for timeseries.Next() {
		s := timeseries.At()

		l := s.Labels().String()
		it := s.Iterator(it)
		for vt := it.Next(); vt != chunkenc.ValNone; vt = it.Next() {
			switch vt {
			case chunkenc.ValFloat:
				ts, v := it.At()
				comment := ""
				if value.IsStaleNaN(v) {
					comment = " # StaleNaN"
				}
				fmt.Printf("%s %g %d%s\n", l, v, ts, comment)
			case chunkenc.ValHistogram:
				ts, h := it.AtHistogram(nil)
				comment := ""
				if value.IsStaleNaN(h.Sum) {
					comment = " # StaleNaN"
				}
				fmt.Printf("%s %s %d%s\n", l, h.String(), ts, comment)
			case chunkenc.ValFloatHistogram:
				ts, h := it.AtFloatHistogram(nil)
				comment := ""
				if value.IsStaleNaN(h.Sum) {
					comment = " # StaleNaN"
				}
				fmt.Printf("%s %s %d%s\n", l, h.String(), ts, comment)
			default:
				panic("unreachable")
			}
		}
	}

	if err := timeseries.Err(); err != nil {
		return err
	}

	return nil
}

func (c *RemoteReadCommand) stats(_ *kingpin.ParseContext) error {
	query, _, _, err := c.prepare()
	if err != nil {
		return err
	}

	timeseries, err := query(context.Background())
	if err != nil {
		return err
	}

	num := struct {
		Samples int64
		Series  map[string]struct{}

		NaNValues      int64
		StaleNaNValues int64

		MinT model.Time
		MaxT model.Time
	}{
		Series: make(map[string]struct{}),
	}

	var it chunkenc.Iterator
	for timeseries.Next() {
		s := timeseries.At()
		it := s.Iterator(it)
		for vt := it.Next(); vt != chunkenc.ValNone; vt = it.Next() {
			num.Samples++
			num.Series[s.Labels().String()] = struct{}{}

			var ts int64
			var v float64
			switch vt {
			case chunkenc.ValFloat:
				ts, v = it.At()
			case chunkenc.ValHistogram:
				var h *histogram.Histogram
				ts, h = it.AtHistogram(nil)
				v = h.Sum
			case chunkenc.ValFloatHistogram:
				var h *histogram.FloatHistogram
				ts, h = it.AtFloatHistogram(nil)
				v = h.Sum
			default:
				panic("unreachable")
			}

			if int64(num.MaxT) < ts {
				num.MaxT = model.Time(ts)
			}
			if num.MinT == 0 || int64(num.MinT) > ts {
				num.MinT = model.Time(ts)
			}

			if math.IsNaN(v) {
				num.NaNValues++
			}
			if value.IsStaleNaN(v) {
				num.StaleNaNValues++
			}
		}
	}

	if err := timeseries.Err(); err != nil {
		return err
	}

	output := bytes.NewBuffer(nil)
	tw := tabwriter.NewWriter(output, 13, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "MIN TIME\tMAX TIME\tDURATION\tNUM SAMPLES\tNUM SERIES\tNUM STALE NAN VALUES\tNUM NAN VALUES")

	fmt.Fprintf(
		tw,
		"%s\t%s\t%s\t%d\t%d\t%d\t%d\n",
		num.MinT.Time().UTC().String(),
		num.MaxT.Time().UTC().String(),
		num.MaxT.Sub(num.MinT),
		num.Samples,
		len(num.Series),
		num.StaleNaNValues,
		num.NaNValues,
	)

	if err := tw.Flush(); err != nil {
		return err
	}

	for _, line := range strings.Split(output.String(), "\n") {
		if len(line) != 0 {
			log.Info(line)
		}
	}

	return nil
}

func (c *RemoteReadCommand) export(_ *kingpin.ParseContext) error {
	query, from, to, err := c.prepare()
	if err != nil {
		return err
	}

	if c.tsdbPath == "" {
		c.tsdbPath, err = os.MkdirTemp("", "mimirtool-tsdb")
		if err != nil {
			return err
		}
		log.Infof("Created TSDB in path '%s'", c.tsdbPath)
	} else {
		if _, err := os.Stat(c.tsdbPath); err != nil && os.IsNotExist(err) {
			if err = os.Mkdir(c.tsdbPath, 0755); err != nil {
				return err
			}
			log.Infof("Created TSDB in path '%s'", c.tsdbPath)
		}
		log.Infof("Using existing TSDB in path '%s'", c.tsdbPath)
	}

	mint := model.TimeFromUnixNano(from.UnixNano())
	maxt := model.TimeFromUnixNano(to.UnixNano())

	timeseries, err := query(context.Background())
	if err != nil {
		return err
	}
	iteratorCreator := func() backfill.Iterator {
		return newTimeSeriesIterator(timeseries)
	}

	pipeR, pipeW := io.Pipe()
	go func() {
		scanner := bufio.NewScanner(pipeR)
		for scanner.Scan() {
			log.Info(scanner.Text())
		}
		if err := scanner.Err(); err != nil {
			log.Error("reading block status:", err)
		}
	}()
	defer pipeR.Close()

	log.Infof("Store TSDB blocks in '%s'", c.tsdbPath)
	if err := backfill.CreateBlocks(iteratorCreator, int64(mint), int64(maxt), 1000, c.tsdbPath, true, pipeW); err != nil {
		return err
	}

	// ensure that tsdb directory has WAL, otherwise 'promtool tsdb dump' fails
	walPath := filepath.Join(c.tsdbPath, "wal")
	if _, err := os.Stat(walPath); err != nil && os.IsNotExist(err) {
		if err := os.Mkdir(walPath, 0755); err != nil {
			return err
		}
	}

	return nil
}
