// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/grafana/cortex-tools/blob/main/pkg/commands/remote_read.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package commands

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
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
	chunkDigest   bool
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

	dumpCmd.Flag("chunk-digest", "Print chunk metadata (min time, max time, SHA256 checksum) instead of decoding samples when using chunked responses. Can only be combined with --use-chunks").
		Default("false").
		BoolVar(&c.chunkDigest)
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

	if c.chunkDigest && !c.useChunks {
		return nil, time.Time{}, time.Time{}, fmt.Errorf("-chunk-digest only works with -use-chunks")
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

// executeMultipleQueries sends multiple queries in a single protobuf request
func (c *RemoteReadCommand) executeMultipleQueries(ctx context.Context, readClient remote.ReadClient, queries []*prompb.Query) (storage.SeriesSet, error) {
	// We'll use reflection to access the private fields of the client to send a batched request
	// This is hacky but gets the job done for now
	client, ok := readClient.(*remote.Client)
	if !ok {
		return nil, fmt.Errorf("unexpected readClient type: %T", readClient)
	}

	// Build the batched request with user-selected response type preference
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

	// Marshal the batched request
	data, err := proto.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("unable to marshal read request: %w", err)
	}

	compressed := snappy.Encode(nil, data)

	// Use reflection to get the URL from the client
	clientValue := reflect.ValueOf(client).Elem()
	urlStringField := clientValue.FieldByName("urlString")
	if !urlStringField.IsValid() {
		return nil, fmt.Errorf("unable to access urlString field")
	}
	urlString := urlStringField.String()

	// Create HTTP request
	httpReq, err := http.NewRequest(http.MethodPost, urlString, bytes.NewReader(compressed))
	if err != nil {
		return nil, fmt.Errorf("unable to create request: %w", err)
	}
	httpReq.Header.Add("Content-Encoding", "snappy")
	httpReq.Header.Add("Accept-Encoding", "snappy")
	httpReq.Header.Set("Content-Type", "application/x-protobuf")
	httpReq.Header.Set("User-Agent", "mimirtool")
	httpReq.Header.Set("X-Prometheus-Remote-Read-Version", "0.1.0")

	// Send the request using the client's HTTP client
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
	log.Debugf("Response headers %v", httpResp.Header)

	// Handle different response types
	switch {
	case strings.HasPrefix(contentType, "application/x-streamed-protobuf; proto=prometheus.ChunkedReadResponse"):
		log.Debugf("Processing chunked streaming response")
		return c.handleChunkedResponse(httpResp, queries)
	case strings.HasPrefix(contentType, "application/x-protobuf"):
		log.Debugf("Processing sampled response")
		return c.handleSampledResponse(httpResp, queries)
	default:
		return nil, fmt.Errorf("unsupported content type: %s", contentType)
	}
}

// concatenatedSeriesSet implements storage.SeriesSet for multiple series
type concatenatedSeriesSet struct {
	series []storage.Series
	index  int
	err    error
}

func (c *concatenatedSeriesSet) Next() bool {
	c.index++
	return c.index < len(c.series)
}

func (c *concatenatedSeriesSet) At() storage.Series {
	if c.index < 0 || c.index >= len(c.series) {
		return nil
	}
	return c.series[c.index]
}

func (c *concatenatedSeriesSet) Err() error {
	return c.err
}

func (c *concatenatedSeriesSet) Warnings() annotations.Annotations {
	return nil
}

// handleSampledResponse handles the traditional sampled response format
func (c *RemoteReadCommand) handleSampledResponse(httpResp *http.Response, queries []*prompb.Query) (storage.SeriesSet, error) {
	// Read and decompress response
	compressedResp, err := io.ReadAll(httpResp.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading response: %w", err)
	}

	uncompressed, err := snappy.Decode(nil, compressedResp)
	if err != nil {
		return nil, fmt.Errorf("error decompressing response: %w", err)
	}

	// Unmarshal response
	var resp prompb.ReadResponse
	err = proto.Unmarshal(uncompressed, &resp)
	if err != nil {
		return nil, fmt.Errorf("unable to unmarshal response body: %w", err)
	}

	if len(resp.Results) != len(queries) {
		return nil, fmt.Errorf("responses: want %d, got %d", len(queries), len(resp.Results))
	}

	// Combine all results from all queries
	var allSeries []storage.Series
	for i, result := range resp.Results {
		log.Infof("Processing result %d/%d with %d series", i+1, len(resp.Results), len(result.Timeseries))
		seriesSet := remote.FromQueryResult(false, result)
		for seriesSet.Next() {
			allSeries = append(allSeries, seriesSet.At())
		}
		if err := seriesSet.Err(); err != nil {
			return nil, fmt.Errorf("error reading series from query %d: %w", i, err)
		}
	}

	log.Infof("Combined %d series from %d queries", len(allSeries), len(queries))
	return &concatenatedSeriesSet{series: allSeries, index: -1}, nil
}

// handleChunkedResponse handles the streamed chunked response format
func (c *RemoteReadCommand) handleChunkedResponse(httpResp *http.Response, queries []*prompb.Query) (storage.SeriesSet, error) {
	// Use the chunked reader from the remote package
	reader := remote.NewChunkedReader(httpResp.Body, c.readSizeLimit, nil)

	// Collect all series from all queries
	var allSeries []storage.Series
	processedQueries := make(map[int64]int)
	totalBytes := 0

	for {
		var chunkedResp prompb.ChunkedReadResponse
		err := reader.NextProto(&chunkedResp)
		if errors.Is(err, io.EOF) {
			break
		}
		totalBytes += chunkedResp.Size()
		if err != nil {
			return nil, fmt.Errorf("error reading chunked response: %w", err)
		}

		queryIndex := chunkedResp.QueryIndex
		if queryIndex < 0 || queryIndex >= int64(len(queries)) {
			return nil, fmt.Errorf("invalid query index %d, expected 0-%d", queryIndex, len(queries)-1)
		}

		processedQueries[queryIndex]++
		if processedQueries[queryIndex] == 1 {
			log.Infof("Processing chunked response for query %d", queryIndex)
		}

		// Each ChunkedSeries contains chunks for a single series
		for _, chunkSeries := range chunkedResp.ChunkedSeries {
			builder := labels.NewScratchBuilder(len(chunkSeries.Labels))
			for _, l := range chunkSeries.Labels {
				builder.Add(l.Name, l.Value)
			}
			series := &multiQueryChunkedSeries{
				labels: builder.Labels(),
				chunks: chunkSeries.Chunks,
			}
			allSeries = append(allSeries, series)
		}
	}

	log.Infof("Combined %d series from %d queries using chunked streaming (%d bytes)", len(allSeries), len(queries), totalBytes)
	return &concatenatedSeriesSet{series: allSeries, index: -1}, nil
}

// multiQueryChunkedSeries implements storage.Series for chunked data from multiple queries
type multiQueryChunkedSeries struct {
	labels labels.Labels
	chunks []prompb.Chunk
}

func (s *multiQueryChunkedSeries) Labels() labels.Labels {
	return s.labels
}

func (s *multiQueryChunkedSeries) Iterator(it chunkenc.Iterator) chunkenc.Iterator {
	// Create a new chunked series iterator
	return newMultiQueryChunkedIterator(s.chunks)
}

// newMultiQueryChunkedIterator creates an iterator for chunked series data
func newMultiQueryChunkedIterator(chunks []prompb.Chunk) chunkenc.Iterator {
	return &multiQueryChunkedIterator{
		chunks:   chunks,
		chunkIdx: 0,
	}
}

// multiQueryChunkedIterator implements an iterator for chunked data
type multiQueryChunkedIterator struct {
	chunks   []prompb.Chunk
	chunkIdx int
	cur      chunkenc.Iterator
	err      error
}

func (it *multiQueryChunkedIterator) Next() chunkenc.ValueType {
	// If we have a current chunk iterator, try to get next value
	if it.cur != nil {
		if vt := it.cur.Next(); vt != chunkenc.ValNone {
			return vt
		}
		// Current chunk is exhausted, move to next
		it.chunkIdx++
	}

	// Find next non-empty chunk
	for it.chunkIdx < len(it.chunks) {
		chunk := it.chunks[it.chunkIdx]
		// Convert protobuf chunk to storage chunk
		c, err := chunkenc.FromData(chunkenc.Encoding(chunk.Type), chunk.Data)
		if err != nil {
			it.err = fmt.Errorf("error decoding chunk %d: %w", it.chunkIdx, err)
			return chunkenc.ValNone
		}
		it.cur = c.Iterator(nil)
		if vt := it.cur.Next(); vt != chunkenc.ValNone {
			return vt
		}
		// This chunk was empty, try next
		it.chunkIdx++
	}

	// No more chunks
	return chunkenc.ValNone
}

func (it *multiQueryChunkedIterator) At() (int64, float64) {
	if it.cur == nil {
		return 0, 0
	}
	return it.cur.At()
}

func (it *multiQueryChunkedIterator) AtHistogram(h *histogram.Histogram) (int64, *histogram.Histogram) {
	if it.cur == nil {
		return 0, nil
	}
	return it.cur.AtHistogram(h)
}

func (it *multiQueryChunkedIterator) AtFloatHistogram(fh *histogram.FloatHistogram) (int64, *histogram.FloatHistogram) {
	if it.cur == nil {
		return 0, nil
	}
	return it.cur.AtFloatHistogram(fh)
}

func (it *multiQueryChunkedIterator) AtT() int64 {
	if it.cur == nil {
		return 0
	}
	return it.cur.AtT()
}

func (it *multiQueryChunkedIterator) Seek(t int64) chunkenc.ValueType {
	// Reset to beginning and iterate until we find t
	it.chunkIdx = 0
	it.cur = nil
	for {
		vt := it.Next()
		if vt == chunkenc.ValNone {
			return chunkenc.ValNone
		}
		if it.AtT() >= t {
			return vt
		}
	}
}

func (it *multiQueryChunkedIterator) Err() error {
	return it.err
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

	if c.chunkDigest {
		return c.dumpChunkDigest(timeseries)
	}

	return c.dumpSamples(timeseries)
}

func (c *RemoteReadCommand) dumpSamples(timeseries storage.SeriesSet) error {
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

func (c *RemoteReadCommand) dumpChunkDigest(timeseries storage.SeriesSet) error {
	for timeseries.Next() {
		s := timeseries.At()
		labels := s.Labels()

		// Check if this is a chunked series (from streaming response)
		chunkedSeries, ok := s.(*multiQueryChunkedSeries)
		if !ok {
			return fmt.Errorf("unexpected series type %T; expected *multiQueryChunkedSeries", s)
		}
		// Process chunks directly
		for i, chunk := range chunkedSeries.chunks {
			minTime := chunk.MinTimeMs
			maxTime := chunk.MaxTimeMs

			hash := sha256.Sum256(chunk.Data)
			checksum := fmt.Sprintf("%x", hash)

			fmt.Printf("%s chunk_%d min_time=%d max_time=%d checksum=%s\n",
				labels.String(), i, minTime, maxTime, checksum)
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
