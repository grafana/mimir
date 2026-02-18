// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/grafana/cortex-tools/blob/main/pkg/commands/remote_read.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.
// Provenance-includes-location: https://github.com/grafana/cortex-tools/blob/main/pkg/backfill/backfill.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/cmd/promtool/tsdb.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package commands

import (
	"bufio"
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/alecthomas/kingpin/v2"
	"github.com/alecthomas/units"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid/v2"
	config_util "github.com/prometheus/common/config"
	"github.com/prometheus/common/model"
	"github.com/prometheus/common/promslog"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"

	"github.com/grafana/mimir/pkg/mimirtool/backfill"
	mimirtool_client "github.com/grafana/mimir/pkg/mimirtool/client"
	"github.com/grafana/mimir/pkg/mimirtool/config"
	"github.com/grafana/mimir/pkg/util"
)

// selectorFlag implements kingpin.Value for parsing metric selectors into label matchers
type selectorFlag struct {
	selectors *[][]*labels.Matcher
}

func (s *selectorFlag) Set(value string) error {
	matchers, err := config.CreateParser().ParseMetricSelector(value)
	if err != nil {
		return fmt.Errorf("error parsing selector '%s': %w", value, err)
	}
	*s.selectors = append(*s.selectors, matchers)
	return nil
}

func (s *selectorFlag) String() string {
	var result []string
	for _, selectorMatchers := range *s.selectors {
		var matcherStrs []string
		for _, matcher := range selectorMatchers {
			matcherStrs = append(matcherStrs, matcher.String())
		}
		result = append(result, "{"+strings.Join(matcherStrs, ",")+"}")
	}
	return strings.Join(result, ",")
}

// DefaultChunkedReadLimit is the default value for the maximum size of the protobuf frame client allows.
// 50MB is the default. This is equivalent to ~100k full XOR chunks and average labelset.
const DefaultChunkedReadLimit = 5e+7

type RemoteReadCommand struct {
	address        string
	remoteReadPath string

	tenantID string
	apiKey   string
	apiUser  string

	readTimeout time.Duration
	tsdbPath    string

	selectors     [][]*labels.Matcher
	from          string
	to            string
	readSizeLimit uint64
	blockDuration time.Duration
	useChunks     bool

	logger log.Logger
}

func (c *RemoteReadCommand) Register(app *kingpin.Application, envVars EnvVarNames, logConfig *LoggerConfig) {
	remoteReadCmd := app.Command("remote-read", "Inspect stored series in Grafana Mimir using the remote read API.").PreAction(func(_ *kingpin.ParseContext) error {
		c.logger = logConfig.Logger()
		return nil
	})
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
		cmd.Flag("id", "Grafana Mimir tenant ID; alternatively, set "+envVars.TenantID+". Used for X-Scope-OrgID HTTP header. Also used for basic auth if --user is not provided.").
			Envar(envVars.TenantID).
			Default("").
			StringVar(&c.tenantID)
		cmd.Flag("user",
			fmt.Sprintf("Basic auth username to use when contacting Grafana Mimir; alternatively, set %s. If empty, %s is used instead.", envVars.APIUser, envVars.TenantID)).
			Envar(envVars.APIUser).
			Default("").
			StringVar(&c.apiUser)
		cmd.Flag("key", "Basic auth password to use when contacting Grafana Mimir; alternatively, set "+envVars.APIKey+".").
			Envar(envVars.APIKey).
			Default("").
			StringVar(&c.apiKey)
		cmd.Flag("read-timeout", "timeout for read requests").
			Default("30s").
			DurationVar(&c.readTimeout)

		flag := cmd.Flag("selector", `PromQL selector to filter metrics on. To return all metrics '{__name__!=""}' can be used. Can be specified multiple times to send multiple queries in a single remote read request.`)
		flag.SetValue(&selectorFlag{selectors: &c.selectors})
		flag.Default("up")

		cmd.Flag("from", "Start of the time window to select metrics (inclusive).").
			Default(now.Add(-time.Hour).Format(time.RFC3339)).
			StringVar(&c.from)
		cmd.Flag("to", "End of the time window to select metrics (inclusive).").
			Default(now.Format(time.RFC3339)).
			StringVar(&c.to)
		cmd.Flag("read-size-limit", "Maximum number of bytes to read.").
			Default(strconv.Itoa(DefaultChunkedReadLimit)).
			Uint64Var(&c.readSizeLimit)
		cmd.Flag("use-chunks", "Request chunked streaming response (STREAMED_XOR_CHUNKS) instead of samples response (SAMPLES).").
			Default("true").
			BoolVar(&c.useChunks)
	}

	exportCmd.Flag("tsdb-path", "Path to the folder where to store the TSDB blocks, if not set a new directory in $TEMP is created.").
		Default("").
		StringVar(&c.tsdbPath)

	exportCmd.Flag("block-duration", "Maximum block duration to create. Requests that span multiple blocks will be split, and all blocks will be aligned to the Unix epoch.").
		Default((time.Duration(tsdb.DefaultBlockDuration) * time.Millisecond).String()).
		DurationVar(&c.blockDuration)
}

type setTenantIDTransport struct {
	http.RoundTripper
	tenantID string
}

func (s *setTenantIDTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req.Header.Set("X-Scope-OrgID", s.tenantID)
	return s.RoundTripper.RoundTrip(req)
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
				Username: cmp.Or(c.apiUser, c.tenantID),
				Password: config_util.Secret(c.apiKey),
			},
		},
		ChunkedReadLimit: c.readSizeLimit,
		Headers: map[string]string{
			"User-Agent": mimirtool_client.UserAgent(),
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

	level.Info(c.logger).Log("msg", "created remote read client", "endpoint", redactedURL(addressURL))

	return readClient, nil
}

func (c *RemoteReadCommand) parseArgsAndPrepareClient() (query func(context.Context, time.Time, time.Time) (storage.SeriesSet, error), from, to time.Time, err error) {
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

	readClient, err := c.readClient()
	if err != nil {
		return nil, time.Time{}, time.Time{}, err
	}

	return func(ctx context.Context, queryFrom, queryTo time.Time) (storage.SeriesSet, error) {
		level.Info(c.logger).Log("msg", "querying time", "from", queryFrom.Format(time.RFC3339), "to", queryTo.Format(time.RFC3339), "selectors", len(c.selectors))
		// Use already parsed selectors
		var pbQueries []*prompb.Query
		for i, matchers := range c.selectors {
			level.Debug(c.logger).Log("msg", "selector", "index", i+1, "matchers", fmt.Sprintf("%v", matchers))

			pbQuery, err := remote.ToQuery(
				int64(model.TimeFromUnixNano(queryFrom.UnixNano())),
				int64(model.TimeFromUnixNano(queryTo.UnixNano())),
				matchers,
				nil,
			)
			if err != nil {
				return nil, fmt.Errorf("error creating query for selector %s: %w", util.MatchersStringer(matchers), err)
			}
			pbQueries = append(pbQueries, pbQuery)
		}

		return readClient.ReadMultiple(ctx, pbQueries, true)
	}, from, to, nil
}

// prepare() validates the input and prepares the client to query remote read endpoints
func (c *RemoteReadCommand) prepare() (func(context.Context) (storage.SeriesSet, error), time.Time, time.Time, error) {
	query, from, to, err := c.parseArgsAndPrepareClient()
	if err != nil {
		return nil, time.Time{}, time.Time{}, err
	}

	return func(ctx context.Context) (storage.SeriesSet, error) {
		return query(ctx, from, to)
	}, from, to, nil
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
			level.Info(c.logger).Log("msg", line)
		}
	}

	return nil
}

func (c *RemoteReadCommand) export(_ *kingpin.ParseContext) error {
	if c.blockDuration <= 0 {
		return errors.New("block duration must be greater than zero")
	}

	query, from, to, err := c.parseArgsAndPrepareClient()
	if err != nil {
		return err
	}

	if c.tsdbPath == "" {
		c.tsdbPath, err = os.MkdirTemp("", "mimirtool-tsdb")
		if err != nil {
			return err
		}
		level.Info(c.logger).Log("msg", "created TSDB", "path", c.tsdbPath)
	} else {
		if _, err := os.Stat(c.tsdbPath); err != nil && os.IsNotExist(err) {
			if err = os.Mkdir(c.tsdbPath, 0755); err != nil {
				return err
			}
			level.Info(c.logger).Log("msg", "created TSDB", "path", c.tsdbPath)
		} else {
			level.Info(c.logger).Log("msg", "using existing TSDB", "path", c.tsdbPath)
		}
	}

	ctx := context.Background()
	var blocks []ulid.ULID

	for blockStart := alignToStartOfBlock(from, c.blockDuration); blockStart.Before(to) || blockStart.Equal(to); blockStart = blockStart.Add(c.blockDuration) {
		queryStart := maxTime(blockStart, from)
		queryEnd := minTime(blockStart.Add(c.blockDuration-time.Millisecond), to) // The query time range is inclusive at both ends, so don't query the first millisecond included in the next block.

		timeseries, err := query(ctx, queryStart, queryEnd)
		if err != nil {
			return err
		}

		block, err := backfill.CreateBlock(timeseries, c.tsdbPath, c.blockDuration)
		if err != nil {
			return err
		}

		if !block.IsZero() {
			// If there were no samples for this block, no block will be created and the ULID will be 0, so skip it.
			blocks = append(blocks, block)
		}
	}

	// Ensure that tsdb directory has WAL, otherwise 'promtool tsdb dump' fails.
	walPath := filepath.Join(c.tsdbPath, "wal")
	if _, err := os.Stat(walPath); err != nil && os.IsNotExist(err) {
		if err := os.Mkdir(walPath, 0755); err != nil {
			return err
		}
	}

	if err := c.printBlocks(blocks); err != nil {
		return fmt.Errorf("print blocks: %w", err)
	}

	return nil
}

// Based on https://github.com/prometheus/prometheus/blob/2f54aa060484a9a221eb227e1fb917ae66051c76/cmd/promtool/tsdb.go#L361-L398
func (c *RemoteReadCommand) printBlocks(blocks []ulid.ULID) error {
	db, err := tsdb.OpenDBReadOnly(c.tsdbPath, "", promslog.NewNopLogger())
	if err != nil {
		return fmt.Errorf("open db: %w", err)
	}

	defer db.Close()

	// Write block information to the logger, not directly to stdout.
	pipeR, pipeW := io.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer pipeR.Close()
		scanner := bufio.NewScanner(pipeR)
		for scanner.Scan() {
			level.Info(c.logger).Log("msg", scanner.Text())
		}
	}()
	defer func() { <-done }()
	defer pipeW.Close()

	tw := tabwriter.NewWriter(pipeW, 13, 0, 2, ' ', 0)
	defer tw.Flush()

	fmt.Fprintln(tw, "BLOCK ULID\tMIN TIME\tMAX TIME\tDURATION\tNUM SAMPLES\tNUM CHUNKS\tNUM SERIES\tSIZE")

	for _, b := range blocks {
		reader, err := db.Block(b.String(), nil)
		if err != nil {
			return fmt.Errorf("read block %s: %w", b, err)
		}

		meta := reader.Meta()

		fmt.Fprintf(tw,
			"%v\t%v\t%v\t%v\t%v\t%v\t%v\t%v\n",
			meta.ULID,
			formatTime(meta.MinTime),
			formatTime(meta.MaxTime),
			time.Duration(meta.MaxTime-meta.MinTime)*time.Millisecond,
			meta.Stats.NumSamples,
			meta.Stats.NumChunks,
			meta.Stats.NumSeries,
			formatBytes(reader.Size()),
		)
	}

	return nil
}

func formatTime(timestamp int64) string {
	return time.Unix(timestamp/1000, 0).UTC().String()
}

func formatBytes(bytes int64) string {
	return units.Base2Bytes(bytes).String()
}

func alignToStartOfBlock(t time.Time, blockSize time.Duration) time.Time {
	return timestamp.Time(blockSize.Milliseconds() * (timestamp.FromTime(t) / blockSize.Milliseconds()))
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}

	return b
}

func maxTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return b
	}

	return a
}
