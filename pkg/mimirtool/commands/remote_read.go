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
	"strings"
	"text/tabwriter"
	"time"

	"github.com/alecthomas/kingpin/v2"
	"github.com/pkg/errors"
	config_util "github.com/prometheus/common/config"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/storage/remote"
	log "github.com/sirupsen/logrus"

	"github.com/grafana/mimir/pkg/mimirtool/backfill"
	"github.com/grafana/mimir/pkg/mimirtool/client"
)

type RemoteReadCommand struct {
	address        string
	remoteReadPath string

	tenantID string
	apiKey   string

	readTimeout time.Duration
	tsdbPath    string

	selector string
	from     string
	to       string
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

		cmd.Flag("selector", `PromQL selector to filter metrics on. To return all metrics '{__name__!=""}' can be used.`).
			Default("up").
			StringVar(&c.selector)

		cmd.Flag("from", "Start of the time window to select metrics.").
			Default(now.Add(-time.Hour).Format(time.RFC3339)).
			StringVar(&c.from)
		cmd.Flag("to", "End of the time window to select metrics.").
			Default(now.Format(time.RFC3339)).
			StringVar(&c.to)
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
	posSeries int
	posSample int
	ts        []*prompb.TimeSeries

	// labels slice is reused across samples within a series
	labels          labels.Labels
	labelsSeriesPos int
}

func newTimeSeriesIterator(ts []*prompb.TimeSeries) *timeSeriesIterator {
	return &timeSeriesIterator{
		posSeries: 0,
		posSample: -1,

		// ensure we are not pointing to a valid slice position
		labelsSeriesPos: -1,

		ts: ts,
	}

}

func (i *timeSeriesIterator) Next() error {
	if i.posSeries >= len(i.ts) {
		return io.EOF
	}

	i.posSample++

	if i.posSample >= len(i.ts[i.posSeries].Samples) {
		i.posSample = -1
		i.posSeries++
		return i.Next()
	}

	return nil
}

func (i *timeSeriesIterator) Labels() (l labels.Labels) {
	// if it's the same label as previously return it
	if i.posSeries == i.labelsSeriesPos {
		return i.labels
	}

	series := i.ts[i.posSeries]
	builder := labels.NewScratchBuilder(len(series.Labels))
	for posLabel := range series.Labels {
		builder.Add(series.Labels[posLabel].Name, series.Labels[posLabel].Value)
	}
	i.labels = builder.Labels()
	i.labelsSeriesPos = i.posSeries
	return i.labels
}

func (i *timeSeriesIterator) Sample() (ts int64, v float64) {
	series := i.ts[i.posSeries]
	sample := series.Samples[i.posSample]

	//sample.GetValue()
	return sample.GetTimestamp(), sample.GetValue()
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
func (c *RemoteReadCommand) prepare() (query func(context.Context) ([]*prompb.TimeSeries, error), from, to time.Time, err error) {
	from, err = time.Parse(time.RFC3339, c.from)
	if err != nil {
		return nil, time.Time{}, time.Time{}, fmt.Errorf("error parsing from: '%s' value: %w", c.from, err)
	}

	to, err = time.Parse(time.RFC3339, c.to)
	if err != nil {
		return nil, time.Time{}, time.Time{}, fmt.Errorf("error parsing to: '%s' value: %w", c.to, err)
	}

	matchers, err := parser.ParseMetricSelector(c.selector)
	if err != nil {
		return nil, time.Time{}, time.Time{}, err
	}

	readClient, err := c.readClient()
	if err != nil {
		return nil, time.Time{}, time.Time{}, err
	}

	pbQuery, err := remote.ToQuery(
		int64(model.TimeFromUnixNano(from.UnixNano())),
		int64(model.TimeFromUnixNano(to.UnixNano())),
		matchers,
		nil,
	)
	if err != nil {
		return nil, time.Time{}, time.Time{}, err
	}

	return func(ctx context.Context) ([]*prompb.TimeSeries, error) {
		log.Infof("Querying time from=%s to=%s with selector=%s", from.Format(time.RFC3339), to.Format(time.RFC3339), c.selector)
		resp, err := readClient.Read(ctx, pbQuery)
		if err != nil {
			return nil, err
		}

		return resp.Timeseries, nil

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

	iterator := newTimeSeriesIterator(timeseries)
	for {
		err := iterator.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}

		l := iterator.Labels()
		ts, v := iterator.Sample()
		comment := ""
		if value.IsStaleNaN(v) {
			comment = " # StaleNaN"
		}
		fmt.Printf("%s %g %d%s\n", l, v, ts, comment)
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

	iterator := newTimeSeriesIterator(timeseries)
	for {
		err := iterator.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
		num.Samples++
		num.Series[iterator.Labels().String()] = struct{}{}

		ts, v := iterator.Sample()

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
