// SPDX-License-Identifier: AGPL-3.0-only

package continuoustest

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/go-kit/log"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/grafana/dskit/flagext"
	"github.com/prometheus/prometheus/prompb"
)

const (
	maxErrMsgLen = 256
)

// MimirClient is the interface implemented by a client used to interact with Mimir.
type MimirClient interface {
	// WriteSeries writes input series to Mimir. Returns the response status code and optionally
	// an error. The error is always returned if request was not successful (eg. received a 4xx or 5xx error).
	WriteSeries(ctx context.Context, series []prompb.TimeSeries) (statusCode int, err error)
}

type ClientConfig struct {
	TenantID string

	WriteBaseEndpoint flagext.URLValue
	WriteBatchSize    int
	WriteTimeout      time.Duration
}

func (cfg *ClientConfig) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&cfg.TenantID, "tests.tenant-id", "anonymous", "The tenant ID to use to write and read metrics in tests.")

	f.Var(&cfg.WriteBaseEndpoint, "tests.write-endpoint", "The base endpoint on the write path. The URL should have no trailing slash.")
	f.IntVar(&cfg.WriteBatchSize, "tests.write-batch-size", 1000, "The maximum number of series to write in a single request.")
	f.DurationVar(&cfg.WriteTimeout, "tests.write-timeout", 5*time.Second, "The timeout for a single write request.")
}

type Client struct {
	client *http.Client
	cfg    ClientConfig
	logger log.Logger
}

func NewClient(cfg ClientConfig, logger log.Logger) (*Client, error) {
	rt := http.DefaultTransport
	rt = &clientRoundTripper{tenantID: cfg.TenantID, rt: rt}

	// Ensure the required config has been set.
	if cfg.WriteBaseEndpoint.URL == nil {
		return nil, errors.New("the write endpoint has not been set")
	}

	return &Client{
		client: &http.Client{Transport: rt},
		cfg:    cfg,
		logger: logger,
	}, nil
}

// WriteSeries implements MimirClient.
func (c *Client) WriteSeries(ctx context.Context, series []prompb.TimeSeries) (int, error) {
	lastStatusCode := 0

	// Honor the batch size.
	for o := 0; o < len(series); o += c.cfg.WriteBatchSize {
		end := o + c.cfg.WriteBatchSize
		if end > len(series) {
			end = len(series)
		}

		req := &prompb.WriteRequest{
			Timeseries: series[o:end],
		}

		var err error
		lastStatusCode, err = c.sendWriteRequest(ctx, req)
		if err != nil {
			return lastStatusCode, err
		}
	}

	return lastStatusCode, nil
}

func (c *Client) sendWriteRequest(ctx context.Context, req *prompb.WriteRequest) (int, error) {
	data, err := proto.Marshal(req)
	if err != nil {
		return 0, err
	}

	compressed := snappy.Encode(nil, data)
	httpReq, err := http.NewRequest("POST", c.cfg.WriteBaseEndpoint.String()+"/api/v1/push", bytes.NewReader(compressed))
	if err != nil {
		// Errors from NewRequest are from unparseable URLs, so are not
		// recoverable.
		return 0, err
	}
	httpReq.Header.Add("Content-Encoding", "snappy")
	httpReq.Header.Set("Content-Type", "application/x-protobuf")
	httpReq.Header.Set("User-Agent", "mimir-continuous-test")
	httpReq.Header.Set("X-Prometheus-Remote-Write-Version", "0.1.0")
	httpReq = httpReq.WithContext(ctx)

	ctx, cancel := context.WithTimeout(context.Background(), c.cfg.WriteTimeout)
	defer cancel()

	httpResp, err := c.client.Do(httpReq.WithContext(ctx))
	if err != nil {
		return 0, err
	}
	defer httpResp.Body.Close()

	if httpResp.StatusCode/100 != 2 {
		scanner := bufio.NewScanner(io.LimitReader(httpResp.Body, maxErrMsgLen))
		line := ""
		if scanner.Scan() {
			line = scanner.Text()
		}
		return httpResp.StatusCode, fmt.Errorf("server returned HTTP status %s and body %q (truncated to %d bytes)", httpResp.Status, line, maxErrMsgLen)
	}

	return httpResp.StatusCode, nil
}

type clientRoundTripper struct {
	tenantID string
	rt       http.RoundTripper
}

// RoundTrip add the tenant ID header required by Mimir.
func (rt *clientRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	req = cloneRequest(req)
	req.Header.Set("X-Scope-OrgID", rt.tenantID)
	return rt.rt.RoundTrip(req)
}

// cloneRequest returns a clone of the provided *http.Request.
// The clone is a shallow copy of the struct and its Header map.
func cloneRequest(r *http.Request) *http.Request {
	// Shallow copy of the struct.
	r2 := new(http.Request)
	*r2 = *r

	// Deep copy of the Header.
	r2.Header = make(http.Header)
	for k, s := range r.Header {
		r2.Header[k] = s
	}

	return r2
}
