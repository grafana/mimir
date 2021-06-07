package bench

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"

	"github.com/cortexproject/cortex/pkg/util/spanlogger"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/opentracing-contrib/go-stdlib/nethttp"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	config_util "github.com/prometheus/common/config"
	"github.com/prometheus/common/version"
	"github.com/prometheus/prometheus/storage/remote"
)

const maxErrMsgLen = 512

var UserAgent = fmt.Sprintf("Benchtool/%s", version.Version)

// writeClient allows reading and writing from/to a remote HTTP endpoint.
type writeClient struct {
	remoteName string // Used to differentiate clients in metrics.
	url        *config_util.URL
	Client     *http.Client
	timeout    time.Duration
	tenantName string

	logger          log.Logger
	requestDuration *prometheus.HistogramVec
}

// newWriteClient creates a new client for remote write.
func newWriteClient(name string, tenantName string, conf *remote.ClientConfig, logger log.Logger, requestHistogram *prometheus.HistogramVec) (*writeClient, error) {
	httpClient, err := config_util.NewClientFromConfig(conf.HTTPClientConfig, "bench_write_client", config_util.WithHTTP2Disabled())
	if err != nil {
		return nil, err
	}

	t := httpClient.Transport
	httpClient.Transport = &nethttp.Transport{
		RoundTripper: t,
	}

	return &writeClient{
		remoteName: name,
		url:        conf.URL,
		Client:     httpClient,
		timeout:    time.Duration(conf.Timeout),
		tenantName: tenantName,

		requestDuration: requestHistogram,
	}, nil
}

// Store sends a batch of samples to the HTTP endpoint, the request is the proto marshalled
// and encoded bytes from codec.go.
func (c *writeClient) Store(ctx context.Context, req []byte) error {
	spanLog, ctx := spanlogger.New(ctx, "writeClient.Store")
	defer spanLog.Span.Finish()
	httpReq, err := http.NewRequest("POST", c.url.String(), bytes.NewReader(req))
	if err != nil {
		// Errors from NewRequest are from unparsable URLs, so are not
		// recoverable.
		return err
	}
	httpReq.Header.Add("Content-Encoding", "snappy")
	httpReq.Header.Set("Content-Type", "application/x-protobuf")
	httpReq.Header.Set("User-Agent", UserAgent)
	httpReq.Header.Set("X-Prometheus-Remote-Write-Version", "0.1.0")
	if c.tenantName != "" {
		httpReq.Header.Set("X-Scope-OrgID", c.tenantName)
	}
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	httpReq = httpReq.WithContext(ctx)

	start := time.Now()

	httpResp, err := c.Client.Do(httpReq)
	if err != nil {
		// Errors from Client.Do are from (for example) network errors, so are
		// recoverable.
		return err
	}
	c.requestDuration.WithLabelValues(strconv.Itoa(httpResp.StatusCode)).Observe(time.Since(start).Seconds())

	defer func() {
		_, err := io.Copy(ioutil.Discard, httpResp.Body)
		if err != nil {
			level.Error(c.logger).Log("msg", "unable to discard write request body", "err", err)
		}
		httpResp.Body.Close()
	}()

	if httpResp.StatusCode/100 != 2 {
		scanner := bufio.NewScanner(io.LimitReader(httpResp.Body, maxErrMsgLen))
		line := ""
		if scanner.Scan() {
			line = scanner.Text()
		}
		err = errors.Errorf("server returned HTTP status %s: %s", httpResp.Status, line)
	}
	if httpResp.StatusCode/100 == 5 {
		return err
	}
	return err
}
