// SPDX-License-Identifier: AGPL-3.0-only

package continuoustest

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/grafana/dskit/flagext"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/prompb"

	"github.com/grafana/mimir/pkg/distributor"
)

type otlpHTTPWriter struct {
	httpClient        *http.Client
	writeBaseEndpoint flagext.URLValue
	writeBatchSize    int
	writeTimeout      time.Duration
}

func (pw *otlpHTTPWriter) sendWriteRequest(ctx context.Context, req *prompb.WriteRequest) (int, error) {
	metricRequest := distributor.TimeseriesToOTLPRequest(req.Timeseries, nil)
	rawBytes, err := metricRequest.MarshalProto()
	if err != nil {
		return 0, err
	}

	compressedBytes, err := compress(rawBytes)
	if err != nil {
		return 0, err
	}

	ctx, cancel := context.WithTimeout(ctx, pw.writeTimeout)
	defer cancel()

	httpReq, err := http.NewRequestWithContext(ctx, "POST", pw.writeBaseEndpoint.String()+"/otlp/v1/metrics", bytes.NewReader(compressedBytes))
	if err != nil {
		// Errors from NewRequest are from unparseable URLs, so are not
		// recoverable.
		return 0, err
	}
	httpReq.Header.Add("Content-Encoding", "gzip")
	httpReq.Header.Add("Accept-Encoding", "gzip")
	httpReq.Header.Set("Content-Type", "application/x-protobuf")

	httpResp, err := pw.httpClient.Do(httpReq)
	if err != nil {
		return 0, err
	}
	defer httpResp.Body.Close()

	if httpResp.StatusCode/100 != 2 {
		truncatedBody, err := io.ReadAll(io.LimitReader(httpResp.Body, maxErrMsgLen))
		if err != nil {
			return httpResp.StatusCode, errors.Wrapf(err, "server returned HTTP status %s and client failed to read response body", httpResp.Status)
		}

		return httpResp.StatusCode, fmt.Errorf("server returned HTTP status %s and body %q (truncated to %d bytes)", httpResp.Status, string(truncatedBody), maxErrMsgLen)
	}

	return httpResp.StatusCode, nil
}

func compress(input []byte) ([]byte, error) {
	var b bytes.Buffer
	gz := gzip.NewWriter(&b)
	_, err := gz.Write(input)
	if err != nil {
		return nil, errors.Wrap(err, "unable to compress data")
	}

	gz.Close()
	return b.Bytes(), nil
}
