// SPDX-License-Identifier: AGPL-3.0-only

package continuoustest

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/grafana/dskit/flagext"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/prompb"
)

type prometheusWriter struct {
	httpClient        *http.Client
	writeBaseEndpoint flagext.URLValue
	writeBatchSize    int
	writeTimeout      time.Duration
}

func (pw *prometheusWriter) sendWriteRequest(ctx context.Context, req *prompb.WriteRequest) (int, error) {
	data, err := proto.Marshal(req)
	if err != nil {
		return 0, err
	}

	ctx, cancel := context.WithTimeout(ctx, pw.writeTimeout)
	defer cancel()

	compressed := snappy.Encode(nil, data)
	httpReq, err := http.NewRequestWithContext(ctx, "POST", pw.writeBaseEndpoint.String()+"/api/v1/push", bytes.NewReader(compressed))
	if err != nil {
		// Errors from NewRequest are from unparseable URLs, so are not
		// recoverable.
		return 0, err
	}
	httpReq.Header.Add("Content-Encoding", "snappy")
	httpReq.Header.Set("Content-Type", "application/x-protobuf")
	httpReq.Header.Set("X-Prometheus-Remote-Write-Version", "0.1.0")

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
