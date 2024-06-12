// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/remote"

	"github.com/grafana/mimir/pkg/querier"
	"github.com/grafana/mimir/pkg/util"
)

type remoteReadRoundTripper struct {
	next http.RoundTripper
}

func newRemoteReadRoundTripper(next http.RoundTripper) http.RoundTripper {
	return &remoteReadRoundTripper{
		next: next,
	}
}

func (r *remoteReadRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	_, err := getRemoteReadRequestWithoutConsumingBody(req)
	if err != nil {
		return nil, err
	}

	// apply middlewares kept in 'r' to each individual request

	return r.next.RoundTrip(req)
}

// ParseRemoteReadRequestWithoutConsumingBody parses a remote read request
// without consuming the body. It does not check the req.Body size, so it is
// the caller's responsibility to ensure that the body is not too large.
func ParseRemoteReadRequestWithoutConsumingBody(req *http.Request) (url.Values, error) {
	remoteReadRequest, err := getRemoteReadRequestWithoutConsumingBody(req)
	if err != nil {
		return nil, err
	}
	return parseRemoteReadRequest(remoteReadRequest)
}

func getRemoteReadRequestWithoutConsumingBody(req *http.Request) (*prompb.ReadRequest, error) {
	if req.Body == nil {
		return nil, nil
	}

	bodyBytes, err := util.ReadRequestBodyWithoutConsuming(req)
	if err != nil {
		return nil, err
	}

	remoteReadRequest := &prompb.ReadRequest{}

	_, err = util.ParseProtoReader(req.Context(), io.NopCloser(bytes.NewReader(bodyBytes)), int(req.ContentLength), querier.MaxRemoteReadQuerySize, nil, remoteReadRequest, util.RawSnappy)
	if err != nil {
		return nil, err
	}

	return remoteReadRequest, nil
}

func parseRemoteReadRequest(remoteReadRequest *prompb.ReadRequest) (url.Values, error) {
	if remoteReadRequest == nil {
		return nil, nil
	}

	params := make(url.Values)
	add := func(i int, name, value string) { params.Add(name+"_"+strconv.Itoa(i), value) }

	queries := remoteReadRequest.GetQueries()

	for i, query := range queries {
		add(i, "start", fmt.Sprintf("%d", query.GetStartTimestampMs()))
		add(i, "end", fmt.Sprintf("%d", query.GetEndTimestampMs()))

		matchersStrings := make([]string, 0, len(query.Matchers))
		matchers, err := remote.FromLabelMatchers(query.Matchers)
		if err != nil {
			return nil, err
		}
		for _, m := range matchers {
			matchersStrings = append(matchersStrings, m.String())
		}
		params.Add("matchers_"+strconv.Itoa(i), strings.Join(matchersStrings, ","))
		if query.Hints != nil {
			if hints, err := json.Marshal(query.Hints); err == nil {
				add(i, "hints", string(hints))
			} else {
				add(i, "hints", fmt.Sprintf("error marshalling hints: %v", err))
			}
		}
	}

	return params, nil
}
