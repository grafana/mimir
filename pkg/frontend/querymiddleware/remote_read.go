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

// ParseRemoteReadRequestWithoutConsumingBody parses a remote read request
// without consuming the body. It does not check the req.Body size, so it is
// the caller's responsibility to ensure that the body is not too large.
func ParseRemoteReadRequestWithoutConsumingBody(req *http.Request) (url.Values, error) {
	params := make(url.Values)

	if req.Body == nil {
		return params, nil
	}

	bodyBytes, err := util.ReadRequestBodyWithoutConsuming(req)
	if err != nil {
		return nil, err
	}

	remoteReadRequest := prompb.ReadRequest{}

	_, err = util.ParseProtoReader(req.Context(), io.NopCloser(bytes.NewReader(bodyBytes)), int(req.ContentLength), querier.MaxRemoteReadQuerySize, nil, &remoteReadRequest, util.RawSnappy)
	if err != nil {
		return nil, err
	}

	queries := remoteReadRequest.GetQueries()

	for i, query := range queries {
		params.Add("start_"+strconv.Itoa(i), fmt.Sprintf("%d", query.GetStartTimestampMs()))
		params.Add("end_"+strconv.Itoa(i), fmt.Sprintf("%d", query.GetEndTimestampMs()))

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
				params.Add("hints_"+strconv.Itoa(i), string(hints))
			} else {
				params.Add("hints_"+strconv.Itoa(i), fmt.Sprintf("error marshalling hints: %v", err))
			}
		}
	}

	return params, err
}
