// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ingester/client/cortex_util.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package client

import (
	context "context"
	"fmt"

	"google.golang.org/grpc"

	"github.com/grafana/mimir/pkg/mimirpb"
)

// SendQueryStream wraps the stream's Send() checking if the context is done
// before calling Send().
func SendQueryStream(s Ingester_QueryStreamServer, m *QueryStreamResponse) error {
	return sendWithContextErrChecking(s.Context(), func() error {
		return s.Send(m)
	})
}

// SendLabelNamesAndValuesResponse wraps the stream's Send() checking if the context is done
// before calling Send().
func SendLabelNamesAndValuesResponse(s Ingester_LabelNamesAndValuesServer, response *LabelNamesAndValuesResponse) error {
	return sendWithContextErrChecking(s.Context(), func() error {
		return s.Send(response)
	})
}

// SendLabelValuesCardinalityResponse wraps the stream's Send() checking if the context is done
// before calling Send().
func SendLabelValuesCardinalityResponse(s Ingester_LabelValuesCardinalityServer, response *LabelValuesCardinalityResponse) error {
	return sendWithContextErrChecking(s.Context(), func() error {
		return s.Send(response)
	})
}

func sendWithContextErrChecking(ctx context.Context, send func() error) error {
	// If the context has been canceled or its deadline exceeded, we should return it
	// instead of the cryptic error the Send() will return.
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}

	if err := send(); err != nil {
		// Experimentally, we've seen the context switching to done after the Send()
		// has been  called, so here we do recheck the context in case of error.
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}

		return err
	}

	return nil
}

// AccumulateChunks builds a slice of chunks, eliminating duplicates.
// This is O(N^2) but most of the time N is small.
func AccumulateChunks(a, b []Chunk) []Chunk {
	ret := a
	for j := range b {
		if !containsChunk(a, b[j]) {
			ret = append(ret, b[j])
		}
	}
	return ret
}

func containsChunk(a []Chunk, b Chunk) bool {
	for i := range a {
		if a[i].Equal(b) {
			return true
		}
	}
	return false
}

func PushRaw(ctx context.Context, client IngesterClient, in interface{}, opts ...grpc.CallOption) (*mimirpb.WriteResponse, error) {
	if c, ok := client.(*closableHealthAndIngesterClient); ok {
		client = c.IngesterClient
	}
	c, ok := client.(*ingesterClient)
	if !ok {
		return nil, fmt.Errorf("invalid ingester client: %T", client)
	}

	out := new(mimirpb.WriteResponse)
	err := c.cc.Invoke(ctx, "/cortex.Ingester/Push", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}
