// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ingester/client/cortex_util.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package client

import (
	context "context"
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

// SendLabelValuesStreamResponse wraps the stream's Send() checking if the context is done
// before calling Send().
func SendLabelValuesStreamResponse(s Ingester_LabelValuesStreamServer, response *LabelValuesResponse) error {
	return sendWithContextErrChecking(s.Context(), func() error {
		return s.Send(response)
	})
}

func SendActiveSeriesResponse(s Ingester_ActiveSeriesServer, response *ActiveSeriesResponse) error {
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
