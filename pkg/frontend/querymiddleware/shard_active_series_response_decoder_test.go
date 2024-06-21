// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestShardActiveSeriesResponseDecoder(t *testing.T) {
	tcs := []struct {
		name               string
		input              string
		expectedOutput     string
		expectedError      string
		chunkBufferMaxSize int
	}{
		{
			name:          "empty response",
			input:         "",
			expectedError: "EOF",
		},
		{
			name:           "empty data array",
			input:          `{"data":[]}`,
			expectedOutput: "",
		},
		{
			name:           "skip object",
			input:          `{"unexpected_1":3.141516, "unexpected_2":"skip me", "unexpected_3":[[{}]], "unexpected_4": {"key":[]}, "unexpected_5":null, "unexpected_6":true, "data":[{"__name__":"metric","shard":"1"}]}`,
			expectedOutput: `{"__name__":"metric","shard":"1"}`,
		},
		{
			name:           "multiple labels",
			input:          `{"data":[{"__name__":"metric","shard":"1"},{"__name__":"metric","shard":"2"}]}`,
			expectedOutput: `{"__name__":"metric","shard":"1"},{"__name__":"metric","shard":"2"}`,
		},
		{
			name:               "reach max buffer size",
			input:              `{"data":[{"__name__":"metric","shard":"1"},{"__name__":"metric","shard":"2"},{"__name__":"metric","shard":"3"}]}`,
			expectedOutput:     `{"__name__":"metric","shard":"1"},{"__name__":"metric","shard":"2"},{"__name__":"metric","shard":"3"}`,
			chunkBufferMaxSize: 16,
		},
		{
			name:          "unexpected comma",
			input:         `{"data":[{"__name__":"metric","shard":"1"},,,{"__name__":"metric","shard":"2"}`,
			expectedError: "streamData: unexpected comma",
		},
		{
			name:          "unexpected end of input",
			input:         `{"data":[{"__name__":"metric","shard":"1"},{"__name__":"metric","shard":"2"}`,
			expectedError: "EOF",
		},
		{
			name:          "error response",
			input:         `{"status":"error","error":"some error"}`,
			expectedError: "error in partial response: some error",
		},
		{
			name:          "unicode escaped characters",
			input:         `{"error":"\u3053\u3093\u306B\u3061\u306F"}`,
			expectedError: "error in partial response: こんにちは",
		},
		{
			name:          "wrong data type",
			input:         `{"data":3.141516}`,
			expectedError: "expected data field to contain an array",
		},
		{
			name:          "missing 'data' and 'error' fields",
			input:         `{"unexpected":3.141516}`,
			expectedError: "expected data field at top level",
		},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			var dataStr strings.Builder

			errCh := make(chan error, 1)

			streamCh := make(chan *bytes.Buffer)

			r := strings.NewReader(tc.input)
			d := borrowShardActiveSeriesResponseDecoder(context.Background(), io.NopCloser(r), streamCh)
			if tc.chunkBufferMaxSize > 0 {
				d.chunkBufferMaxSize = tc.chunkBufferMaxSize
			}

			err := d.decode()
			if err == nil {
				go func() {
					errCh <- d.streamData()
					close(streamCh)
				}()

				// Drain the data channel.
				firstItem := true
				for streamBuf := range streamCh {
					if !firstItem {
						dataStr.WriteString(",")
					} else {
						firstItem = false
					}
					dataStr.WriteString(streamBuf.String())
				}
			} else {
				errCh <- err
			}

			err = <-errCh

			if len(tc.expectedError) > 0 {
				require.Error(t, err)
				require.EqualError(t, err, tc.expectedError)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expectedOutput, dataStr.String())
			}
		})
	}
}
