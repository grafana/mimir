// SPDX-License-Identifier: AGPL-3.0-only

package requestoptions

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDecodeOptions(t *testing.T) {
	for _, tt := range []struct {
		name     string
		input    *http.Request
		expected Options
	}{
		{
			name: "default",
			input: &http.Request{
				Header: http.Header{},
			},
			expected: Options{},
		},
		{
			name: "disable cache",
			input: &http.Request{
				Header: http.Header{
					CacheControlHeader: []string{NoStoreValue},
				},
			},
			expected: Options{
				CacheDisabled: true,
			},
		},
		{
			name: "custom sharding",
			input: &http.Request{
				Header: http.Header{
					TotalShardsControlHeader: []string{"64"},
				},
			},
			expected: Options{
				TotalShards: 64,
			},
		},
		{
			name: "disable sharding",
			input: &http.Request{
				Header: http.Header{
					TotalShardsControlHeader: []string{"0"},
				},
			},
			expected: Options{
				ShardingDisabled: true,
			},
		},
		{
			name: "multiple sharding-control values, last valid wins",
			input: &http.Request{
				Header: http.Header{
					TotalShardsControlHeader: []string{"16", "32"},
				},
			},
			expected: Options{
				TotalShards: 32,
			},
		},
		{
			name: "multiple sharding-control values, invalid entries skipped",
			input: &http.Request{
				Header: http.Header{
					TotalShardsControlHeader: []string{"16", "not-a-number"},
				},
			},
			expected: Options{
				TotalShards: 16,
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tt.expected, DecodeOptions(tt.input))
		})
	}
}

func TestEncodeDecodeOptionsRoundTrip(t *testing.T) {
	for _, tt := range []struct {
		name string
		in   Options
	}{
		{name: "zero value", in: Options{}},
		{name: "cache disabled", in: Options{CacheDisabled: true}},
		{name: "sharding disabled", in: Options{ShardingDisabled: true}},
		{name: "total shards set", in: Options{TotalShards: 128}},
		{name: "all set", in: Options{CacheDisabled: true, TotalShards: 32}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req, err := http.NewRequest(http.MethodGet, "/", nil)
			require.NoError(t, err)
			EncodeOptions(req, tt.in)

			require.Equal(t, tt.in, DecodeOptions(req))
		})
	}
}

func TestDecodeCacheDisabledOption(t *testing.T) {
	for _, tt := range []struct {
		name     string
		headers  http.Header
		expected bool
	}{
		{
			name:    "absent",
			headers: http.Header{},
		},
		{
			name:     "present",
			headers:  http.Header{CacheControlHeader: []string{NoStoreValue}},
			expected: true,
		},
		{
			name:    "other value",
			headers: http.Header{CacheControlHeader: []string{"max-age=0"}},
		},
		{
			name:     "multiple values, no-store among them",
			headers:  http.Header{CacheControlHeader: []string{"max-age=0", NoStoreValue}},
			expected: true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tt.expected, DecodeCacheDisabledOption(&http.Request{Header: tt.headers}))
		})
	}
}

func TestContextRoundTrip(t *testing.T) {
	in := Options{CacheDisabled: true, ShardingDisabled: true, TotalShards: 16}
	ctx := ContextWithOptions(context.Background(), in)
	require.Equal(t, in, OptionsFromContext(ctx))
}

func TestOptionsFromContextZeroValueWhenAbsent(t *testing.T) {
	require.Equal(t, Options{}, OptionsFromContext(context.Background()))
}
