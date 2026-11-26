// SPDX-License-Identifier: AGPL-3.0-only

package client

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/encoding/proto"
	"google.golang.org/grpc/mem"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/mimirpb"
)

func TestMergeNativeResourceVersions(t *testing.T) {
	resource := func(host string) map[string]string { return map[string]string{"host.name": host} }
	version := func(minTime, maxTime int64, host string) NativeResourceVersion {
		return NativeResourceVersion{MinTimeMs: minTime, MaxTimeMs: maxTime, Identifying: map[string]string{"service.name": "api"}, Descriptive: resource(host)}
	}
	for name, tc := range map[string]struct {
		answers [][]NativeResourceVersion
		want    []NativeResourceVersion
	}{
		// The ingesters of a partition answer about the same series. One behind the other hasn't seen the latest change
		// yet, which the merge keeps, with the last version ending at the newest sample either has.
		"a lagging replica": {
			answers: [][]NativeResourceVersion{
				{version(100, 199, "a"), version(200, 500, "b")},
				{version(100, 300, "a")},
			},
			want: []NativeResourceVersion{version(100, 199, "a"), version(200, 500, "b")},
		},
		// Two answers have different content from the same time: the one with the newer sample has read further.
		"the same time, different content": {
			answers: [][]NativeResourceVersion{
				{version(100, 300, "a")},
				{version(100, 400, "b")},
			},
			want: []NativeResourceVersion{version(100, 400, "b")},
		},
		// A target's series in different partitions see its resource change at their own next samples: the change is in
		// force from the first of them.
		"a change seen at different samples": {
			answers: [][]NativeResourceVersion{
				{version(100, 999, "a"), version(1_000, 2_000, "b")},
				{version(100, 1_014, "a"), version(1_015, 2_015, "b")},
			},
			want: []NativeResourceVersion{version(100, 999, "a"), version(1_000, 2_015, "b")},
		},
		// Two identities push for the same target at once: the one whose version started later wins from its start on,
		// and the first takes over again only with a change of its own.
		"a concurrent collision": {
			answers: [][]NativeResourceVersion{
				{version(100, 299, "a"), version(300, 600, "c")},
				{version(150, 600, "b")},
			},
			want: []NativeResourceVersion{version(100, 149, "a"), version(150, 299, "b"), version(300, 600, "c")},
		},
		// An answer whose series' newest sample the ingester doesn't hold ends its version at the version's start.
		"a last version without a newer sample": {
			answers: [][]NativeResourceVersion{{version(100, 100, "a")}},
			want:    []NativeResourceVersion{version(100, 100, "a")},
		},
	} {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.want, MergeNativeResourceVersions(tc.answers))
		})
	}
}

// Metric metadata merges like resources, by its type, unit and help.
func TestMergeNativeMetricMetadataVersions(t *testing.T) {
	requests := NativeMetricMetadataVersion{Type: mimirpb.COUNTER, Help: "Requests.", Unit: "requests"}
	served := NativeMetricMetadataVersion{Type: mimirpb.COUNTER, Help: "Requests served.", Unit: "requests"}
	at := func(v NativeMetricMetadataVersion, minTime, maxTime int64) NativeMetricMetadataVersion {
		v.MinTimeMs, v.MaxTimeMs = minTime, maxTime
		return v
	}
	merged := MergeNativeMetricMetadataVersions([][]NativeMetricMetadataVersion{
		{at(requests, 100, 199), at(served, 200, 299), at(requests, 300, 400)},
		{at(requests, 100, 199), at(served, 200, 250)},
	})
	require.Equal(t, []NativeMetricMetadataVersion{at(requests, 100, 199), at(served, 200, 299), at(requests, 300, 400)}, merged)
}

// A querier tells a disabled tenant and a lookup over its cap apart from failures, and from each other, through the
// gRPC status, also when an error wraps it.
func TestNativeLookupErrors(t *testing.T) {
	tooMany := NativeLookupTooManySeriesError(10)
	require.True(t, IsNativeMetadataDisabled(fmt.Errorf("ingester: %w", ErrNativeMetadataDisabled)))
	require.False(t, IsNativeMetadataDisabled(tooMany))
	require.True(t, IsNativeLookupTooManySeries(fmt.Errorf("ingester: %w", tooMany)))
	require.False(t, IsNativeLookupTooManySeries(ErrNativeMetadataDisabled))
	require.False(t, IsNativeMetadataDisabled(status.Error(codes.FailedPrecondition, "something else")))
}

// The codec unmarshals the responses' labels without copying them, so they refer to the buffer the response came in.
// gRPC releases its reference to the buffer as soon as the codec returns, and its pool hands the buffer to a later
// message, so each response keeps a reference until FreeBuffer. Without one, a querier merged series whose labels
// had become another message's, or zeros.
func TestNativeLookupResponses_Unmarshal(t *testing.T) {
	var series [][]mimirpb.LabelAdapter
	// Enough series for gRPC to pool the buffer, which it doesn't for small ones.
	for i := range 64 {
		series = append(series, []mimirpb.LabelAdapter{{Name: "instance", Value: fmt.Sprintf("10.0.0.%d:9100", i)}, {Name: "job", Value: "node"}})
	}
	unmarshal := func(t *testing.T, sent any, received mimirpb.MessageWithBufferRef) {
		codec := encoding.GetCodecV2(proto.Name)
		data, err := codec.Marshal(sent)
		require.NoError(t, err)
		raw := data.Materialize()
		data.Free()
		require.False(t, mem.IsBelowBufferPoolingThreshold(len(raw)), "gRPC doesn't pool a buffer of %d bytes", len(raw))
		buf := mem.NewBuffer(&raw, zeroingPool{})
		require.NoError(t, codec.Unmarshal(mem.BufferSlice{buf}, received))
		// gRPC's reference.
		buf.Free()
	}

	t.Run("metric metadata", func(t *testing.T) {
		sent := &NativeMetricMetadataResponse{}
		for _, lbls := range series {
			sent.Series = append(sent.Series, NativeMetricMetadataSeries{Labels: lbls})
		}
		var received NativeMetricMetadataResponse
		unmarshal(t, sent, &received)
		defer received.FreeBuffer()
		for i, s := range received.Series {
			require.Equal(t, series[i], s.Labels)
		}
	})
	t.Run("resources", func(t *testing.T) {
		sent := &NativeResourcesResponse{}
		for _, lbls := range series {
			sent.Series = append(sent.Series, NativeResourcesSeries{Labels: lbls})
		}
		var received NativeResourcesResponse
		unmarshal(t, sent, &received)
		defer received.FreeBuffer()
		for i, s := range received.Series {
			require.Equal(t, series[i], s.Labels)
		}
	})
}

// zeroingPool stands for gRPC's buffer pool, and zeroes a buffer it gets back, as a later message reusing it would
// overwrite it.
type zeroingPool struct{}

func (zeroingPool) Get(length int) *[]byte {
	b := make([]byte, length)
	return &b
}

func (zeroingPool) Put(b *[]byte) { clear(*b) }
