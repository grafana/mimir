// SPDX-License-Identifier: AGPL-3.0-only
//go:build rustengine && cgo

package rustengine

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRustWireOwnershipAndMalformedFrames(t *testing.T) {
	expected := Response{LabelCounts: map[string]map[string]uint64{"zone": {"a": 7}}, Error: "failure", Ref: 17, Series: []Series{{Ref: 17, Labels: []Label{{Name: "name", Value: "λ"}}, Samples: map[int64]Sample{2: {Timestamp: 2, Kind: 1, Bits: 0x7ff0000000000002}}, Exemplars: []Exemplar{}}}, Refs: []uint64{1, 2}, Values: []string{"a", "b"}, Results: []FloatResult{{Index: 5, Ref: 17, Accepted: 2, Errors: []SampleError{{Timestamp: 3, Error: "too_old"}}}}, Leftovers: []int{7}, Sample: &Sample{Timestamp: 2, Kind: 2, Data: "opaque"}, Bytes: map[string]uint64{"name": 100}, Cursor: 18, Chunks: []QueryChunk{{Mint: 1, Maxt: 2, Data: []byte{1, 2, 3}, Samples: []Sample{}}, {Mint: 3, Maxt: 4, Data: []byte{4, 5}, Samples: []Sample{}}}}
	w := wireWriter(binary.LittleEndian.AppendUint32(nil, wireMagic))
	w.Response(expected)
	got, err := decodeResponse(w)
	require.NoError(t, err)
	require.Equal(t, expected, got)
	for end := range len(w) {
		_, err = decodeResponse(w[:end])
		require.Error(t, err, "truncated at %d", end)
	}
	_, err = decodeResponse(append(append([]byte(nil), w...), 0))
	require.EqualError(t, err, "trailing Rust frame bytes")
	clear(w)
	require.Equal(t, expected, got)
	require.Equal(t, len(got.Chunks[0].Data), cap(got.Chunks[0].Data))
	modified := append(got.Chunks[0].Data, 9, 9, 9)
	clear(modified)
	require.Equal(t, expected, got)
}

func FuzzRustWireResponse(f *testing.F) {
	w := wireWriter(binary.LittleEndian.AppendUint32(nil, wireMagic))
	w.Response(Response{})
	f.Add([]byte(w))
	f.Add([]byte("{}"))
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = decodeResponse(data)
		_, _ = decodeResponseWithLabelSets(data, true)
	})
}

func TestRustWireValueFilters(t *testing.T) {
	req := Request{Op: "cursor_open_select", Filters: []LabelFilter{
		{Name: "group", Values: []string{"a", "λ\n\x00"}},
		{Name: "missing", Values: []string{"x"}, Exclude: true, MatchMissing: true},
		{Name: "empty", Values: []string{""}, MatchMissing: true},
	}}
	frame := encodeRequest(nil, req)
	r := wireReader{data: frame[4:]}
	decoded := r.Request()
	require.NoError(t, r.err)
	require.Empty(t, r.data)
	require.Equal(t, req.Filters, decoded.Filters)
	for end := 4; end < len(frame); end++ {
		r := wireReader{data: frame[4:end]}
		r.Request()
		require.Error(t, r.err, "truncated at %d", end)
	}
}

func TestRustWireValueSetOwnershipAndTruncation(t *testing.T) {
	w := wireWriter(binary.LittleEndian.AppendUint32(nil, wireMagic))
	w.Response(Response{LabelCounts: map[string]map[string]uint64{"zone": {"λ\x00": 7, "": 0, "b": 3, "a": 2}}})
	frame := append([]byte(nil), w...)
	got, err := decodeResponseWithLabelSets(w, true)
	require.NoError(t, err)
	expected := map[string][]string{"zone": {"", "a", "b", "λ\x00"}}
	require.Equal(t, expected, got.LabelSets)
	require.Nil(t, got.LabelCounts)
	clear(w)
	require.Equal(t, expected, got.LabelSets)
	for end := range len(frame) {
		_, err := decodeResponseWithLabelSets(frame[:end], true)
		require.Error(t, err, "truncated at %d", end)
	}
	_, err = decodeResponseWithLabelSets(append(frame, 0), true)
	require.EqualError(t, err, "trailing Rust frame bytes")
}
