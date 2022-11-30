// Copyright 2017 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package index

import (
	"hash/crc32"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/prometheus/prometheus/tsdb/encoding"
	"github.com/prometheus/prometheus/tsdb/index"

	stream_encoding "github.com/grafana/mimir/pkg/storegateway/indexheader/encoding"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestSymbols(t *testing.T) {
	buf := encoding.Encbuf{}

	// Add prefix to the buffer to simulate symbols as part of larger buffer.
	buf.PutUvarintStr("something")

	symbolsStart := buf.Len()
	buf.PutBE32int(204) // Length of symbols table.
	buf.PutBE32int(100) // Number of symbols.
	for i := 0; i < 100; i++ {
		// i represents index in unicode characters table.
		buf.PutUvarintStr(string(rune(i))) // Symbol.
	}
	checksum := crc32.Checksum(buf.Get()[symbolsStart+4:], castagnoliTable)
	buf.PutBE32(checksum) // Check sum at the end.

	dir := t.TempDir()
	filePath := path.Join(dir, "index")
	require.NoError(t, os.WriteFile(filePath, buf.Get(), 0700))

	f, err := os.Open(filePath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = f.Close() })

	d := stream_encoding.NewDecbufFromFile(f, symbolsStart, castagnoliTable)
	require.NoError(t, d.E)
	s, err := NewSymbols(f, index.FormatV2, symbolsStart)
	require.NoError(t, err)

	// We store only 4 offsets to symbols.
	require.Equal(t, 32, s.Size())

	for i := 99; i >= 0; i-- {
		s, err := s.Lookup(uint32(i))
		require.NoError(t, err)
		require.Equal(t, string(rune(i)), s)
	}
	_, err = s.Lookup(100)
	require.Error(t, err)

	for i := 99; i >= 0; i-- {
		r, err := s.ReverseLookup(string(rune(i)))
		require.NoError(t, err)
		require.Equal(t, uint32(i), r)
	}
	_, err = s.ReverseLookup(string(rune(100)))
	require.Error(t, err)

	//	iter := s.Iter()
	//	i := 0
	//	for iter.Next() {
	//		require.Equal(t, string(rune(i)), iter.At())
	//		i++
	//	}
	//	require.NoError(t, iter.Err())
}
