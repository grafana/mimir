// SPDX-License-Identifier: AGPL-3.0-only

package indexheader

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/gate"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"

	streamencoding "github.com/grafana/mimir/pkg/storage/indexheader/encoding"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
)

func writeV4SymbolFixture(t testing.TB, count int) []byte {
	t.Helper()
	path := filepath.Join(t.TempDir(), block.IndexFilename)
	w, err := index.NewWriterWithOptions(t.Context(), path, index.WriterOptions{FormatVersion: index.FormatV4})
	require.NoError(t, err)
	for i := range count {
		require.NoError(t, w.AddSymbol(fmt.Sprintf("%064d", i)))
	}
	require.NoError(t, w.AddSeries(0, labels.FromStrings(fmt.Sprintf("%064d", 0), fmt.Sprintf("%064d", count-1))))
	require.NoError(t, w.Close())
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	return data
}

func TestWriteBinary_V4SymbolValidationBeforePublication(t *testing.T) {
	ctx := t.Context()
	id := ulid.MustNew(1, nil)
	raw := writeV4SymbolFixture(t, 97)
	toc, err := index.NewTOCFromByteSlice(realByteSlice(raw))
	require.NoError(t, err)
	payloadEnd := toc.Symbols + 8 + binary.BigEndian.Uint64(raw[toc.Symbols:])
	for _, tc := range []struct {
		name   string
		damage func([]byte)
		want   string
	}{
		{"payload checksum", func(b []byte) { b[payloadEnd] ^= 1 }, "invalid checksum"},
		{"wrong sample ordinal", func(b []byte) {
			// Point the second sample at symbol 33 instead of 32, preserving the
			// other offsets, sample ordering and both checksums.
			d := b[payloadEnd+8 : toc.Series-4]
			_, n := binary.Uvarint(d[4:])
			next := d[4+n:]
			for _, shift := range []int{65, -65} {
				delta, width := binary.Uvarint(next)
				require.Positive(t, width)
				require.Equal(t, width, binary.PutUvarint(next, uint64(int(delta)+shift)))
				next = next[width:]
			}
			binary.BigEndian.PutUint32(b[toc.Series-4:], crc32.Checksum(d, castagnoliTable))
		}, "symbol sample does not point to its ordinal"},
	} {
		damaged := slices.Clone(raw)
		tc.damage(damaged)
		for _, version := range []int{BinaryFormatV1, BinaryFormatV2} {
			t.Run(fmt.Sprintf("%s/header-%d", tc.name, version), func(t *testing.T) {
				bkt := objstore.NewInMemBucket()
				blockDir := t.TempDir()
				object := id.String() + "/index"
				require.NoError(t, bkt.Upload(ctx, object, bytes.NewReader(raw)))
				require.NoError(t, WriteBinary(ctx, bkt, id, blockDir, version))
				path := indexHeaderPath(blockDir, version)
				admitted, err := os.ReadFile(path)
				require.NoError(t, err)
				require.EqualValues(t, magicValidatedV4, binary.BigEndian.Uint32(admitted))
				require.NoError(t, bkt.Upload(ctx, object, bytes.NewReader(damaged)))
				require.ErrorContains(t, WriteBinary(ctx, bkt, id, blockDir, version), tc.want)
				onDisk, err := os.ReadFile(path)
				require.NoError(t, err)
				require.Equal(t, admitted, onDisk)
			})
		}
	}
}

func TestLazyBinaryReader_V4SymbolValidationSurvivesReload(t *testing.T) {
	ctx := t.Context()
	id := ulid.MustNew(1, nil)
	raw := writeV4SymbolFixture(t, 129)
	buildBucket := objstore.NewInMemBucket()
	require.NoError(t, buildBucket.Upload(ctx, id.String()+"/index", bytes.NewReader(raw)))
	// Keep the original postings accessible in bucket mode, but make a symbol
	// download fail validation. A successful upgrade must use the local copy.
	toc, err := index.NewTOCFromByteSlice(realByteSlice(raw))
	require.NoError(t, err)
	damaged := slices.Clone(raw)
	damaged[toc.Symbols+8] ^= 1
	bkt := objstore.WithNoopInstr(objstore.NewInMemBucket())
	require.NoError(t, bkt.Upload(ctx, id.String()+"/index", bytes.NewReader(damaged)))
	for _, version := range []int{BinaryFormatV1, BinaryFormatV2} {
		t.Run(fmt.Sprintf("header-%d", version), func(t *testing.T) {
			dir := t.TempDir()
			blockDir := filepath.Join(dir, id.String())
			require.NoError(t, os.Mkdir(blockDir, 0700))
			require.NoError(t, WriteBinary(ctx, buildBucket, id, blockDir, version))
			path := indexHeaderPath(blockDir, version)
			data, err := os.ReadFile(path)
			require.NoError(t, err)
			// Recreate the prefix left by an older gateway. The first load must
			// validate and atomically replace it even with verify-on-load disabled.
			binary.BigEndian.PutUint32(data, MagicIndex)
			require.NoError(t, os.WriteFile(path, data, 0600))
			old, err := os.Stat(path)
			require.NoError(t, err)
			cfg := Config{Version: version}
			logger := log.NewNopLogger()
			metrics := NewStreamBinaryReaderMetrics(nil)
			factory := func(ctx context.Context) (Reader, error) {
				return NewStreamBinaryReader(ctx, id, bkt, dir, cfg, 32, logger, metrics)
			}
			r, err := NewLazyBinaryReader(ctx, cfg, factory, logger, bkt, dir, id, NewLazyBinaryReaderMetrics(nil), nil, gate.NewNoop())
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, r.Close()) })
			got, err := r.LookupSymbol(ctx, 0)
			require.NoError(t, err)
			require.Equal(t, fmt.Sprintf("%064d", 0), got)
			require.NoError(t, r.unloadIfIdleSince(0))
			admitted, err := os.Stat(path)
			require.NoError(t, err)
			require.False(t, os.SameFile(old, admitted))
			data, err = os.ReadFile(path)
			require.NoError(t, err)
			require.EqualValues(t, magicValidatedV4, binary.BigEndian.Uint32(data))
			// Break the order of an intermediate sample outside the label-name
			// lookup path. A repeated sampled-symbol walk would fail here.
			sample := bytes.Index(data, []byte(fmt.Sprintf("%064d", 96)))
			require.Positive(t, sample)
			data[sample] = 'z'
			require.NoError(t, os.WriteFile(path, data, 0600))
			got, err = r.LookupSymbol(ctx, 1)
			require.NoError(t, err)
			require.Equal(t, fmt.Sprintf("%064d", 1), got)
			require.NoError(t, r.Close())

			// A fresh reader has no in-memory record of the previous admission.
			fresh, err := factory(ctx)
			require.NoError(t, err)
			require.NoError(t, fresh.Close())
			cfg.VerifyOnLoad = true
			_, err = factory(ctx)
			require.ErrorIs(t, err, streamencoding.ErrInvalidChecksum)
		})
	}
}

func BenchmarkV4IndexHeaderSymbols(b *testing.B) {
	ctx := b.Context()
	id := ulid.MustNew(1, nil)
	bkt := objstore.WithNoopInstr(objstore.NewInMemBucket())
	require.NoError(b, bkt.Upload(ctx, id.String()+"/index", bytes.NewReader(writeV4SymbolFixture(b, 131072))))
	logger := log.NewNopLogger()
	metrics := NewStreamBinaryReaderMetrics(nil)
	for _, version := range []int{BinaryFormatV1, BinaryFormatV2} {
		local := b.TempDir()
		blockDir := filepath.Join(local, id.String())
		require.NoError(b, os.Mkdir(blockDir, 0700))
		cfg := Config{Version: version}
		require.NoError(b, WriteBinary(ctx, bkt, id, blockDir, version))
		b.Run(fmt.Sprintf("header-%d/build", version), func(b *testing.B) {
			for b.Loop() {
				if err := WriteBinary(ctx, bkt, id, blockDir, version); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(fmt.Sprintf("header-%d/reload", version), func(b *testing.B) {
			for b.Loop() {
				r, err := NewStreamBinaryReader(ctx, id, bkt, local, cfg, 32, logger, metrics)
				if err != nil {
					b.Fatal(err)
				}
				if err := r.Close(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
