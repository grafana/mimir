// SPDX-License-Identifier: AGPL-3.0-only

package indexheader

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/encoding"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"

	streamencoding "github.com/grafana/mimir/pkg/storage/indexheader/encoding"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
)

func TestV4IndexHeaders(t *testing.T) {
	ctx := t.Context()
	source := t.TempDir()
	id := ulid.MustNew(1, nil)
	dir := filepath.Join(source, id.String())
	require.NoError(t, os.MkdirAll(dir, 0755))
	filename := filepath.Join(dir, block.IndexFilename)
	w, err := index.NewWriterWithOptions(ctx, filename, index.WriterOptions{FormatVersion: index.FormatV4})
	require.NoError(t, err)
	symbols := []string{"", "job", "value", "test"}
	for i := 0; i < 300; i++ {
		symbols = append(symbols, fmt.Sprintf("value-%04d", i))
	}
	slices.Sort(symbols)
	for _, s := range symbols {
		require.NoError(t, w.AddSymbol(s))
	}
	require.NoError(t, w.AddSeries(0, labels.FromStrings("job", "test", "value", "")))
	for i := 0; i < 300; i++ {
		require.NoError(t, w.AddSeries(storage.SeriesRef(i+1), labels.FromStrings("job", "test", "value", fmt.Sprintf("value-%04d", i))))
	}
	require.NoError(t, w.Close())
	raw, err := os.ReadFile(filename)
	require.NoError(t, err)
	bkt := objstore.NewInMemBucket()
	require.NoError(t, bkt.Upload(ctx, id.String()+"/index", bytes.NewReader(raw)))
	require.NoError(t, BuildAndWriteSparseHeaderFromTSDBIndex(ctx, id, source, 32, log.NewNopLogger()))
	require.NoFileExists(t, filepath.Join(dir, block.SparseIndexHeaderFilename))
	require.NoError(t, os.WriteFile(filepath.Join(dir, block.SparseIndexHeaderFilename), []byte("obsolete"), 0600))
	require.NoError(t, BuildAndWriteSparseHeaderFromTSDBIndex(ctx, id, source, 32, log.NewNopLogger()))
	require.NoFileExists(t, filepath.Join(dir, block.SparseIndexHeaderFilename))
	// An old or corrupt sidecar must not override the embedded V4 directory.
	sparse := []byte("obsolete sparse sidecar")
	for _, staleSidecar := range []bool{false, true} {
		if staleSidecar {
			require.NoError(t, bkt.Upload(ctx, id.String()+"/"+block.SparseIndexHeaderFilename, bytes.NewReader(sparse)))
		}
		for _, version := range []int{BinaryFormatV1, BinaryFormatV2} {
			t.Run(fmt.Sprintf("header-%d/staleSidecar-%v", version, staleSidecar), func(t *testing.T) {
				local := t.TempDir()
				for pass := 0; pass < 2; pass++ {
					r, err := NewStreamBinaryReader(ctx, id, objstore.WithNoopInstr(bkt), local, Config{Version: version, VerifyOnLoad: true}, 32, log.NewNopLogger(), NewStreamBinaryReaderMetrics(nil))
					require.NoError(t, err)
					gotVersion, err := r.IndexVersion(ctx)
					require.NoError(t, err)
					require.Equal(t, index.FormatV4, gotVersion)
					names, err := r.LabelNames(ctx)
					require.NoError(t, err)
					require.Equal(t, []string{"job", "value"}, names)
					for i, s := range symbols {
						got, err := r.LookupSymbol(ctx, uint32(i))
						require.NoError(t, err)
						require.Equal(t, s, got)
					}
					toc, err := index.NewTOCFromByteSlice(realByteSlice(raw))
					require.NoError(t, err)
					require.NoError(t, index.ReadPostingsOffsetTable(realByteSlice(raw), toc.PostingsTable, func(name, value []byte, offset uint64, _ int) error {
						got, err := r.PostingsOffset(ctx, string(name), string(value))
						require.NoError(t, err)
						require.Equal(t, index.Range{Start: int64(offset) + 4, End: int64(offset) + 4 + int64(binary.BigEndian.Uint32(raw[offset:]))}, got)
						return nil
					}))
					values, err := r.LabelValuesOffsets(ctx, "value", "value-02", nil)
					require.NoError(t, err)
					require.Len(t, values, 100)
					for i, v := range values {
						require.Equal(t, fmt.Sprintf("value-%04d", 200+i), v.LabelValue)
					}
					_, err = r.PostingsOffset(ctx, "missing", "value")
					require.ErrorIs(t, err, NotFoundRangeErr)
					_, err = r.PostingsOffset(ctx, "value", "value-9999")
					require.ErrorIs(t, err, NotFoundRangeErr)
					filtered, err := r.LabelValuesOffsets(ctx, "value", "value-02", func(v string) bool { return v == "value-0250" })
					require.NoError(t, err)
					require.Len(t, filtered, 1)
					require.Equal(t, "value-0250", filtered[0].LabelValue)
					require.NoError(t, r.Close())
				}
				require.NoFileExists(t, filepath.Join(local, id.String(), block.SparseIndexHeaderFilename))
				// Switching header formats reuses the existing V1 symbols section.
				if version == BinaryFormatV1 {
					migrated, err := NewStreamBinaryReader(ctx, id, objstore.WithNoopInstr(bkt), local, Config{Version: BinaryFormatV2, VerifyOnLoad: true}, 32, log.NewNopLogger(), NewStreamBinaryReaderMetrics(nil))
					require.NoError(t, err)
					require.True(t, migrated.symbolsTOC.v4SymbolsValidated)
					got, err := migrated.LookupSymbol(ctx, 1)
					require.NoError(t, err)
					require.Equal(t, symbols[1], got)
					_, err = migrated.PostingsOffset(ctx, "value", "value-0250")
					require.NoError(t, err)
					require.NoError(t, migrated.Close())
				}
			})
		}
	}
}

func TestV4IndexHeaderRejectsCorruption(t *testing.T) {
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "index")
	w, err := index.NewWriterWithOptions(ctx, path, index.WriterOptions{FormatVersion: index.FormatV4})
	require.NoError(t, err)
	require.NoError(t, w.AddSymbol("a"))
	require.NoError(t, w.AddSymbol("b"))
	require.NoError(t, w.AddSeries(0, labels.FromStrings("a", "b")))
	require.NoError(t, w.Close())
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	toc, err := index.NewTOCFromByteSlice(realByteSlice(raw))
	require.NoError(t, err)
	directory := binary.BigEndian.Uint64(raw[toc.PostingsTable+8:])
	symbolDirectory := toc.Symbols + 8 + binary.BigEndian.Uint64(raw[toc.Symbols:]) + 4
	tests := []struct {
		name      string
		mutate    func([]byte)
		openError bool
	}{
		{"symbol-directory-checksum", func(b []byte) { b[symbolDirectory+4] ^= 1 }, true},
		{"header-checksum", func(b []byte) { b[toc.PostingsTable+24] ^= 1 }, true},
		{"directory-checksum", func(b []byte) { b[directory] ^= 1 }, true},
		{"directory-bounds", func(b []byte) {
			binary.BigEndian.PutUint64(b[toc.PostingsTable+8:], ^uint64(0))
			binary.BigEndian.PutUint32(b[toc.PostingsTable+24:], crc32.Checksum(b[toc.PostingsTable:toc.PostingsTable+24], castagnoliTable))
		}, true},
		{"old-directory-format", func(b []byte) {
			copy(b[toc.PostingsTable:], "PPOT0001")
			binary.BigEndian.PutUint32(b[toc.PostingsTable+24:], crc32.Checksum(b[toc.PostingsTable:toc.PostingsTable+24], castagnoliTable))
		}, true},
		{"directory-postings-bound", func(b []byte) {
			size := binary.BigEndian.Uint64(b[toc.PostingsTable+16:])
			d := encoding.Decbuf{B: b[directory : directory+size-4]}
			_ = d.Be32()
			_ = d.UvarintStr()
			_ = d.UvarintStr()
			_ = d.Be32()
			_ = d.Be64()
			_ = d.Be32()
			_ = d.Be32()
			at := directory + size - 4 - uint64(d.Len())
			binary.BigEndian.PutUint64(b[at:], toc.PostingsTable)
			binary.BigEndian.PutUint32(b[directory+size-4:], crc32.Checksum(b[directory:directory+size-4], castagnoliTable))
		}, true},
		{"page-checksum", func(b []byte) { b[toc.PostingsTable+28] ^= 1 }, false},
	}
	for _, tt := range tests {
		for _, version := range []int{BinaryFormatV1, BinaryFormatV2} {
			t.Run(fmt.Sprintf("%s/header-%d", tt.name, version), func(t *testing.T) {
				damaged := slices.Clone(raw)
				tt.mutate(damaged)
				bkt := objstore.NewInMemBucket()
				id := ulid.MustNew(1, nil)
				require.NoError(t, bkt.Upload(ctx, id.String()+"/index", bytes.NewReader(damaged)))
				r, err := NewStreamBinaryReader(ctx, id, objstore.WithNoopInstr(bkt), t.TempDir(), Config{Version: version}, 32, log.NewNopLogger(), NewStreamBinaryReaderMetrics(nil))
				if tt.openError {
					switch tt.name {
					case "symbol-directory-checksum":
						require.ErrorIs(t, err, streamencoding.ErrInvalidChecksum)
					case "header-checksum", "old-directory-format":
						require.ErrorContains(t, err, "invalid paged postings header")
					case "directory-checksum":
						require.ErrorIs(t, err, encoding.ErrInvalidChecksum)
					case "directory-postings-bound":
						require.ErrorContains(t, err, "invalid postings page bounds")
					case "directory-bounds":
						require.ErrorIs(t, err, encoding.ErrInvalidSize)
					}
					return
				}
				require.NoError(t, err)
				_, err = r.PostingsOffset(ctx, "", "")
				require.ErrorIs(t, err, encoding.ErrInvalidChecksum)
				require.NoError(t, r.Close())
			})
		}
	}
}

func TestV4EmptyIndexHeader(t *testing.T) {
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "index")
	w, err := index.NewWriterWithOptions(ctx, path, index.WriterOptions{FormatVersion: index.FormatV4})
	require.NoError(t, err)
	require.NoError(t, w.Close())
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	bkt := objstore.NewInMemBucket()
	id := ulid.MustNew(1, nil)
	require.NoError(t, bkt.Upload(ctx, id.String()+"/index", bytes.NewReader(raw)))
	for _, version := range []int{BinaryFormatV1, BinaryFormatV2} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			r, err := NewStreamBinaryReader(ctx, id, objstore.WithNoopInstr(bkt), t.TempDir(), Config{Version: version}, 32, log.NewNopLogger(), NewStreamBinaryReaderMetrics(nil))
			require.NoError(t, err)
			names, err := r.LabelNames(ctx)
			require.NoError(t, err)
			require.Empty(t, names)
			values, err := r.LabelValuesOffsets(ctx, "missing", "", nil)
			require.NoError(t, err)
			require.Empty(t, values)
			_, err = r.PostingsOffset(ctx, "missing", "")
			require.ErrorIs(t, err, NotFoundRangeErr)
			require.NoError(t, r.Close())
		})
	}
}
