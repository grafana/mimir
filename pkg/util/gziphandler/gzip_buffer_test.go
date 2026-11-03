// SPDX-License-Identifier: AGPL-3.0-only

package gziphandler

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGzipResponseWriter(t *testing.T) {
	t.Run("large write", func(t *testing.T) {
		body := bytes.Repeat([]byte("a"), 1<<20)
		// The first 128 bytes look like text, but sniffing 512 bytes detects binary.
		body[400] = 0
		for _, tc := range []struct {
			name         string
			prefix       int
			minSize      int
			contentType  string
			contentTypes []parsedContentType
			encoding     string
			wantGzip     bool
		}{
			{name: "single write", minSize: DefaultMinSize, wantGzip: true},
			{name: "buffered prefix", prefix: 37, minSize: DefaultMinSize, wantGzip: true},
			{name: "sniff beyond small threshold", prefix: 37, minSize: 128, contentTypes: []parsedContentType{{mediaType: "application/octet-stream"}}, wantGzip: true},
			{name: "explicit content type", minSize: DefaultMinSize, contentType: "application/json", contentTypes: []parsedContentType{{mediaType: "application/json"}}, wantGzip: true},
			{name: "rejected content type", minSize: DefaultMinSize, contentType: "text/plain", contentTypes: []parsedContentType{{mediaType: "application/json"}}},
			{name: "rejected inferred content type", prefix: 37, minSize: DefaultMinSize, contentTypes: []parsedContentType{{mediaType: "application/json"}}},
			{name: "already encoded", minSize: DefaultMinSize, encoding: "br"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				rec := httptest.NewRecorder()
				w := newBufferTestWriter(rec, tc.minSize)
				w.contentTypes = tc.contentTypes
				if tc.contentType != "" {
					rec.Header().Set(contentType, tc.contentType)
				}
				if tc.encoding != "" {
					rec.Header().Set(contentEncoding, tc.encoding)
				}
				if tc.prefix > 0 {
					n, err := w.Write(body[:tc.prefix])
					require.NoError(t, err)
					require.Equal(t, tc.prefix, n)
					require.LessOrEqual(t, len(w.buf), max(tc.minSize, sniffLen))
				}
				n, err := w.Write(body[tc.prefix:])
				require.NoError(t, err)
				require.Equal(t, len(body)-tc.prefix, n)
				require.Nil(t, w.buf, "the initial buffer must be released before Close")
				require.NoError(t, w.Close())
				encoded := bytes.Clone(rec.Body.Bytes())
				require.NoError(t, w.Close())
				require.Equal(t, encoded, rec.Body.Bytes(), "Close must not replay the initial buffer")
				if tc.contentType != "" {
					require.Equal(t, tc.contentType, rec.Header().Get(contentType), "an explicit Content-Type must be kept")
				}
				if tc.wantGzip {
					require.Equal(t, "gzip", rec.Header().Get(contentEncoding))
					reader, err := gzip.NewReader(bytes.NewReader(encoded))
					require.NoError(t, err)
					decoded, err := io.ReadAll(reader)
					require.NoError(t, err)
					require.NoError(t, reader.Close())
					require.Equal(t, body, decoded)
				} else {
					require.Equal(t, tc.encoding, rec.Header().Get(contentEncoding))
					require.Equal(t, body, encoded)
				}
			})
		}
	})

	t.Run("large write allocations", func(t *testing.T) {
		body := bytes.Repeat([]byte("a"), 16<<20)
		for _, encoding := range []string{"", "br"} {
			for _, prefix := range []int{0, 37} {
				t.Run(fmt.Sprintf("encoding %q prefix %d", encoding, prefix), func(t *testing.T) {
					rec := &discardResponseWriter{header: make(http.Header)}
					w := newBufferTestWriter(rec, DefaultMinSize)
					if prefix > 0 {
						n, err := w.Write(body[:prefix])
						require.NoError(t, err)
						require.Equal(t, prefix, n)
					}
					// Set the encoding after the prefix so that the large write selects plain output.
					if encoding != "" {
						rec.header.Set(contentEncoding, encoding)
					}
					var before, after runtime.MemStats
					runtime.ReadMemStats(&before)
					n, err := w.Write(body[prefix:])
					runtime.ReadMemStats(&after)
					require.NoError(t, err)
					require.Equal(t, len(body)-prefix, n)
					require.NoError(t, w.Close())
					// Leave room for a gzip writer allocated by an empty pool, but not for a copy of the body.
					require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(len(body)/4), "the write must not copy the body")
				})
			}
		}
	})

	t.Run("threshold", func(t *testing.T) {
		for _, tc := range []struct {
			name            string
			minSize         int
			size            int
			rejectsIdentity bool
		}{
			{name: "minimum 0 body 1", minSize: 0, size: 1},
			{name: "minimum 128 body 127", minSize: 128, size: 127},
			{name: "minimum 128 body 128", minSize: 128, size: 128},
			{name: "minimum 128 body 129", minSize: 128, size: 129},
			{name: "minimum 1400 body 1399", minSize: DefaultMinSize, size: DefaultMinSize - 1},
			{name: "minimum 1400 body 1400", minSize: DefaultMinSize, size: DefaultMinSize},
			{name: "minimum 1400 body 1401", minSize: DefaultMinSize, size: DefaultMinSize + 1},
			// The identity rejection overrides the size threshold.
			{name: "identity rejected", minSize: DefaultMinSize, size: 1, rejectsIdentity: true},
		} {
			t.Run(tc.name, func(t *testing.T) {
				rec := httptest.NewRecorder()
				w := newBufferTestWriter(rec, tc.minSize)
				w.rejectsIdentity = tc.rejectsIdentity
				for i := range tc.size {
					n, err := w.Write([]byte("a"))
					require.NoError(t, err)
					require.Equal(t, 1, n)
					require.Equal(t, tc.rejectsIdentity || i+1 >= tc.minSize, w.gw != nil, "compression must start at the configured threshold")
				}
				require.NoError(t, w.Close())
				if tc.rejectsIdentity || tc.size >= tc.minSize {
					require.Equal(t, "gzip", rec.Header().Get(contentEncoding))
				} else {
					require.Empty(t, rec.Header().Get(contentEncoding))
					require.Equal(t, bytes.Repeat([]byte("a"), tc.size), rec.Body.Bytes())
				}
			})
		}
	})

	t.Run("flush", func(t *testing.T) {
		rec := httptest.NewRecorder()
		w := newBufferTestWriter(rec, DefaultMinSize)
		body := bytes.Repeat([]byte("a"), DefaultMinSize+100)
		n, err := w.Write(body)
		require.NoError(t, err)
		require.Equal(t, len(body), n)
		w.Flush()
		require.True(t, rec.Flushed)
		n, err = w.Write(body)
		require.NoError(t, err)
		require.Equal(t, len(body), n)
		require.NoError(t, w.Close())
		reader, err := gzip.NewReader(rec.Body)
		require.NoError(t, err)
		decoded, err := io.ReadAll(reader)
		require.NoError(t, err)
		require.NoError(t, reader.Close())
		require.Equal(t, bytes.Repeat(body, 2), decoded)
	})

	t.Run("owns buffered input", func(t *testing.T) {
		rec := httptest.NewRecorder()
		// math.MaxInt keeps the write buffered until Close, so the output shows whether Write copied its input.
		w := newBufferTestWriter(rec, math.MaxInt)
		body := make([]byte, 1<<20)
		body[0] = 'a'
		n, err := w.Write(body[:1])
		require.NoError(t, err)
		require.Equal(t, 1, n)
		require.Less(t, cap(w.buf), 1024, "a tiny write must not preallocate minSize or retain its input's backing array")
		body[0] = 'b'
		require.NoError(t, w.Close())
		require.Equal(t, "a", rec.Body.String())
	})

	// Each trigger makes an empty write select gzip.
	emptyWriteTriggers := []struct {
		name            string
		minSize         int
		contentLength   bool // Declare a Content-Length of at least minSize.
		rejectsIdentity bool
	}{
		{name: "content length", minSize: DefaultMinSize, contentLength: true},
		{name: "identity rejected", minSize: DefaultMinSize, rejectsIdentity: true},
		{name: "no minimum size", minSize: 0},
	}

	t.Run("empty write", func(t *testing.T) {
		// As for a HEAD or 304 response, the headers claim gzip, but no gzip stream is written.
		for _, tc := range emptyWriteTriggers {
			t.Run(tc.name, func(t *testing.T) {
				rec := httptest.NewRecorder()
				w := newBufferTestWriter(rec, tc.minSize)
				w.rejectsIdentity = tc.rejectsIdentity
				if tc.contentLength {
					rec.Header().Set(contentLength, strconv.Itoa(tc.minSize))
				}
				w.WriteHeader(http.StatusNotModified)
				n, err := w.Write(nil)
				require.NoError(t, err)
				require.Zero(t, n)
				require.Nil(t, w.gw, "an empty write must not create a gzip stream")
				require.NoError(t, w.Close())
				require.NoError(t, w.Close())
				require.Equal(t, http.StatusNotModified, rec.Code)
				require.Equal(t, "gzip", rec.Header().Get(contentEncoding))
				require.Empty(t, rec.Body.Bytes())
			})
		}
	})

	t.Run("empty write before body", func(t *testing.T) {
		body := bytes.Repeat([]byte("a"), 2*DefaultMinSize)
		for _, tc := range emptyWriteTriggers {
			t.Run(tc.name, func(t *testing.T) {
				rec := httptest.NewRecorder()
				w := newBufferTestWriter(rec, tc.minSize)
				w.rejectsIdentity = tc.rejectsIdentity
				if tc.contentLength {
					rec.Header().Set(contentLength, strconv.Itoa(len(body)))
				}
				// The empty write selects GZIP and sets the headers.
				n, err := w.Write(nil)
				require.NoError(t, err)
				require.Zero(t, n)
				require.Nil(t, w.gw, "an empty write must not create a gzip stream")
				w.Flush()
				require.True(t, rec.Flushed, "the headers are decided, so Flush must reach the client")
				n, err = w.Write(body)
				require.NoError(t, err)
				require.Equal(t, len(body), n)
				require.NoError(t, w.Close())

				require.Equal(t, "gzip", rec.Header().Get(contentEncoding))
				require.Empty(t, rec.Header().Get(contentLength))
				reader, err := gzip.NewReader(rec.Body)
				require.NoError(t, err, "the body must be compressed")
				decoded, err := io.ReadAll(reader)
				require.NoError(t, err)
				require.NoError(t, reader.Close())
				require.Equal(t, body, decoded)
			})
		}
	})

	t.Run("status set after empty write", func(t *testing.T) {
		// The status reaches the client whether or not a body follows.
		body := bytes.Repeat([]byte("a"), 2*DefaultMinSize)
		for _, tc := range emptyWriteTriggers {
			t.Run(tc.name, func(t *testing.T) {
				for _, seq := range []struct {
					name  string
					flush bool
					body  []byte
				}{
					{name: "no body"},
					{name: "body", body: body},
					{name: "flush before body", flush: true, body: body},
				} {
					t.Run(seq.name, func(t *testing.T) {
						rec := httptest.NewRecorder()
						w := newBufferTestWriter(rec, tc.minSize)
						w.rejectsIdentity = tc.rejectsIdentity
						if tc.contentLength {
							rec.Header().Set(contentLength, strconv.Itoa(len(body)))
						}
						n, err := w.Write(nil)
						require.NoError(t, err)
						require.Zero(t, n)
						w.WriteHeader(http.StatusNotFound)
						if seq.flush {
							w.Flush()
						}
						if seq.body != nil {
							n, err = w.Write(seq.body)
							require.NoError(t, err)
							require.Equal(t, len(seq.body), n)
						}
						require.NoError(t, w.Close())

						require.Equal(t, http.StatusNotFound, rec.Code)
						require.Equal(t, "gzip", rec.Header().Get(contentEncoding))
						if seq.body == nil {
							require.Empty(t, rec.Body.Bytes())
							return
						}
						reader, err := gzip.NewReader(rec.Body)
						require.NoError(t, err, "the body must be compressed")
						decoded, err := io.ReadAll(reader)
						require.NoError(t, err)
						require.NoError(t, reader.Close())
						require.Equal(t, seq.body, decoded)
					})
				}
			})
		}
	})

	t.Run("plain write errors", func(t *testing.T) {
		writeErr := errors.New("response write failed")
		// The response accepts limit bytes in total. Its first write is the buffer (any earlier prefix plus the start
		// of this write, DefaultMinSize bytes in total), and its second is the remainder. wantN counts only this call's bytes.
		for _, tc := range []struct {
			name     string
			prefix   int
			limit    int
			writeErr error
			wantN    int
			wantErr  error
			writes   int
		}{
			{name: "prefix fails", limit: 3, writeErr: writeErr, wantN: 3, wantErr: writeErr, writes: 1},
			{name: "previously acknowledged bytes", prefix: 37, limit: 40, writeErr: writeErr, wantN: 3, wantErr: writeErr, writes: 1},
			{name: "previous prefix fails", prefix: 37, limit: 3, writeErr: writeErr, wantErr: writeErr, writes: 1},
			{name: "short prefix write", prefix: 37, limit: 40, wantN: 3, wantErr: io.ErrShortWrite, writes: 1},
			{name: "remainder fails", prefix: 37, limit: DefaultMinSize + 19, writeErr: writeErr, wantN: DefaultMinSize + 19 - 37, wantErr: writeErr, writes: 2},
			{name: "short remainder write", limit: DefaultMinSize + 19, wantN: DefaultMinSize + 19, wantErr: io.ErrShortWrite, writes: 2},
		} {
			t.Run(tc.name, func(t *testing.T) {
				rec := &limitedResponseWriter{header: make(http.Header), limit: tc.limit, err: tc.writeErr}
				w := newBufferTestWriter(rec, DefaultMinSize)
				if tc.prefix > 0 {
					n, err := w.Write(bytes.Repeat([]byte("a"), tc.prefix))
					require.NoError(t, err)
					require.Equal(t, tc.prefix, n)
				}
				// Set the encoding after the prefix so that the large write selects plain output.
				rec.Header().Set(contentEncoding, "br")
				n, err := w.Write(make([]byte, 1<<20))
				require.Equal(t, tc.wantN, n)
				require.ErrorIs(t, err, tc.wantErr)
				require.Equal(t, tc.writes, rec.writes)
				require.Nil(t, w.buf)
				require.NoError(t, w.Close())
				require.Equal(t, tc.writes, rec.writes, "a failed prefix must not be replayed")
			})
		}
	})

	t.Run("gzip write errors", func(t *testing.T) {
		writeErr := errors.New("response write failed")
		// With a limit of 0, writing the gzip header fails. With a limit of 10, the header fits exactly,
		// and the failure happens while the remainder is compressed.
		for _, limit := range []int{0, 10} {
			t.Run(fmt.Sprintf("output limit %d", limit), func(t *testing.T) {
				rec := &limitedResponseWriter{header: make(http.Header), limit: limit, err: writeErr}
				w := newBufferTestWriter(rec, DefaultMinSize)
				prefix := bytes.Repeat([]byte("a"), 37)
				n, err := w.Write(prefix)
				require.NoError(t, err)
				require.Equal(t, len(prefix), n)
				body := make([]byte, 1<<20)
				// Incompressible input makes the compressor write output during Write, not only at Close.
				random := rand.New(rand.NewPCG(1, 2))
				for i := range body {
					body[i] = byte(random.Uint32())
				}
				n, err = w.Write(body)
				require.ErrorIs(t, err, writeErr)
				require.GreaterOrEqual(t, n, 0)
				require.LessOrEqual(t, n, len(body))
				if limit == 0 {
					require.Zero(t, n)
					require.Equal(t, 1, rec.writes, "do not stream the remainder after the prefix failed")
				} else {
					require.GreaterOrEqual(t, n, DefaultMinSize-len(prefix))
					require.Greater(t, rec.writes, 1, "the error must occur while streaming the remainder")
				}
				require.Nil(t, w.buf)
				writes := rec.writes
				require.ErrorIs(t, w.Close(), writeErr)
				require.NoError(t, w.Close())
				require.Equal(t, writes, rec.writes)
			})
		}
	})
}

func BenchmarkGzipResponseWriterLargeWrite(b *testing.B) {
	for _, size := range []int{1 << 20, 16 << 20} {
		body := bytes.Repeat([]byte("a"), size)
		for _, prefix := range []int{0, 37} {
			b.Run(fmt.Sprintf("bytes %d prefix %d", size, prefix), func(b *testing.B) {
				rec := &discardResponseWriter{header: make(http.Header)}
				b.ReportAllocs()
				b.SetBytes(int64(size))
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					clear(rec.header)
					w := newBufferTestWriter(rec, DefaultMinSize)
					if prefix > 0 {
						n, err := w.Write(body[:prefix])
						require.NoError(b, err)
						require.Equal(b, prefix, n)
					}
					n, err := w.Write(body[prefix:])
					require.NoError(b, err)
					require.Equal(b, size-prefix, n)
					require.NoError(b, w.Close())
				}
			})
		}
	}
}

func newBufferTestWriter(w http.ResponseWriter, minSize int) *GzipResponseWriter {
	return &GzipResponseWriter{ResponseWriter: w, index: poolIndex(gzip.DefaultCompression), minSize: minSize}
}

// limitedResponseWriter accepts limit bytes in total. A write beyond that returns a partial count with err,
// or a short write without an error if err is nil.
type limitedResponseWriter struct {
	header  http.Header
	limit   int
	err     error
	written int
	writes  int
}

func (w *limitedResponseWriter) Header() http.Header { return w.header }
func (*limitedResponseWriter) WriteHeader(int)       {}
func (w *limitedResponseWriter) Write(b []byte) (int, error) {
	w.writes++
	n := min(len(b), max(0, w.limit-w.written))
	w.written += n
	if n < len(b) {
		return n, w.err
	}
	return n, nil
}

// discardResponseWriter drops the body, so allocation measurements exclude the response itself.
type discardResponseWriter struct{ header http.Header }

func (w *discardResponseWriter) Header() http.Header { return w.header }
func (*discardResponseWriter) WriteHeader(int)       {}
func (*discardResponseWriter) Write(b []byte) (int, error) {
	return len(b), nil
}
