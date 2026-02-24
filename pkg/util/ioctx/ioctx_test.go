// SPDX-License-Identifier: AGPL-3.0-only

package ioctx

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRead(t *testing.T) {
	t.Run("reads data and keeps reader open when context is not cancelled", func(t *testing.T) {
		t.Parallel()

		r := newBlockingReadWriter()

		data := []byte("test")
		go func() { (<-r.reqs) <- data }()

		got := make([]byte, len(data))
		_, err := Read(t.Context(), r, got)
		require.ErrorIs(t, err, io.EOF)
		assert.Equal(t, data, got)
		assert.False(t, r.Closed())
	})

	t.Run("returns context error and closes reader when context is cancelled beforehand", func(t *testing.T) {
		t.Parallel()

		r := newBlockingReadWriter()

		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		got := make([]byte, 10)
		_, err := Read(ctx, r, got)
		require.ErrorIs(t, err, context.Canceled)
		assert.True(t, r.Closed())
	})

	t.Run("returns context error and closes reader when context is cancelled while reading", func(t *testing.T) {
		t.Parallel()

		r := newBlockingReadWriter()

		ctx, cancel := context.WithCancel(t.Context())

		go func() {
			<-r.reqs
			// Read is blocking waiting for data. Let's cancel the context to unblock.
			cancel()
		}()

		got := make([]byte, 10)
		_, err := Read(ctx, r, got)
		require.ErrorIs(t, err, context.Canceled)
		assert.True(t, r.Closed())
	})
}

func TestWrite(t *testing.T) {
	t.Run("writes data and keeps writer open when context is not cancelled", func(t *testing.T) {
		t.Parallel()

		r := newBlockingReadWriter()

		data := []byte("test")
		got := make(chan []byte, 1)
		go func() { req := <-r.reqs; got <- <-req }()

		_, err := Write(t.Context(), r, data)
		require.NoError(t, err)
		assert.Equal(t, data, <-got)
		assert.False(t, r.Closed())
	})

	t.Run("returns context error and closes reader when context is cancelled beforehand", func(t *testing.T) {
		t.Parallel()

		w := newBlockingReadWriter()

		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		_, err := Write(ctx, w, []byte("test"))
		require.ErrorIs(t, err, context.Canceled)
		assert.True(t, w.Closed())
	})

	t.Run("returns context error and closes reader when context is cancelled while reading", func(t *testing.T) {
		t.Parallel()

		w := newBlockingReadWriter()

		ctx, cancel := context.WithCancel(t.Context())

		go func() {
			<-w.reqs
			// Write is blocking. Let's cancel the context to unblock.
			cancel()
		}()

		_, err := Write(ctx, w, []byte("test"))
		require.ErrorIs(t, err, context.Canceled)
		assert.True(t, w.Closed())
	})
}

func TestOpen(t *testing.T) {
	t.Run("opens file and keeps it open when context is not cancelled", func(t *testing.T) {
		t.Parallel()

		o := newBlockingOpener()

		f := newCloser()
		go func() { (<-o.reqs) <- f }()

		got, err := open(o.Open, t.Context(), "foo", 0)
		require.NoError(t, err)
		assert.Equal(t, f, got)
		assert.False(t, f.Closed())
	})

	t.Run("returns context error and doesn't open file when context is cancelled beforehand", func(t *testing.T) {
		t.Parallel()

		o := newBlockingOpener()

		go func() {
			select {
			case <-o.reqs:
				t.Error("should not have tried to open file")
			case <-t.Context().Done():
			}
		}()

		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		got, err := open(o.Open, ctx, "foo", 0)
		require.ErrorIs(t, err, context.Canceled)
		assert.Nil(t, got)
	})

	t.Run("returns context error and eventually closes file when context is cancelled while opening", func(t *testing.T) {
		t.Parallel()

		o := newBlockingOpener()

		ctx, cancel := context.WithCancel(t.Context())

		f := newCloser()
		go func() {
			req := <-o.reqs
			cancel()
			req <- f
		}()

		got, err := open(o.Open, ctx, "foo", 0)
		require.ErrorIs(t, err, context.Canceled)
		assert.Nil(t, got)

		select {
		case <-f.closed:
		case <-time.After(1 * time.Second):
			t.Error("expected file to be closed")
		}
	})
}

func TestReadAll(t *testing.T) {
	t.Parallel()

	r := newBlockingReadWriter()

	foos := bytes.Repeat([]byte("foo"), 3000)
	go func() {
		// Force a growth by filling the full buffer first.
		(<-r.reqs) <- foos[:2048]
		(<-r.reqs) <- foos[2048 : 2048+100]
		close(<-r.reqs)
	}()

	expected := string(foos[:2048+100])

	got, err := ReadAll(t.Context(), r)
	require.NoError(t, err)
	assert.Equal(t, expected, string(got))
	assert.False(t, r.Closed())
}

type blockingReadWriter struct {
	reqs chan chan []byte
	closer
}

func newBlockingReadWriter() *blockingReadWriter {
	return &blockingReadWriter{
		reqs:   make(chan chan []byte),
		closer: newCloser(),
	}
}

func (r *blockingReadWriter) Read(p []byte) (int, error) {
	req := make(chan []byte)
	select {
	case <-r.closed:
		return 0, errors.New("closed")
	case r.reqs <- req:
		select {
		case <-r.closed:
			return 0, errors.New("closed")
		case data, ok := <-req:
			if !ok {
				return 0, io.EOF
			}
			n := copy(p, data)
			return n, nil
		}
	}
}

func (r *blockingReadWriter) Write(p []byte) (int, error) {
	req := make(chan []byte)
	select {
	case <-r.closed:
		return 0, errors.New("closed")
	case r.reqs <- req:
		select {
		case <-r.closed:
			return 0, errors.New("closed")
		case req <- p:
			return len(p), nil
		}
	}
}

type closer struct {
	closed chan struct{}
}

func newCloser() closer {
	return closer{closed: make(chan struct{})}
}

func (c closer) Close() error {
	close(c.closed)
	return nil
}

func (c closer) Closed() bool {
	select {
	case _, _ = <-c.closed:
		return true
	default:
		return false
	}
}

type blockingOpener struct {
	reqs chan chan io.Closer
}

func newBlockingOpener() blockingOpener {
	return blockingOpener{reqs: make(chan chan io.Closer)}
}

func (o blockingOpener) Open(path string, flag int, perm os.FileMode) (io.Closer, error) {
	req := make(chan io.Closer)
	o.reqs <- req
	return <-req, nil
}
