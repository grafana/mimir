// SPDX-License-Identifier: AGPL-3.0-only

// Package ioctx provides context-aware I/O operations.
package ioctx

import (
	"context"
	"errors"
	"io"
	"os"

	"go.uber.org/atomic"
)

// Open opens a file, respecting context cancellation.
// If the context is cancelled before the function has returned, the file is
// eventually closed in the background.
//
// Useful for opening FIFOs, which may block.
func Open(ctx context.Context, path string, flag int) (*os.File, error) {
	return open(os.OpenFile, ctx, path, flag)
}

type openFunc[F any] func(path string, flag int, perm os.FileMode) (F, error)

func open[F io.Closer](openFile openFunc[F], ctx context.Context, path string, flag int) (file F, err error) {
	if err := ctx.Err(); err != nil {
		return file, err
	}

	type result struct {
		f   F
		err error
	}
	ch := make(chan result, 1)
	go func() {
		f, err := openFile(path, flag, 0)
		ch <- result{f, err}
	}()

	select {
	case r := <-ch:
		return r.f, r.err
	case <-ctx.Done():
		// If the open eventually completes, close the file to avoid leaking it.
		go func() {
			r := <-ch
			if r.err == nil {
				r.f.Close()
			}
		}()
		return file, ctx.Err()
	}
}

// Read reads from r, respecting context cancellation.
// If the context is cancelled while reading, r is closed to unblock the
// operation. r is always closed before Read returns.
func Read(ctx context.Context, r io.ReadCloser, p []byte) (int, error) {
	return readWrite(ctx, r, p, r.Read)
}

// Write writes data to w, respecting context cancellation.
// If the context is cancelled while writing, w is closed to unblock the
// operation.
func Write(ctx context.Context, w io.WriteCloser, data []byte) (int, error) {
	return readWrite(ctx, w, data, w.Write)
}

func readWrite(ctx context.Context, c io.Closer, p []byte, do func([]byte) (int, error)) (int, error) {
	if err := ctx.Err(); err != nil {
		_ = c.Close()
		return 0, err
	}

	type result struct {
		n   int
		err error
	}

	ch := make(chan result, 1)
	go func() {
		n, err := do(p)
		ch <- result{n: n, err: err}
	}()

	select {
	case res := <-ch:
		return res.n, res.err
	case <-ctx.Done():
		err := c.Close()
		<-ch
		return 0, errors.Join(err, ctx.Err())
	}
}

// ReadAll is like [io.ReadAll], but respects context cancellation.
// If the context is cancelled while reading, r is closed to unblock the
// operation. r is always closed before ReadAll returns.
func ReadAll(ctx context.Context, r io.ReadCloser) (data []byte, err error) {
	r = &readCloseOnce{ReadCloser: r}
	defer func() { err = errors.Join(err, r.Close()) }()

	data = make([]byte, 0, 2048)

	for {
		window := data[len(data):cap(data)]
		n, err := Read(ctx, r, window)
		data = data[:len(data)+n]
		if err != nil {
			if errors.Is(err, io.EOF) {
				return data, nil
			}
			return data, err
		}

		// Grow if we're out of space for more reads.
		if cap(data)-len(data) == 0 {
			data = append(data, 0)[:len(data)]
		}
	}
}

type readCloseOnce struct {
	io.ReadCloser
	closed atomic.Bool
}

func (c *readCloseOnce) Close() error {
	if c.closed.CompareAndSwap(false, true) {
		return c.ReadCloser.Close()
	}
	return nil
}
