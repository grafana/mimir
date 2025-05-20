// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package opentelemetry

import (
	"context"
	"io"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/thanos-io/objstore"
)

// TracingBucket is a wrapper around objstore.Bucket that adds tracing to all operations using OpenTelemetry.
type TracingBucket struct {
	tracer trace.Tracer
	bkt    objstore.Bucket
}

func WrapWithTraces(bkt objstore.Bucket, tracer trace.Tracer) objstore.InstrumentedBucket {
	return TracingBucket{tracer: tracer, bkt: bkt}
}

func (t TracingBucket) Provider() objstore.ObjProvider { return t.bkt.Provider() }

func (t TracingBucket) Iter(ctx context.Context, dir string, f func(string) error, options ...objstore.IterOption) (err error) {
	ctx, span := t.tracer.Start(ctx, "bucket_iter")
	defer span.End()
	span.SetAttributes(attribute.String("dir", dir))

	defer func() {
		if err != nil {
			span.RecordError(err)
		}
	}()
	return t.bkt.Iter(ctx, dir, f, options...)
}

func (t TracingBucket) IterWithAttributes(ctx context.Context, dir string, f func(attrs objstore.IterObjectAttributes) error, options ...objstore.IterOption) (err error) {
	ctx, span := t.tracer.Start(ctx, "bucket_iter_with_attrs")
	defer span.End()
	span.SetAttributes(attribute.String("dir", dir))

	defer func() {
		if err != nil {
			span.RecordError(err)
		}
	}()
	return t.bkt.IterWithAttributes(ctx, dir, f, options...)
}

// SupportedIterOptions returns a list of supported IterOptions by the underlying provider.
func (t TracingBucket) SupportedIterOptions() []objstore.IterOptionType {
	return t.bkt.SupportedIterOptions()
}

func (t TracingBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	ctx, span := t.tracer.Start(ctx, "bucket_get")
	defer span.End()
	span.SetAttributes(attribute.String("name", name))

	r, err := t.bkt.Get(ctx, name)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	return newTracingReadCloser(r, span), nil
}

func (t TracingBucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	ctx, span := t.tracer.Start(ctx, "bucket_getrange")
	defer span.End()
	span.SetAttributes(attribute.String("name", name), attribute.Int64("offset", off), attribute.Int64("length", length))

	r, err := t.bkt.GetRange(ctx, name, off, length)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	return newTracingReadCloser(r, span), nil
}

func (t TracingBucket) Exists(ctx context.Context, name string) (_ bool, err error) {
	ctx, span := t.tracer.Start(ctx, "bucket_exists")
	defer span.End()
	span.SetAttributes(attribute.String("name", name))

	defer func() {
		if err != nil {
			span.RecordError(err)
		}
	}()
	return t.bkt.Exists(ctx, name)
}

func (t TracingBucket) Attributes(ctx context.Context, name string) (_ objstore.ObjectAttributes, err error) {
	ctx, span := t.tracer.Start(ctx, "bucket_attributes")
	defer span.End()
	span.SetAttributes(attribute.String("name", name))

	defer func() {
		if err != nil {
			span.RecordError(err)
		}
	}()
	return t.bkt.Attributes(ctx, name)
}

func (t TracingBucket) Upload(ctx context.Context, name string, r io.Reader) (err error) {
	ctx, span := t.tracer.Start(ctx, "bucket_upload")
	defer span.End()
	span.SetAttributes(attribute.String("name", name))

	defer func() {
		if err != nil {
			span.RecordError(err)
		}
	}()
	return t.bkt.Upload(ctx, name, r)
}

func (t TracingBucket) Delete(ctx context.Context, name string) (err error) {
	ctx, span := t.tracer.Start(ctx, "bucket_delete")
	defer span.End()
	span.SetAttributes(attribute.String("name", name))

	defer func() {
		if err != nil {
			span.RecordError(err)
		}
	}()
	return t.bkt.Delete(ctx, name)
}

func (t TracingBucket) Name() string {
	return "tracing: " + t.bkt.Name()
}

func (t TracingBucket) Close() error {
	return t.bkt.Close()
}

func (t TracingBucket) IsObjNotFoundErr(err error) bool {
	return t.bkt.IsObjNotFoundErr(err)
}

func (t TracingBucket) IsAccessDeniedErr(err error) bool {
	return t.bkt.IsAccessDeniedErr(err)
}

func (t TracingBucket) WithExpectedErrs(expectedFunc objstore.IsOpFailureExpectedFunc) objstore.Bucket {
	if ib, ok := t.bkt.(objstore.InstrumentedBucket); ok {
		return TracingBucket{tracer: t.tracer, bkt: ib.WithExpectedErrs(expectedFunc)}
	}
	return t
}

func (t TracingBucket) ReaderWithExpectedErrs(expectedFunc objstore.IsOpFailureExpectedFunc) objstore.BucketReader {
	return t.WithExpectedErrs(expectedFunc)
}

type tracingReadCloser struct {
	r io.ReadCloser
	s trace.Span

	objSize    int64
	objSizeErr error

	read int
}

func newTracingReadCloser(r io.ReadCloser, span trace.Span) io.ReadCloser {
	// Since TryToGetSize can only reliably return size before doing any read calls,
	// we call during "construction" and remember the results.
	objSize, objSizeErr := objstore.TryToGetSize(r)

	return &tracingReadCloser{r: r, s: span, objSize: objSize, objSizeErr: objSizeErr}
}

func (t *tracingReadCloser) ObjectSize() (int64, error) {
	return t.objSize, t.objSizeErr
}

func (t *tracingReadCloser) Read(p []byte) (int, error) {
	n, err := t.r.Read(p)
	if n > 0 {
		t.read += n
	}
	if err != nil && err != io.EOF && t.s != nil {
		t.s.RecordError(err)
	}
	return n, err
}

func (t *tracingReadCloser) Close() error {
	err := t.r.Close()
	if t.s != nil {
		t.s.SetAttributes(attribute.Int64("read", int64(t.read)))
		if err != nil {
			t.s.SetAttributes(attribute.String("close_err", err.Error()))
		}
		t.s.End()
		t.s = nil
	}
	return err
}
