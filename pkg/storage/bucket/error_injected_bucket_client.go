// SPDX-License-Identifier: AGPL-3.0-only

package bucket

import (
	"context"
	"io"

	"github.com/thanos-io/objstore"
)

type Operation uint8

const (
	OpGet = 1 << iota
	OpExists
	OpUpload
	OpDelete
	OpIter
	OpAttributes
)

func InjectErrorOn(mask Operation, target string, err error) func(Operation, string) error {
	return func(op Operation, tar string) error {
		if (op&mask) == op && tar == target {
			return err
		}
		return nil
	}
}

// ErrorInjectedBucketClient facilitates injecting errors into a bucket client
type ErrorInjectedBucketClient struct {
	objstore.Bucket
	Injector func(Operation, string) error
}

func (b *ErrorInjectedBucketClient) injectError(op Operation, name string) error {
	if b.Injector == nil {
		return nil
	}
	return b.Injector(op, name)
}

func (b *ErrorInjectedBucketClient) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	if err := b.injectError(OpGet, name); err != nil {
		return nil, err
	}
	return b.Bucket.Get(ctx, name)
}

func (b *ErrorInjectedBucketClient) Exists(ctx context.Context, name string) (bool, error) {
	if err := b.injectError(OpExists, name); err != nil {
		return false, err
	}
	return b.Bucket.Exists(ctx, name)
}

func (b *ErrorInjectedBucketClient) Upload(ctx context.Context, name string, r io.Reader) error {
	if err := b.injectError(OpUpload, name); err != nil {
		return err
	}
	return b.Bucket.Upload(ctx, name, r)
}

func (b *ErrorInjectedBucketClient) Delete(ctx context.Context, name string) error {
	if err := b.injectError(OpDelete, name); err != nil {
		return err
	}
	return b.Bucket.Delete(ctx, name)
}

func (b *ErrorInjectedBucketClient) Iter(ctx context.Context, dir string, f func(string) error, options ...objstore.IterOption) error {
	if err := b.injectError(OpIter, dir); err != nil {
		return err
	}
	return b.Bucket.Iter(ctx, dir, f, options...)
}

func (b *ErrorInjectedBucketClient) Attributes(ctx context.Context, name string) (objstore.ObjectAttributes, error) {
	if err := b.injectError(OpAttributes, name); err != nil {
		return objstore.ObjectAttributes{}, err
	}
	return b.Bucket.Attributes(ctx, name)
}
