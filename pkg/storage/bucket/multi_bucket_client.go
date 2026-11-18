// SPDX-License-Identifier: AGPL-3.0-only

package bucket

import (
	"context"
	"errors"
	"io"
	"maps"
	"slices"

	"github.com/thanos-io/objstore"
)

// MultiBucketClient reads from a primary bucket and a secondary bucket, and writes only to the primary
// bucket. A read tries the primary bucket first. If the primary bucket doesn't have the object, the
// read uses the secondary bucket. A listing returns the union of the two buckets. A delete removes the
// object from the two buckets.
type MultiBucketClient struct {
	primary   objstore.InstrumentedBucket
	secondary objstore.InstrumentedBucket

	primaryReader   objstore.Bucket
	secondaryReader objstore.Bucket

	// A "not found" error from these buckets doesn't increment the operation failures metric, because
	// the object can be in the other bucket.
	primaryMissExpected   objstore.Bucket
	secondaryMissExpected objstore.Bucket
}

// NewMultiBucketClient returns a new MultiBucketClient.
func NewMultiBucketClient(primary, secondary objstore.InstrumentedBucket) *MultiBucketClient {
	return newMultiBucketClient(primary, secondary, nil)
}

func newMultiBucketClient(primary, secondary objstore.InstrumentedBucket, isOpFailureExpected objstore.IsOpFailureExpectedFunc) *MultiBucketClient {
	return &MultiBucketClient{
		primary:               primary,
		secondary:             secondary,
		primaryReader:         withExpectedErrs(primary, isOpFailureExpected, nil),
		secondaryReader:       withExpectedErrs(secondary, isOpFailureExpected, nil),
		primaryMissExpected:   withExpectedErrs(primary, isOpFailureExpected, primary.IsObjNotFoundErr),
		secondaryMissExpected: withExpectedErrs(secondary, isOpFailureExpected, secondary.IsObjNotFoundErr),
	}
}

// withExpectedErrs returns bkt with the errors of first and second marked as expected. A nil function
// marks no errors.
func withExpectedErrs(bkt objstore.InstrumentedBucket, first, second objstore.IsOpFailureExpectedFunc) objstore.Bucket {
	switch {
	case first == nil && second == nil:
		return bkt
	case first == nil:
		return bkt.WithExpectedErrs(second)
	case second == nil:
		return bkt.WithExpectedErrs(first)
	}
	return bkt.WithExpectedErrs(func(err error) bool { return first(err) || second(err) })
}

// Close implements io.Closer.
func (b *MultiBucketClient) Close() error {
	return errors.Join(b.primary.Close(), b.secondary.Close())
}

// Provider returns the provider of the primary bucket.
func (b *MultiBucketClient) Provider() objstore.ObjProvider {
	return b.primary.Provider()
}

// Name returns the name of the primary bucket.
func (b *MultiBucketClient) Name() string {
	return b.primary.Name()
}

// Upload writes the object into the primary bucket. Conditional upload options apply only to the
// primary bucket.
func (b *MultiBucketClient) Upload(ctx context.Context, name string, r io.Reader, opts ...objstore.ObjectUploadOption) error {
	return b.primaryReader.Upload(ctx, name, r, opts...)
}

// SupportedObjectUploadOptions returns the upload options of the primary bucket.
func (b *MultiBucketClient) SupportedObjectUploadOptions() []objstore.ObjectUploadOptionType {
	return b.primary.SupportedObjectUploadOptions()
}

// Delete removes the object from the two buckets. It returns an error that is not "not found" from
// either bucket, also when the delete in the other bucket succeeded: a copy of the object that stays in
// the secondary bucket becomes visible again to reads. It returns a "not found" error only when the two
// buckets return "not found".
func (b *MultiBucketClient) Delete(ctx context.Context, name string) error {
	primaryErr := b.primaryMissExpected.Delete(ctx, name)
	secondaryErr := b.secondaryMissExpected.Delete(ctx, name)

	primaryMissing := primaryErr != nil && b.primary.IsObjNotFoundErr(primaryErr)
	secondaryMissing := secondaryErr != nil && b.secondary.IsObjNotFoundErr(secondaryErr)

	switch {
	case primaryErr != nil && !primaryMissing:
		return primaryErr
	case secondaryErr != nil && !secondaryMissing:
		return secondaryErr
	case primaryMissing && secondaryMissing:
		return primaryErr
	}
	return nil
}

// Iter calls f for each entry in the given directory of the two buckets, in lexicographic order. An
// entry that is in the two buckets is given to f once.
func (b *MultiBucketClient) Iter(ctx context.Context, dir string, f func(string) error, options ...objstore.IterOption) error {
	return iterUnion(
		func(fn func(string) error) error { return b.primaryReader.Iter(ctx, dir, fn, options...) },
		func(fn func(string) error) error { return b.secondaryReader.Iter(ctx, dir, fn, options...) },
		func(name string) string { return name },
		f,
	)
}

// IterWithAttributes is like Iter, and also gives the requested object attributes to f. For an entry
// that is in the two buckets, f gets the attributes from the primary bucket.
func (b *MultiBucketClient) IterWithAttributes(ctx context.Context, dir string, f func(objstore.IterObjectAttributes) error, options ...objstore.IterOption) error {
	return iterUnion(
		func(fn func(objstore.IterObjectAttributes) error) error {
			return b.primaryReader.IterWithAttributes(ctx, dir, fn, options...)
		},
		func(fn func(objstore.IterObjectAttributes) error) error {
			return b.secondaryReader.IterWithAttributes(ctx, dir, fn, options...)
		},
		func(attrs objstore.IterObjectAttributes) string { return attrs.Name },
		f,
	)
}

// iterUnion calls f once for each name in the primary and the secondary listing, in lexicographic
// order of the names. When a name is in the two listings, f gets the entry of the primary listing.
// The providers don't all list in the same order, so the two listings are collected before f is called.
func iterUnion[T any](iterPrimary, iterSecondary func(func(T) error) error, nameOf func(T) string, f func(T) error) error {
	entries := map[string]T{}
	collect := func(entry T) error {
		if _, ok := entries[nameOf(entry)]; !ok {
			entries[nameOf(entry)] = entry
		}
		return nil
	}
	if err := iterPrimary(collect); err != nil {
		return err
	}
	if err := iterSecondary(collect); err != nil {
		return err
	}

	for _, name := range slices.Sorted(maps.Keys(entries)) {
		if err := f(entries[name]); err != nil {
			return err
		}
	}
	return nil
}

// SupportedIterOptions returns the iter options that the two buckets support.
func (b *MultiBucketClient) SupportedIterOptions() []objstore.IterOptionType {
	secondaryOptions := b.secondary.SupportedIterOptions()
	return slices.DeleteFunc(slices.Clone(b.primary.SupportedIterOptions()), func(option objstore.IterOptionType) bool {
		return !slices.Contains(secondaryOptions, option)
	})
}

// Get returns a reader for the object from the primary bucket, or from the secondary bucket if the
// primary bucket doesn't have it.
func (b *MultiBucketClient) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	reader, err := b.primaryMissExpected.Get(ctx, name)
	if err == nil || !b.primary.IsObjNotFoundErr(err) {
		return reader, err
	}
	return b.secondaryReader.Get(ctx, name)
}

// GetRange returns a range reader for the object from the primary bucket, or from the secondary bucket
// if the primary bucket doesn't have it.
func (b *MultiBucketClient) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	reader, err := b.primaryMissExpected.GetRange(ctx, name, off, length)
	if err == nil || !b.primary.IsObjNotFoundErr(err) {
		return reader, err
	}
	return b.secondaryReader.GetRange(ctx, name, off, length)
}

// Exists returns true if the object is in the primary bucket or in the secondary bucket.
func (b *MultiBucketClient) Exists(ctx context.Context, name string) (bool, error) {
	exists, err := b.primaryReader.Exists(ctx, name)
	if err != nil || exists {
		return exists, err
	}
	return b.secondaryReader.Exists(ctx, name)
}

// Attributes returns the attributes of the object from the primary bucket, or from the secondary bucket
// if the primary bucket doesn't have it.
func (b *MultiBucketClient) Attributes(ctx context.Context, name string) (objstore.ObjectAttributes, error) {
	attrs, err := b.primaryMissExpected.Attributes(ctx, name)
	if err == nil || !b.primary.IsObjNotFoundErr(err) {
		return attrs, err
	}
	return b.secondaryReader.Attributes(ctx, name)
}

// IsObjNotFoundErr returns true if either bucket identifies err as "not found".
func (b *MultiBucketClient) IsObjNotFoundErr(err error) bool {
	return b.primary.IsObjNotFoundErr(err) || b.secondary.IsObjNotFoundErr(err)
}

// IsAccessDeniedErr returns true if either bucket identifies err as "access denied".
func (b *MultiBucketClient) IsAccessDeniedErr(err error) bool {
	return b.primary.IsAccessDeniedErr(err) || b.secondary.IsAccessDeniedErr(err)
}

// IsConditionNotMetErr returns true if either bucket identifies err as "condition not met".
func (b *MultiBucketClient) IsConditionNotMetErr(err error) bool {
	return b.primary.IsConditionNotMetErr(err) || b.secondary.IsConditionNotMetErr(err)
}

// ReaderWithExpectedErrs allows to specify a filter that marks certain errors as expected, so it will not increment
// thanos_objstore_bucket_operation_failures_total metric.
func (b *MultiBucketClient) ReaderWithExpectedErrs(fn objstore.IsOpFailureExpectedFunc) objstore.BucketReader {
	return b.WithExpectedErrs(fn)
}

// WithExpectedErrs allows to specify a filter that marks certain errors as expected, so it will not increment
// thanos_objstore_bucket_operation_failures_total metric.
func (b *MultiBucketClient) WithExpectedErrs(fn objstore.IsOpFailureExpectedFunc) objstore.Bucket {
	return newMultiBucketClient(b.primary, b.secondary, fn)
}
