// SPDX-License-Identifier: AGPL-3.0-only

package bucket

import (
	"bytes"
	"context"
	"errors"
	"io"
	"slices"
	"sort"
	"strings"
	"sync"

	"github.com/oklog/ulid/v2"
	"github.com/thanos-io/objstore"
)

// These names mirror the ones in pkg/storage/tsdb, pkg/storage/tsdb/block and pkg/storage/tsdb/bucketindex,
// which can't be imported because they import this package.
const (
	blockMetaFilename          = "meta.json"
	markersDirname             = "markers"
	tenantDeletionMarkFilename = "tenant-deletion-mark.json"
	bucketIndexFilename        = "bucket-index.json.gz"
)

type objectKind int

const (
	// otherObject is an object that doesn't belong to a block, such as the cluster seed file.
	otherObject objectKind = iota
	// blockObject is an object in a block directory that isn't a block mark, or the block directory itself.
	blockObject
	// blockMarkObject is a mark of a block, in the block directory or in the tenant's markers directory.
	blockMarkObject
	tenantDeletionMarkObject
	bucketIndexObject
)

// parseObjectName returns the kind of the object with the given name and, for block objects and block marks,
// the tenant and the block ID.
func parseObjectName(name string) (kind objectKind, tenantID, blockID string) {
	parts := strings.SplitN(name, objstore.DirDelim, 3)
	if len(parts) < 2 {
		return otherObject, "", ""
	}
	tenantID = parts[0]

	if isULID(parts[1]) {
		if len(parts) == 3 && parts[2] != blockMetaFilename && strings.HasSuffix(parts[2], ".json") && !strings.Contains(parts[2], objstore.DirDelim) {
			return blockMarkObject, tenantID, parts[1]
		}
		return blockObject, tenantID, parts[1]
	}

	switch {
	case len(parts) == 2 && parts[1] == bucketIndexFilename:
		return bucketIndexObject, tenantID, ""
	case len(parts) == 3 && parts[1] == markersDirname && parts[2] == tenantDeletionMarkFilename:
		return tenantDeletionMarkObject, tenantID, ""
	case len(parts) == 3 && parts[1] == markersDirname:
		// Global block marks are named "<block>-<mark filename>".
		if id, _, ok := strings.Cut(parts[2], "-"); ok && isULID(id) {
			return blockMarkObject, tenantID, id
		}
	}
	return otherObject, tenantID, ""
}

func isULID(s string) bool {
	_, err := ulid.Parse(s)
	return err == nil
}

// isTenantDir returns whether dir is a tenant directory, such as "<tenant>/".
func isTenantDir(dir string) bool {
	return strings.HasSuffix(dir, objstore.DirDelim) && strings.Count(dir, objstore.DirDelim) == 1 && len(dir) > 1
}

// blockLocations maps tenants and their blocks to the index of the bucket that holds the block.
type blockLocations struct {
	mu      sync.RWMutex
	tenants map[string]map[string]int
}

func (l *blockLocations) get(tenantID, blockID string) (int, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	idx, ok := l.tenants[tenantID][blockID]
	return idx, ok
}

func (l *blockLocations) set(tenantID, blockID string, idx int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	blocks := l.tenants[tenantID]
	if blocks == nil {
		blocks = map[string]int{}
		l.tenants[tenantID] = blocks
	}
	blocks[blockID] = idx
}

func (l *blockLocations) replace(tenantID string, blocks map[string]int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(blocks) == 0 {
		delete(l.tenants, tenantID)
		return
	}
	l.tenants[tenantID] = blocks
}

// BlockLocationBucket is a bucket client over one write bucket and zero or more read buckets. Each bucket is
// self-contained: it holds its blocks, the marks of these blocks, its own bucket index and its own tenant
// deletion marks.
//
// Calls on an object of a block go to the bucket that holds the block. The location of a block comes from
// the bucket indexes (SetTenantBlockLocations), from the listing of a tenant directory, or from trying the
// buckets in order. A block with an unknown location is assumed to be in the write bucket, so new blocks are
// uploaded to the write bucket.
//
// Other objects are handled as follows:
//   - The bucket index is only read from and written to the write bucket. The bucket index of each bucket
//     must be accessed through Buckets.
//   - Tenant deletion marks are written to all buckets.
//   - Other objects, such as the cluster seed file, are read from the first bucket that has them and written
//     to the write bucket.
//   - Listings of directories outside of a block, such as the bucket root or a tenant directory, return the
//     union of all buckets.
type BlockLocationBucket struct {
	names     []string
	buckets   []objstore.Bucket
	expected  objstore.IsOpFailureExpectedFunc
	locations *blockLocations
}

// NewBlockLocationBucket returns a new BlockLocationBucket. The first bucket is the write bucket, the
// others are read buckets. names are the bucket names, in the same order as buckets.
func NewBlockLocationBucket(names []string, buckets []objstore.Bucket) *BlockLocationBucket {
	return &BlockLocationBucket{
		names:     names,
		buckets:   buckets,
		locations: &blockLocations{tenants: map[string]map[string]int{}},
	}
}

// Buckets returns the write bucket followed by the read buckets.
func (b *BlockLocationBucket) Buckets() []objstore.Bucket {
	return b.buckets
}

// BucketNames returns the names of the buckets, in the same order as Buckets.
func (b *BlockLocationBucket) BucketNames() []string {
	return b.names
}

// SetTenantBlockLocations replaces the known block locations of a tenant. locations maps each block to the
// index of its bucket in Buckets.
func (b *BlockLocationBucket) SetTenantBlockLocations(tenantID string, locations map[ulid.ULID]int) {
	blocks := make(map[string]int, len(locations))
	for id, idx := range locations {
		// Blocks with an unknown location are assumed to be in the write bucket.
		if idx > 0 {
			blocks[id.String()] = idx
		}
	}
	b.locations.replace(tenantID, blocks)
}

// Buckets returns the buckets of bkt if it is a *BlockLocationBucket, or bkt otherwise.
func Buckets(bkt objstore.Bucket) []objstore.Bucket {
	if lb, ok := bkt.(*BlockLocationBucket); ok {
		return lb.Buckets()
	}
	return []objstore.Bucket{bkt}
}

// MapBuckets returns a copy of bkt with f applied to each of its buckets if bkt is a *BlockLocationBucket,
// or f applied to bkt otherwise. f gets the index and the name of each bucket. The name is empty if bkt isn't
// a *BlockLocationBucket.
func MapBuckets(bkt objstore.Bucket, f func(idx int, name string, bkt objstore.Bucket) (objstore.Bucket, error)) (objstore.Bucket, error) {
	lb, ok := bkt.(*BlockLocationBucket)
	if !ok {
		return f(0, "", bkt)
	}

	buckets := make([]objstore.Bucket, len(lb.buckets))
	for i, b := range lb.buckets {
		var err error
		if buckets[i], err = f(i, lb.names[i], b); err != nil {
			return nil, err
		}
	}
	return NewBlockLocationBucket(lb.names, buckets), nil
}

// bucketAt returns the bucket with the given index, with the expected errors applied. If notFoundExpected is
// true, a "not found" error is expected too, because another bucket is tried next.
func (b *BlockLocationBucket) bucketAt(idx int, notFoundExpected bool) objstore.Bucket {
	bkt := b.buckets[idx]
	expected := b.expected
	if notFoundExpected {
		if callerExpected := expected; callerExpected != nil {
			expected = func(err error) bool { return callerExpected(err) || bkt.IsObjNotFoundErr(err) }
		} else {
			expected = bkt.IsObjNotFoundErr
		}
	}
	if expected == nil {
		return bkt
	}
	if ib, ok := bkt.(objstore.InstrumentedBucket); ok {
		return ib.WithExpectedErrs(expected)
	}
	return bkt
}

func (b *BlockLocationBucket) isLast(idx int) bool {
	return idx == len(b.buckets)-1
}

// readFromBuckets calls read on each bucket in order, until it returns an error other than "not found". It
// returns the index of the last bucket that it called read on.
func (b *BlockLocationBucket) readFromBuckets(read func(objstore.Bucket) error) (int, error) {
	var err error
	for i := range b.buckets {
		if err = read(b.bucketAt(i, !b.isLast(i))); err == nil || !b.buckets[i].IsObjNotFoundErr(err) {
			return i, err
		}
	}
	return len(b.buckets) - 1, err
}

// read calls read on the bucket that holds the object with the given name.
func (b *BlockLocationBucket) read(name string, read func(objstore.Bucket) error) error {
	kind, tenantID, blockID := parseObjectName(name)
	switch kind {
	case bucketIndexObject:
		return read(b.bucketAt(0, false))
	case blockObject, blockMarkObject:
		if idx, ok := b.locations.get(tenantID, blockID); ok {
			return read(b.bucketAt(idx, false))
		}
		idx, err := b.readFromBuckets(read)
		if err == nil && idx > 0 {
			b.locations.set(tenantID, blockID, idx)
		}
		return err
	default:
		_, err := b.readFromBuckets(read)
		return err
	}
}

// blockBucket returns the index of the bucket that holds a block: the known location, otherwise the first
// bucket that has the block's meta.json, otherwise the write bucket.
func (b *BlockLocationBucket) blockBucket(ctx context.Context, tenantID, blockID string) (int, error) {
	if idx, ok := b.locations.get(tenantID, blockID); ok {
		return idx, nil
	}
	if len(b.buckets) == 1 {
		return 0, nil
	}

	metaName := tenantID + objstore.DirDelim + blockID + objstore.DirDelim + blockMetaFilename
	for i := range b.buckets {
		ok, err := b.bucketAt(i, false).Exists(ctx, metaName)
		if err != nil {
			return 0, err
		}
		if ok {
			b.locations.set(tenantID, blockID, i)
			return i, nil
		}
	}
	return 0, nil
}

// Get implements objstore.Bucket.
func (b *BlockLocationBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	var r io.ReadCloser
	err := b.read(name, func(bkt objstore.Bucket) (err error) {
		r, err = bkt.Get(ctx, name)
		return err
	})
	return r, err
}

// GetRange implements objstore.Bucket.
func (b *BlockLocationBucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	var r io.ReadCloser
	err := b.read(name, func(bkt objstore.Bucket) (err error) {
		r, err = bkt.GetRange(ctx, name, off, length)
		return err
	})
	return r, err
}

// Attributes implements objstore.Bucket.
func (b *BlockLocationBucket) Attributes(ctx context.Context, name string) (objstore.ObjectAttributes, error) {
	var attrs objstore.ObjectAttributes
	err := b.read(name, func(bkt objstore.Bucket) (err error) {
		attrs, err = bkt.Attributes(ctx, name)
		return err
	})
	return attrs, err
}

// Exists implements objstore.Bucket.
func (b *BlockLocationBucket) Exists(ctx context.Context, name string) (bool, error) {
	kind, tenantID, blockID := parseObjectName(name)
	switch kind {
	case bucketIndexObject:
		return b.bucketAt(0, false).Exists(ctx, name)
	case blockObject, blockMarkObject:
		if idx, ok := b.locations.get(tenantID, blockID); ok {
			return b.bucketAt(idx, false).Exists(ctx, name)
		}
	}

	for i := range b.buckets {
		ok, err := b.bucketAt(i, false).Exists(ctx, name)
		if err != nil || ok {
			if ok && i > 0 && blockID != "" {
				b.locations.set(tenantID, blockID, i)
			}
			return ok, err
		}
	}
	return false, nil
}

// Upload implements objstore.Bucket.
func (b *BlockLocationBucket) Upload(ctx context.Context, name string, r io.Reader, opts ...objstore.ObjectUploadOption) error {
	kind, tenantID, blockID := parseObjectName(name)
	switch kind {
	case blockObject:
		idx, _ := b.locations.get(tenantID, blockID)
		return b.bucketAt(idx, false).Upload(ctx, name, r, opts...)
	case blockMarkObject:
		// A mark must be in the same bucket as its block.
		idx, err := b.blockBucket(ctx, tenantID, blockID)
		if err != nil {
			return err
		}
		return b.bucketAt(idx, false).Upload(ctx, name, r, opts...)
	case tenantDeletionMarkObject:
		body, err := io.ReadAll(r)
		if err != nil {
			return err
		}
		for i := range b.buckets {
			if err := b.bucketAt(i, false).Upload(ctx, name, bytes.NewReader(body), opts...); err != nil {
				return err
			}
		}
		return nil
	default:
		return b.bucketAt(0, false).Upload(ctx, name, r, opts...)
	}
}

// Delete implements objstore.Bucket. An object that may be in several buckets is deleted from all of them.
// It returns a "not found" error only if no bucket had the object.
func (b *BlockLocationBucket) Delete(ctx context.Context, name string) error {
	kind, tenantID, blockID := parseObjectName(name)
	switch kind {
	case bucketIndexObject:
		return b.bucketAt(0, false).Delete(ctx, name)
	case blockObject, blockMarkObject:
		if idx, ok := b.locations.get(tenantID, blockID); ok {
			return b.bucketAt(idx, false).Delete(ctx, name)
		}
	}

	var notFoundErr error
	deleted := false
	for i := range b.buckets {
		err := b.bucketAt(i, !b.isLast(i)).Delete(ctx, name)
		switch {
		case err == nil:
			deleted = true
		case b.buckets[i].IsObjNotFoundErr(err):
			notFoundErr = err
		default:
			return err
		}
	}
	if deleted {
		return nil
	}
	return notFoundErr
}

// Iter implements objstore.Bucket.
func (b *BlockLocationBucket) Iter(ctx context.Context, dir string, f func(string) error, options ...objstore.IterOption) error {
	return iterBuckets(b, dir, func(bkt objstore.Bucket, f func(string) error) error {
		return bkt.Iter(ctx, dir, f, options...)
	}, func(name string) string { return name }, f)
}

// IterWithAttributes implements objstore.Bucket.
func (b *BlockLocationBucket) IterWithAttributes(ctx context.Context, dir string, f func(objstore.IterObjectAttributes) error, options ...objstore.IterOption) error {
	return iterBuckets(b, dir, func(bkt objstore.Bucket, f func(objstore.IterObjectAttributes) error) error {
		return bkt.IterWithAttributes(ctx, dir, f, options...)
	}, func(attrs objstore.IterObjectAttributes) string { return attrs.Name }, f)
}

// iterBuckets lists a block directory in the bucket that holds the block, and any other directory in all
// buckets. In the latter case, an entry that is in several buckets is passed to f once, from the first
// bucket. Listing a tenant directory updates the tenant's known block locations.
func iterBuckets[T any](b *BlockLocationBucket, dir string, iter func(objstore.Bucket, func(T) error) error, nameOf func(T) string, f func(T) error) error {
	kind, tenantID, blockID := parseObjectName(dir)
	if kind == blockObject || kind == blockMarkObject {
		if idx, ok := b.locations.get(tenantID, blockID); ok {
			return iter(b.bucketAt(idx, false), f)
		}

		// Find the first bucket that has objects in the directory.
		for i := range b.buckets {
			var entries []T
			if err := iter(b.bucketAt(i, false), func(e T) error {
				entries = append(entries, e)
				return nil
			}); err != nil {
				return err
			}
			if len(entries) == 0 {
				continue
			}
			if i > 0 {
				b.locations.set(tenantID, blockID, i)
			}
			for _, e := range entries {
				if err := f(e); err != nil {
					return err
				}
			}
			return nil
		}
		return nil
	}

	owners := map[string]int{}
	var entries []T
	for i := range b.buckets {
		if err := iter(b.bucketAt(i, false), func(e T) error {
			if _, ok := owners[nameOf(e)]; !ok {
				owners[nameOf(e)] = i
				entries = append(entries, e)
			}
			return nil
		}); err != nil {
			return err
		}
	}
	sort.Slice(entries, func(i, j int) bool { return nameOf(entries[i]) < nameOf(entries[j]) })

	if isTenantDir(dir) {
		blocks := map[string]int{}
		for name, idx := range owners {
			if kind, _, blockID := parseObjectName(name); kind == blockObject && idx > 0 {
				blocks[blockID] = idx
			}
		}
		b.locations.replace(strings.TrimSuffix(dir, objstore.DirDelim), blocks)
	}

	for _, e := range entries {
		if err := f(e); err != nil {
			return err
		}
	}
	return nil
}

// SupportedIterOptions implements objstore.Bucket. It returns the options that all buckets support.
func (b *BlockLocationBucket) SupportedIterOptions() []objstore.IterOptionType {
	supported := b.buckets[0].SupportedIterOptions()
	for _, bkt := range b.buckets[1:] {
		other := bkt.SupportedIterOptions()
		supported = slices.DeleteFunc(slices.Clone(supported), func(o objstore.IterOptionType) bool {
			return !slices.Contains(other, o)
		})
	}
	return supported
}

// SupportedObjectUploadOptions implements objstore.Bucket.
func (b *BlockLocationBucket) SupportedObjectUploadOptions() []objstore.ObjectUploadOptionType {
	return b.buckets[0].SupportedObjectUploadOptions()
}

// IsObjNotFoundErr implements objstore.Bucket.
func (b *BlockLocationBucket) IsObjNotFoundErr(err error) bool {
	return slices.ContainsFunc(b.buckets, func(bkt objstore.Bucket) bool { return bkt.IsObjNotFoundErr(err) })
}

// IsAccessDeniedErr implements objstore.Bucket.
func (b *BlockLocationBucket) IsAccessDeniedErr(err error) bool {
	return slices.ContainsFunc(b.buckets, func(bkt objstore.Bucket) bool { return bkt.IsAccessDeniedErr(err) })
}

// IsConditionNotMetErr implements objstore.Bucket.
func (b *BlockLocationBucket) IsConditionNotMetErr(err error) bool {
	return slices.ContainsFunc(b.buckets, func(bkt objstore.Bucket) bool { return bkt.IsConditionNotMetErr(err) })
}

// Provider implements objstore.Bucket.
func (b *BlockLocationBucket) Provider() objstore.ObjProvider {
	return b.buckets[0].Provider()
}

// Name implements objstore.Bucket. It returns the name of the write bucket.
func (b *BlockLocationBucket) Name() string {
	return b.buckets[0].Name()
}

// Close implements objstore.Bucket.
func (b *BlockLocationBucket) Close() error {
	errs := make([]error, 0, len(b.buckets))
	for _, bkt := range b.buckets {
		errs = append(errs, bkt.Close())
	}
	return errors.Join(errs...)
}

// ReaderWithExpectedErrs implements objstore.InstrumentedBucket.
func (b *BlockLocationBucket) ReaderWithExpectedErrs(fn objstore.IsOpFailureExpectedFunc) objstore.BucketReader {
	return b.WithExpectedErrs(fn)
}

// WithExpectedErrs implements objstore.InstrumentedBucket. The returned bucket shares the block locations
// with b.
func (b *BlockLocationBucket) WithExpectedErrs(fn objstore.IsOpFailureExpectedFunc) objstore.Bucket {
	return &BlockLocationBucket{
		names:     b.names,
		buckets:   b.buckets,
		expected:  fn,
		locations: b.locations,
	}
}
