// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"io"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/pkg/errors"
	"google.golang.org/api/iterator"
)

type gcsBucket struct {
	storage.BucketHandle
	name string
}

func newGCSBucket(client *storage.Client, name string) bucket {
	return &gcsBucket{
		BucketHandle: *client.Bucket(name),
		name:         name,
	}
}

func (bkt *gcsBucket) Get(ctx context.Context, objectName string) (io.ReadCloser, error) {
	obj := bkt.Object(objectName)
	r, err := obj.NewReader(ctx)
	if err != nil {
		return nil, err
	}
	return r, nil
}

func (bkt *gcsBucket) Copy(ctx context.Context, objectName string, dstBucket bucket) error {
	d, ok := dstBucket.(*gcsBucket)
	if !ok {
		return errors.New("destination bucket wasn't a gcs bucket")
	}
	srcObj := bkt.Object(objectName)
	dstObject := d.BucketHandle.Object(objectName)
	copier := dstObject.CopierFrom(srcObj)
	_, err := copier.Run(ctx)
	return err
}

func (bkt *gcsBucket) ListPrefix(ctx context.Context, prefix string, recursive bool) ([]string, error) {
	if len(prefix) > 0 && prefix[len(prefix)-1:] != delim {
		prefix = prefix + delim
	}

	q := &storage.Query{
		Prefix: prefix,
	}
	if !recursive {
		q.Delimiter = delim
	}

	var result []string

	it := bkt.Objects(ctx, q)
	for {
		obj, err := it.Next()

		if errors.Is(err, iterator.Done) {
			break
		}

		if err != nil {
			return nil, errors.Wrapf(err, "listPrefix: error listing %v", prefix)
		}

		path := ""
		if obj.Prefix != "" { // synthetic directory, only returned when recursive=false
			path = obj.Prefix
		} else {
			path = obj.Name
		}

		if strings.HasPrefix(path, prefix) {
			path = strings.TrimPrefix(path, prefix)
		} else {
			return nil, errors.Errorf("listPrefix: path has invalid prefix: %v, expected prefix: %v", path, prefix)
		}

		result = append(result, path)
	}

	return result, nil
}

func (bkt *gcsBucket) UploadMarkerFile(ctx context.Context, objectName string) error {
	obj := bkt.Object(objectName)
	w := obj.NewWriter(ctx)
	return w.Close()
}

func (bkt *gcsBucket) Name() string {
	return bkt.name
}
