// SPDX-License-Identifier: AGPL-3.0-only

package objtools

import (
	"context"
	"flag"
	"fmt"
	"io"
	"strconv"

	"cloud.google.com/go/storage"
	"github.com/grafana/dskit/cancellation"
	"github.com/pkg/errors"
	"google.golang.org/api/iterator"
)

var errUploadTerminated = cancellation.NewErrorf("upload terminated")

type GCSClientConfig struct {
	BucketName string
}

func (c *GCSClientConfig) RegisterFlags(prefix string, f *flag.FlagSet) {
	f.StringVar(&c.BucketName, prefix+"bucket-name", "", "The name of the GCS bucket (not prefixed by a scheme).")
}

func (c *GCSClientConfig) Validate(prefix string) error {
	if c.BucketName == "" {
		return fmt.Errorf("the GCS bucket name provided in (%s) is required", prefix+"bucket-name")
	}
	return nil
}

func (c *GCSClientConfig) ToBucket(ctx context.Context) (Bucket, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create GCS storage client")
	}
	return &gcsBucket{
		client: *client.Bucket(c.BucketName),
		name:   c.BucketName,
	}, nil
}

type gcsBucket struct {
	client storage.BucketHandle
	name   string
}

func (bkt *gcsBucket) Get(ctx context.Context, objectName string, options GetOptions) (io.ReadCloser, error) {
	obj, err := bkt.objectHandle(objectName, options.VersionID)
	if err != nil {
		return nil, err
	}
	return obj.NewReader(ctx)
}

func (bkt *gcsBucket) ServerSideCopy(ctx context.Context, objectName string, dstBucket Bucket, options CopyOptions) error {
	d, ok := dstBucket.(*gcsBucket)
	if !ok {
		return errors.New("destination Bucket wasn't a GCS Bucket")
	}
	srcObj, err := bkt.objectHandle(objectName, options.SourceVersionID)
	if err != nil {
		return err
	}

	dstObject := d.client.Object(options.destinationObjectName(objectName))
	copier := dstObject.CopierFrom(srcObj)
	_, err = copier.Run(ctx)
	return err
}

func (bkt *gcsBucket) ClientSideCopy(ctx context.Context, objectName string, dstBucket Bucket, options CopyOptions) error {
	srcObj, err := bkt.objectHandle(objectName, options.SourceVersionID)
	if err != nil {
		return err
	}
	reader, err := srcObj.NewReader(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to get GCS source object reader")
	}
	if err := dstBucket.Upload(ctx, options.destinationObjectName(objectName), reader, reader.Attrs.Size); err != nil {
		_ = reader.Close()
		return errors.Wrap(err, "failed to upload GCS source object to destination")
	}
	return errors.Wrap(reader.Close(), "failed closing GCS source object reader")
}

func (bkt *gcsBucket) List(ctx context.Context, options ListOptions) (*ListResult, error) {
	prefix := ensureDelimiterSuffix(options.Prefix)

	q := &storage.Query{
		Prefix:   prefix,
		Versions: options.Versioned,
	}
	var attributes []string
	if options.Versioned {
		attributes = []string{"Name", "Size", "Updated", "Generation", "Deleted"}
	} else {
		attributes = []string{"Name", "Size", "Updated"}
	}
	err := q.SetAttrSelection(attributes) // only fields we care about
	if err != nil {
		return nil, err
	}

	var prefixes []string
	if !options.Recursive {
		q.Delimiter = Delim
		prefixes = make([]string, 0, 10)
	}

	objects := make([]ObjectAttributes, 0, 10)
	it := bkt.client.Objects(ctx, q)
	for {
		obj, err := it.Next()

		if errors.Is(err, iterator.Done) {
			break
		}

		if err != nil {
			return nil, errors.Wrapf(err, "listPrefix: error listing %v", prefix)
		}

		if obj.Prefix != "" { // synthetic directory, only returned when recursive=false
			prefixes = append(prefixes, obj.Prefix)
		} else if options.Versioned {
			objects = append(objects, ObjectAttributes{
				Name:         obj.Name,
				Size:         obj.Size,
				LastModified: obj.Updated,
				VersionInfo: VersionInfo{
					VersionID: generationToString(obj.Generation),
					IsCurrent: obj.Deleted.IsZero(),
				}})
		} else {
			objects = append(objects, ObjectAttributes{Name: obj.Name, Size: obj.Size, LastModified: obj.Updated})
		}
	}

	return &ListResult{objects, prefixes}, nil
}

func (bkt *gcsBucket) RestoreVersion(ctx context.Context, objectName string, versionInfo VersionInfo) error {
	// Docs: https://cloud.google.com/storage/docs/using-versioned-objects#restore
	return bkt.ServerSideCopy(ctx, objectName, bkt, CopyOptions{
		SourceVersionID: versionInfo.VersionID,
	})
}

func (bkt *gcsBucket) Upload(ctx context.Context, objectName string, reader io.Reader, contentLength int64) error {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(errUploadTerminated)

	obj := bkt.client.Object(objectName)
	w := obj.NewWriter(ctx)
	n, err := io.Copy(w, reader)
	if err != nil {
		return errors.Wrap(err, "failed during copy stage of GCS upload")
	}
	if n != contentLength {
		return errors.Wrapf(err, "unexpected content length from copy: expected=%d, actual=%d", contentLength, n)
	}
	return w.Close()
}

func (bkt *gcsBucket) Delete(ctx context.Context, objectName string, options DeleteOptions) error {
	obj, err := bkt.objectHandle(objectName, options.VersionID)
	if err != nil {
		return err
	}
	return obj.Delete(ctx)
}

func (bkt *gcsBucket) Name() string {
	return bkt.name
}

func (bkt *gcsBucket) objectHandle(objectName string, versionID string) (*storage.ObjectHandle, error) {
	obj := bkt.client.Object(objectName)
	if versionID == "" {
		return obj, nil
	}
	generation, err := stringToGeneration(versionID)
	if err != nil {
		return nil, err
	}
	return obj.Generation(generation), nil
}

func stringToGeneration(versionID string) (int64, error) {
	if versionID == "" {
		return 0, errors.New("no versionID was provided")
	}
	return strconv.ParseInt(versionID, 10, 64)
}

func generationToString(generation int64) string {
	return strconv.FormatInt(generation, 10)
}
