// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"io"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
	"github.com/grafana/dskit/backoff"
	"github.com/pkg/errors"
	"google.golang.org/api/iterator"
)

type bucket interface {
	Get(ctx context.Context, objectName string) (io.ReadCloser, error)
	Copy(ctx context.Context, objectName string, dstBucket bucket) error
	ListPrefix(ctx context.Context, prefix string, recursive bool) ([]string, error)
	UploadMarkerFile(ctx context.Context, objectName string) error
	Name() string
}

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

type azureBucket struct {
	azblob.Client
	containerClient         container.Client
	containerName           string
	copyStatusBackoffConfig backoff.Config
}

func newAzureBucketClient(containerURL string, accountName string, sharedKey string, copyStatusBackoffConfig backoff.Config) (bucket, error) {
	urlParts, err := blob.ParseURL(containerURL)
	if err != nil {
		return nil, err
	}
	containerName := urlParts.ContainerName
	if containerName == "" {
		return nil, errors.New("container name missing from azure bucket URL")
	}
	serviceURL, found := strings.CutSuffix(containerURL, containerName)
	if !found {
		return nil, errors.New("malformed or unexpected azure bucket URL")
	}
	keyCred, err := azblob.NewSharedKeyCredential(accountName, sharedKey)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get azure shared key credential")
	}
	client, err := azblob.NewClientWithSharedKeyCredential(serviceURL, keyCred, nil)
	if err != nil {
		return nil, err
	}
	containerClient, err := container.NewClientWithSharedKeyCredential(containerURL, keyCred, nil)
	if err != nil {
		return nil, err
	}
	return &azureBucket{
		Client:                  *client,
		containerClient:         *containerClient,
		containerName:           containerName,
		copyStatusBackoffConfig: copyStatusBackoffConfig,
	}, nil
}

func (bkt *azureBucket) Get(ctx context.Context, objectName string) (io.ReadCloser, error) {
	client := bkt.containerClient.NewBlobClient(objectName)
	response, err := client.DownloadStream(ctx, nil)
	if err != nil {
		return nil, err
	}
	return response.Body, nil
}

func (bkt *azureBucket) Copy(ctx context.Context, objectName string, dstBucket bucket) error {
	sourceClient := bkt.containerClient.NewBlobClient(objectName)
	sasURL, err := sourceClient.GetSASURL(sas.BlobPermissions{Read: true}, time.Now(), time.Now().Add(10*time.Minute))
	if err != nil {
		return err
	}
	d, ok := dstBucket.(*azureBucket)
	if !ok {
		return errors.New("destination bucket wasn't a blob storage bucket")
	}

	dstClient := d.containerClient.NewBlobClient(objectName)

	var copyStatus *blob.CopyStatusType
	var copyStatusDescription *string

	response, err := dstClient.StartCopyFromURL(ctx, sasURL, nil)
	if err != nil {
		if !bloberror.HasCode(err, bloberror.PendingCopyOperation) {
			return err
		}
		// There's already a copy operation. Assume it was initiated by us and a restart occurred, so check for the copy status.
		copyStatus, copyStatusDescription, err = checkCopyStatus(ctx, dstClient)
		if err != nil {
			return err
		}
	} else {
		// Note: no copy status description is currently provided from StartCopyFromURL
		// see https://learn.microsoft.com/en-us/rest/api/storageservices/copy-blob
		copyStatus = response.CopyStatus
	}

	backoff := backoff.New(ctx, d.copyStatusBackoffConfig)
	for {
		if copyStatus == nil {
			return errors.New("no copy status present for blob copy")
		}

		switch *copyStatus {
		case blob.CopyStatusTypeSuccess:
			return nil
		case blob.CopyStatusTypeFailed:
			if copyStatusDescription != nil {
				return errors.Errorf("copy failed, description: %s", *copyStatusDescription)
			}
			return errors.New("copy failed")
		case blob.CopyStatusTypeAborted:
			return errors.New("copy aborted")
		case blob.CopyStatusTypePending:
			// proceed
		default:
			return errors.Errorf("unrecognized copy status: %v", *copyStatus)
		}

		if !backoff.Ongoing() {
			break
		}
		backoff.Wait()

		copyStatus, copyStatusDescription, err = checkCopyStatus(ctx, dstClient)
		if err != nil {
			return err
		}
	}

	return errors.Wrap(backoff.Err(), "waiting for blob copy status")
}

func checkCopyStatus(ctx context.Context, client *blob.Client) (*blob.CopyStatusType, *string, error) {
	response, err := client.GetProperties(ctx, nil)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "failed while checking copy status")
	}
	return response.CopyStatus, response.CopyStatusDescription, nil
}

func (bkt *azureBucket) ListPrefix(ctx context.Context, prefix string, recursive bool) ([]string, error) {
	if prefix != "" && !strings.HasSuffix(prefix, delim) {
		prefix = prefix + delim
	}

	list := make([]string, 0, 10)
	if recursive {
		pager := bkt.containerClient.NewListBlobsFlatPager(&container.ListBlobsFlatOptions{Prefix: &prefix})
		for pager.More() {
			page, err := pager.NextPage(ctx)
			if err != nil {
				return nil, err
			}
			for _, blobItem := range page.Segment.BlobItems {
				list = append(list, *blobItem.Name)
			}
		}
	} else {
		pager := bkt.containerClient.NewListBlobsHierarchyPager(delim, &container.ListBlobsHierarchyOptions{Prefix: &prefix})
		for pager.More() {
			page, err := pager.NextPage(ctx)
			if err != nil {
				return nil, err
			}
			for _, blobItem := range page.Segment.BlobItems {
				list = append(list, *blobItem.Name)
			}
			for _, blobPrefix := range page.Segment.BlobPrefixes {
				list = append(list, *blobPrefix.Name)
			}
		}
	}

	var hasPrefix bool
	for i, s := range list {
		list[i], hasPrefix = strings.CutPrefix(s, prefix)
		if !hasPrefix {
			return nil, errors.Errorf("listPrefix: path has invalid prefix: %v, expected prefix: %v", s, prefix)
		}
	}

	return list, nil
}

func (bkt *azureBucket) UploadMarkerFile(ctx context.Context, objectName string) error {
	_, err := bkt.UploadBuffer(ctx, bkt.containerName, objectName, []byte{}, nil)
	return err
}

func (bkt *azureBucket) Name() string {
	return bkt.containerName
}
