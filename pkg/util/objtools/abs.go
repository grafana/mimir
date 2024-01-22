// SPDX-License-Identifier: AGPL-3.0-only

package objtools

import (
	"context"
	"flag"
	"fmt"
	"io"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
	"github.com/grafana/dskit/backoff"
	"github.com/pkg/errors"
)

type azureBucket struct {
	containerClient         container.Client
	containerName           string
	containerURL            string
	copyStatusBackoffConfig backoff.Config
}

type AzureClientConfig struct {
	ContainerName     string
	AccountName       string
	AccountKey        string
	CopyStatusBackoff backoff.Config
}

func (c *AzureClientConfig) RegisterFlags(prefix string, f *flag.FlagSet) {
	f.StringVar(&c.ContainerName, prefix+"container-name", "", "The container name for Azure Blob Storage.")
	f.StringVar(&c.AccountName, prefix+"account-name", "", "The storage account name for Azure Blob Storage.")
	f.StringVar(&c.AccountKey, prefix+"account-key", "", "The storage account key for Azure Blob Storage.")
	f.DurationVar(&c.CopyStatusBackoff.MinBackoff, prefix+"copy-status-backoff-min-duration", 15*time.Second, "The minimum amount of time to back off per copy operation sourced from this bucket.")
	f.DurationVar(&c.CopyStatusBackoff.MaxBackoff, prefix+"copy-status-backoff-max-duration", 20*time.Second, "The maximum amount of time to back off per copy operation sourced from this bucket.")
	f.IntVar(&c.CopyStatusBackoff.MaxRetries, prefix+"copy-status-backoff-max-retries", 40, "The maximum number of retries while checking the copy status of copies sourced from this bucket.")
}

func (c *AzureClientConfig) Validate(prefix string) error {
	if c.ContainerName == "" {
		return fmt.Errorf("the Azure container name (%s) is required", prefix+"container-name")
	}
	if c.AccountName == "" {
		return fmt.Errorf("the Azure storage account name (%s) is required", prefix+"account-name")
	}
	if c.AccountKey == "" {
		return fmt.Errorf("the Azure storage account key (%s) is required", prefix+"account-key")
	}
	return nil
}

func (c *AzureClientConfig) ToBucket() (Bucket, error) {
	// Docs: https://learn.microsoft.com/en-us/rest/api/storageservices/naming-and-referencing-containers--blobs--and-metadata#resource-uri-syntax
	serviceURL := fmt.Sprintf("https://%s.blob.core.windows.net/", c.AccountName)
	keyCred, err := azblob.NewSharedKeyCredential(c.AccountName, c.AccountKey)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get Azure shared key credential")
	}
	containerURL := serviceURL + c.ContainerName
	containerClient, err := container.NewClientWithSharedKeyCredential(containerURL, keyCred, nil)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to construct azure container client")
	}
	return &azureBucket{
		containerClient:         *containerClient,
		containerName:           c.ContainerName,
		containerURL:            containerURL,
		copyStatusBackoffConfig: c.CopyStatusBackoff,
	}, nil
}

func (bkt *azureBucket) Get(ctx context.Context, objectName string, options GetOptions) (io.ReadCloser, error) {
	client := bkt.containerClient.NewBlobClient(objectName)
	client, err := client.WithVersionID(options.VersionID)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("could not construct a client with the provided version ID: %s", options.VersionID))
	}

	response, err := client.DownloadStream(ctx, nil)
	if err != nil {
		return nil, err
	}
	return response.Body, nil
}

func (bkt *azureBucket) ServerSideCopy(ctx context.Context, objectName string, dstBucket Bucket, options CopyOptions) error {
	sourceClient := bkt.containerClient.NewBlobClient(objectName)
	sourceClient, err := sourceClient.WithVersionID(options.SourceVersionID)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("could not construct a client with the provided source version ID: %s", options.SourceVersionID))
	}
	d, ok := dstBucket.(*azureBucket)
	if !ok {
		return errors.New("destination bucket wasn't an Azure bucket")
	}

	dstClient := d.containerClient.NewBlobClient(options.destinationObjectName(objectName))

	var copySource string
	if bkt.containerURL == d.containerURL {
		// Copying within the same container - no need for shared access signature
		copySource = sourceClient.URL()
	} else {
		start := time.Now()
		expiry := start.Add(10 * time.Minute)
		sasURL, err := sourceClient.GetSASURL(sas.BlobPermissions{Read: true}, expiry, &blob.GetSASURLOptions{StartTime: &start})
		if err != nil {
			return err
		}
		copySource = sasURL
	}

	var copyStatus *blob.CopyStatusType
	var copyStatusDescription *string

	response, err := dstClient.StartCopyFromURL(ctx, copySource, nil)
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

func (bkt *azureBucket) ClientSideCopy(ctx context.Context, objectName string, dstBucket Bucket, options CopyOptions) error {
	sourceClient := bkt.containerClient.NewBlobClient(objectName)
	sourceClient, err := sourceClient.WithVersionID(options.SourceVersionID)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("could not construct a client with the provided source version ID: %s", options.SourceVersionID))
	}

	response, err := sourceClient.DownloadStream(ctx, nil)
	if err != nil {
		return errors.Wrap(err, "failed while getting source object from Azure")
	}
	if response.ContentLength == nil {
		return errors.New("source object from Azure did not contain a content length")
	}
	body := response.DownloadResponse.Body
	if err := dstBucket.Upload(ctx, options.destinationObjectName(objectName), body, *response.ContentLength); err != nil {
		_ = body.Close()
		return errors.New("failed uploading source object from Azure to destination")
	}
	return errors.Wrap(body.Close(), "failed closing Azure source object reader")
}

func unpackBlobItem(blobItem *container.BlobItem, isVersioned bool) ObjectAttributes {
	attributes := ObjectAttributes{
		Name:         *blobItem.Name,
		Size:         *blobItem.Properties.ContentLength,
		LastModified: *blobItem.Properties.LastModified,
	}

	// Documentation: https://learn.microsoft.com/en-us/rest/api/storageservices/list-blobs
	if isVersioned {
		attributes.VersionInfo = VersionInfo{
			VersionID:        *blobItem.VersionID,
			IsCurrent:        blobItem.IsCurrentVersion != nil, // only present when true
			RequiresUndelete: blobItem.Deleted != nil,          // only present when true
		}
	}

	return attributes
}

func (bkt *azureBucket) List(ctx context.Context, options ListOptions) (*ListResult, error) {
	prefix := ensureDelimiterSuffix(options.Prefix)
	include := container.ListBlobsInclude{
		Versions: options.Versioned,
		Deleted:  options.Versioned,
	}

	objects := make([]ObjectAttributes, 0, 10)
	var prefixes []string
	if options.Recursive {
		pager := bkt.containerClient.NewListBlobsFlatPager(&container.ListBlobsFlatOptions{
			Prefix:  &prefix,
			Include: include,
		})
		for pager.More() {
			page, err := pager.NextPage(ctx)
			if err != nil {
				return nil, err
			}
			for _, blobItem := range page.Segment.BlobItems {
				objects = append(objects, unpackBlobItem(blobItem, options.Versioned))
			}
		}
	} else {
		prefixes = make([]string, 0, 10)
		pager := bkt.containerClient.NewListBlobsHierarchyPager(Delim, &container.ListBlobsHierarchyOptions{
			Prefix:  &prefix,
			Include: include,
		})
		for pager.More() {
			page, err := pager.NextPage(ctx)
			if err != nil {
				return nil, err
			}
			for _, blobItem := range page.Segment.BlobItems {
				objects = append(objects, unpackBlobItem(blobItem, options.Versioned))
			}
			for _, blobPrefix := range page.Segment.BlobPrefixes {
				prefixes = append(prefixes, *blobPrefix.Name)
			}
		}
	}

	return &ListResult{Objects: objects, Prefixes: prefixes}, nil
}

func (bkt *azureBucket) RestoreVersion(ctx context.Context, objectName string, versionInfo VersionInfo) error {
	// Docs: https://learn.microsoft.com/en-us/azure/storage/blobs/versioning-overview#restoring-a-soft-deleted-version
	if versionInfo.RequiresUndelete {
		blobClient := bkt.containerClient.NewBlobClient(objectName)
		_, err := blobClient.Undelete(ctx, &blob.UndeleteOptions{}) // restores all soft deleted versions (none go to current)
		if err != nil {
			return err
		}
	}
	// Copy the version onto the same object to restore it to the current version
	return bkt.ServerSideCopy(ctx, objectName, bkt, CopyOptions{
		SourceVersionID: versionInfo.VersionID,
	})
}

func (bkt *azureBucket) Upload(ctx context.Context, objectName string, reader io.Reader, _ int64) error {
	client := bkt.containerClient.NewBlockBlobClient(objectName)
	_, err := client.UploadStream(ctx, reader, nil)
	return err
}

func (bkt *azureBucket) Delete(ctx context.Context, objectName string, options DeleteOptions) error {
	blobClient, err := bkt.containerClient.NewBlobClient(objectName).WithVersionID(options.VersionID)
	if err != nil {
		return err
	}
	_, err = blobClient.Delete(ctx, nil)
	return err
}

func (bkt *azureBucket) Name() string {
	return bkt.containerName
}
