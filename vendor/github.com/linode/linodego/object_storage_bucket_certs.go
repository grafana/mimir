package linodego

import (
	"context"
	"encoding/json"
	"fmt"
)

type ObjectStorageBucketCert struct {
	SSL bool `json:"ssl"`
}

type ObjectStorageBucketCertUploadOptions struct {
	Certificate string `json:"certificate"`
	PrivateKey  string `json:"private_key"`
}

// UploadObjectStorageBucketCert uploads a TLS/SSL Cert to be used with an Object Storage Bucket.
func (c *Client) UploadObjectStorageBucketCert(ctx context.Context, clusterID, bucket string, opts ObjectStorageBucketCertUploadOptions) (*ObjectStorageBucketCert, error) {
	body, err := json.Marshal(opts)
	if err != nil {
		return nil, err
	}

	e := fmt.Sprintf("object-storage/buckets/%s/%s/ssl", clusterID, bucket)
	req := c.R(ctx).SetResult(&ObjectStorageBucketCert{}).SetBody(string(body))
	r, err := coupleAPIErrors(req.Post(e))
	if err != nil {
		return nil, err
	}
	return r.Result().(*ObjectStorageBucketCert), nil
}

// GetObjectStorageBucketCert gets an ObjectStorageBucketCert
func (c *Client) GetObjectStorageBucketCert(ctx context.Context, clusterID, bucket string) (*ObjectStorageBucketCert, error) {
	e := fmt.Sprintf("object-storage/buckets/%s/%s/ssl", clusterID, bucket)
	req := c.R(ctx).SetResult(&ObjectStorageBucketCert{})
	r, err := coupleAPIErrors(req.Get(e))
	if err != nil {
		return nil, err
	}
	return r.Result().(*ObjectStorageBucketCert), nil
}

// DeleteObjectStorageBucketCert deletes an ObjectStorageBucketCert
func (c *Client) DeleteObjectStorageBucketCert(ctx context.Context, clusterID, bucket string) error {
	e := fmt.Sprintf("object-storage/buckets/%s/%s/ssl", clusterID, bucket)
	_, err := coupleAPIErrors(c.R(ctx).Delete(e))
	return err
}
