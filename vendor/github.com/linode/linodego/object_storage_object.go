package linodego

import (
	"context"
	"encoding/json"
	"fmt"
)

type ObjectStorageObjectURLCreateOptions struct {
	Name               string `json:"name"`
	Method             string `json:"method"`
	ContentType        string `json:"content_type,omit_empty"`
	ContentDisposition string `json:"content_disposition,omit_empty"`
	ExpiresIn          *int   `json:"expires_in,omit_empty"`
}

type ObjectStorageObjectURL struct {
	URL    string `json:"url"`
	Exists bool   `json:"exists"`
}

type ObjectStorageObjectACLConfig struct {
	ACL    string `json:"acl"`
	ACLXML string `json:"acl_xml"`
}

type ObjectStorageObjectACLConfigUpdateOptions struct {
	Name string `json:"name"`
	ACL  string `json:"acl"`
}

func (c *Client) CreateObjectStorageObjectURL(ctx context.Context, objectID, label string, opts ObjectStorageObjectURLCreateOptions) (*ObjectStorageObjectURL, error) {
	body, err := json.Marshal(opts)
	if err != nil {
		return nil, err
	}

	e := fmt.Sprintf("object-storage/buckets/%s/%s/object-url", objectID, label)
	req := c.R(ctx).SetResult(&ObjectStorageObjectURL{}).SetBody(string(body))
	r, err := coupleAPIErrors(req.Post(e))
	return r.Result().(*ObjectStorageObjectURL), err
}

func (c *Client) GetObjectStorageObjectACLConfig(ctx context.Context, objectID, label, object string) (*ObjectStorageObjectACLConfig, error) {
	e := fmt.Sprintf("object-storage/buckets/%s/%s/object-acl?name=%s", objectID, label, object)
	req := c.R(ctx).SetResult(&ObjectStorageObjectACLConfig{})
	r, err := coupleAPIErrors(req.Get(e))
	return r.Result().(*ObjectStorageObjectACLConfig), err
}

func (c *Client) UpdateObjectStorageObjectACLConfig(ctx context.Context, objectID, label string, opts ObjectStorageObjectACLConfigUpdateOptions) (*ObjectStorageObjectACLConfig, error) {
	body, err := json.Marshal(opts)
	if err != nil {
		return nil, err
	}

	e := fmt.Sprintf("object-storage/buckets/%s/%s/object-acl", objectID, label)
	req := c.R(ctx).SetResult(&ObjectStorageObjectACLConfig{}).SetBody(string(body))
	r, err := coupleAPIErrors(req.Put(e))
	if err != nil {
		return nil, err
	}

	return r.Result().(*ObjectStorageObjectACLConfig), err
}
