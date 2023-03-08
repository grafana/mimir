package linodego

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/go-resty/resty/v2"
)

// ObjectStorageKey represents a linode object storage key object
type ObjectStorageKey struct {
	ID           int                             `json:"id"`
	Label        string                          `json:"label"`
	AccessKey    string                          `json:"access_key"`
	SecretKey    string                          `json:"secret_key"`
	Limited      bool                            `json:"limited"`
	BucketAccess *[]ObjectStorageKeyBucketAccess `json:"bucket_access"`
}

// ObjectStorageKeyBucketAccess represents a linode limited object storage key's bucket access
type ObjectStorageKeyBucketAccess struct {
	Cluster     string `json:"cluster"`
	BucketName  string `json:"bucket_name"`
	Permissions string `json:"permissions"`
}

// ObjectStorageKeyCreateOptions fields are those accepted by CreateObjectStorageKey
type ObjectStorageKeyCreateOptions struct {
	Label        string                          `json:"label"`
	BucketAccess *[]ObjectStorageKeyBucketAccess `json:"bucket_access"`
}

// ObjectStorageKeyUpdateOptions fields are those accepted by UpdateObjectStorageKey
type ObjectStorageKeyUpdateOptions struct {
	Label string `json:"label"`
}

// ObjectStorageKeysPagedResponse represents a linode API response for listing
type ObjectStorageKeysPagedResponse struct {
	*PageOptions
	Data []ObjectStorageKey `json:"data"`
}

// endpoint gets the endpoint URL for Object Storage keys
func (ObjectStorageKeysPagedResponse) endpoint(_ ...any) string {
	return "object-storage/keys"
}

func (resp *ObjectStorageKeysPagedResponse) castResult(r *resty.Request, e string) (int, int, error) {
	res, err := coupleAPIErrors(r.SetResult(ObjectStorageKeysPagedResponse{}).Get(e))
	if err != nil {
		return 0, 0, err
	}
	castedRes := res.Result().(*ObjectStorageKeysPagedResponse)
	resp.Data = append(resp.Data, castedRes.Data...)
	return castedRes.Pages, castedRes.Results, nil
}

// ListObjectStorageKeys lists ObjectStorageKeys
func (c *Client) ListObjectStorageKeys(ctx context.Context, opts *ListOptions) ([]ObjectStorageKey, error) {
	response := ObjectStorageKeysPagedResponse{}
	err := c.listHelper(ctx, &response, opts)
	if err != nil {
		return nil, err
	}
	return response.Data, nil
}

// CreateObjectStorageKey creates a ObjectStorageKey
func (c *Client) CreateObjectStorageKey(ctx context.Context, opts ObjectStorageKeyCreateOptions) (*ObjectStorageKey, error) {
	body, err := json.Marshal(opts)
	if err != nil {
		return nil, NewError(err)
	}

	e := "object-storage/keys"
	req := c.R(ctx).SetResult(&ObjectStorageKey{}).SetBody(string(body))
	r, err := coupleAPIErrors(req.Post(e))
	if err != nil {
		return nil, err
	}
	return r.Result().(*ObjectStorageKey), nil
}

// GetObjectStorageKey gets the object storage key with the provided ID
func (c *Client) GetObjectStorageKey(ctx context.Context, keyID int) (*ObjectStorageKey, error) {
	e := fmt.Sprintf("object-storage/keys/%d", keyID)
	req := c.R(ctx).SetResult(&ObjectStorageKey{})
	r, err := coupleAPIErrors(req.Get(e))
	if err != nil {
		return nil, err
	}
	return r.Result().(*ObjectStorageKey), nil
}

// UpdateObjectStorageKey updates the object storage key with the specified id
func (c *Client) UpdateObjectStorageKey(ctx context.Context, keyID int, opts ObjectStorageKeyUpdateOptions) (*ObjectStorageKey, error) {
	body, err := json.Marshal(opts)
	if err != nil {
		return nil, err
	}

	e := fmt.Sprintf("object-storage/keys/%d", keyID)
	req := c.R(ctx).SetResult(&ObjectStorageKey{}).SetBody(string(body))
	r, err := coupleAPIErrors(req.Put(e))
	if err != nil {
		return nil, err
	}
	return r.Result().(*ObjectStorageKey), nil
}

// DeleteObjectStorageKey deletes the ObjectStorageKey with the specified id
func (c *Client) DeleteObjectStorageKey(ctx context.Context, keyID int) error {
	e := fmt.Sprintf("object-storage/keys/%d", keyID)
	_, err := coupleAPIErrors(c.R(ctx).Delete(e))
	return err
}
