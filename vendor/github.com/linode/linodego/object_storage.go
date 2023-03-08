package linodego

import (
	"context"
)

// ObjectStorageTransfer is an object matching the response of object-storage/transfer
type ObjectStorageTransfer struct {
	AmmountUsed int `json:"used"`
}

// CancelObjectStorage cancels and removes all object storage from the Account
func (c *Client) CancelObjectStorage(ctx context.Context) error {
	e := "object-storage/cancel"
	_, err := coupleAPIErrors(c.R(ctx).Post(e))
	return err
}

// GetObjectStorageTransfer returns the amount of outbound data transferred used by the Account
func (c *Client) GetObjectStorageTransfer(ctx context.Context) (*ObjectStorageTransfer, error) {
	e := "object-storage/transfer"
	req := c.R(ctx).SetResult(&ObjectStorageTransfer{})
	r, err := coupleAPIErrors(req.Get(e))
	if err != nil {
		return nil, err
	}

	return r.Result().(*ObjectStorageTransfer), nil
}
