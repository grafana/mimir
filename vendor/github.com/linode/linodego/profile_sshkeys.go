package linodego

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/linode/linodego/internal/parseabletime"
)

// SSHKey represents a SSHKey object
type SSHKey struct {
	ID      int        `json:"id"`
	Label   string     `json:"label"`
	SSHKey  string     `json:"ssh_key"`
	Created *time.Time `json:"-"`
}

// SSHKeyCreateOptions fields are those accepted by CreateSSHKey
type SSHKeyCreateOptions struct {
	Label  string `json:"label"`
	SSHKey string `json:"ssh_key"`
}

// SSHKeyUpdateOptions fields are those accepted by UpdateSSHKey
type SSHKeyUpdateOptions struct {
	Label string `json:"label"`
}

// UnmarshalJSON implements the json.Unmarshaler interface
func (i *SSHKey) UnmarshalJSON(b []byte) error {
	type Mask SSHKey

	p := struct {
		*Mask
		Created *parseabletime.ParseableTime `json:"created"`
	}{
		Mask: (*Mask)(i),
	}

	if err := json.Unmarshal(b, &p); err != nil {
		return err
	}

	i.Created = (*time.Time)(p.Created)

	return nil
}

// GetCreateOptions converts a SSHKey to SSHKeyCreateOptions for use in CreateSSHKey
func (i SSHKey) GetCreateOptions() (o SSHKeyCreateOptions) {
	o.Label = i.Label
	o.SSHKey = i.SSHKey
	return
}

// GetUpdateOptions converts a SSHKey to SSHKeyCreateOptions for use in UpdateSSHKey
func (i SSHKey) GetUpdateOptions() (o SSHKeyUpdateOptions) {
	o.Label = i.Label
	return
}

// SSHKeysPagedResponse represents a paginated SSHKey API response
type SSHKeysPagedResponse struct {
	*PageOptions
	Data []SSHKey `json:"data"`
}

// endpoint gets the endpoint URL for SSHKey
func (SSHKeysPagedResponse) endpoint(_ ...any) string {
	return "profile/sshkeys"
}

func (resp *SSHKeysPagedResponse) castResult(r *resty.Request, e string) (int, int, error) {
	res, err := coupleAPIErrors(r.SetResult(SSHKeysPagedResponse{}).Get(e))
	if err != nil {
		return 0, 0, err
	}
	castedRes := res.Result().(*SSHKeysPagedResponse)
	resp.Data = append(resp.Data, castedRes.Data...)
	return castedRes.Pages, castedRes.Results, nil
}

// ListSSHKeys lists SSHKeys
func (c *Client) ListSSHKeys(ctx context.Context, opts *ListOptions) ([]SSHKey, error) {
	response := SSHKeysPagedResponse{}
	err := c.listHelper(ctx, &response, opts)
	if err != nil {
		return nil, err
	}
	return response.Data, nil
}

// GetSSHKey gets the sshkey with the provided ID
func (c *Client) GetSSHKey(ctx context.Context, keyID int) (*SSHKey, error) {
	e := fmt.Sprintf("profile/sshkeys/%d", keyID)
	req := c.R(ctx).SetResult(&SSHKey{})
	r, err := coupleAPIErrors(req.Get(e))
	if err != nil {
		return nil, err
	}
	return r.Result().(*SSHKey), nil
}

// CreateSSHKey creates a SSHKey
func (c *Client) CreateSSHKey(ctx context.Context, opts SSHKeyCreateOptions) (*SSHKey, error) {
	body, err := json.Marshal(opts)
	if err != nil {
		return nil, err
	}

	e := "profile/sshkeys"
	req := c.R(ctx).SetResult(&SSHKey{}).SetBody(string(body))
	r, err := coupleAPIErrors(req.Post(e))
	if err != nil {
		return nil, err
	}
	return r.Result().(*SSHKey), nil
}

// UpdateSSHKey updates the SSHKey with the specified id
func (c *Client) UpdateSSHKey(ctx context.Context, keyID int, opts SSHKeyUpdateOptions) (*SSHKey, error) {
	body, err := json.Marshal(opts)
	if err != nil {
		return nil, err
	}

	e := fmt.Sprintf("profile/sshkeys/%d", keyID)
	req := c.R(ctx).SetResult(&SSHKey{}).SetBody(string(body))
	r, err := coupleAPIErrors(req.Put(e))
	if err != nil {
		return nil, err
	}
	return r.Result().(*SSHKey), nil
}

// DeleteSSHKey deletes the SSHKey with the specified id
func (c *Client) DeleteSSHKey(ctx context.Context, keyID int) error {
	e := fmt.Sprintf("profile/sshkeys/%d", keyID)
	_, err := coupleAPIErrors(c.R(ctx).Delete(e))
	return err
}
