package linodego

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/linode/linodego/internal/parseabletime"
)

// FirewallDeviceType represents the different kinds of devices governable by a Firewall
type FirewallDeviceType string

// FirewallDeviceType constants start with FirewallDevice
const (
	FirewallDeviceLinode       FirewallDeviceType = "linode"
	FirewallDeviceNodeBalancer FirewallDeviceType = "nodebalancer"
)

// FirewallDevice represents a device governed by a Firewall
type FirewallDevice struct {
	ID      int                  `json:"id"`
	Entity  FirewallDeviceEntity `json:"entity"`
	Created *time.Time           `json:"-"`
	Updated *time.Time           `json:"-"`
}

// FirewallDeviceCreateOptions fields are those accepted by CreateFirewallDevice
type FirewallDeviceCreateOptions struct {
	ID   int                `json:"id"`
	Type FirewallDeviceType `json:"type"`
}

// UnmarshalJSON implements the json.Unmarshaler interface
func (device *FirewallDevice) UnmarshalJSON(b []byte) error {
	type Mask FirewallDevice

	p := struct {
		*Mask
		Created *parseabletime.ParseableTime `json:"created"`
		Updated *parseabletime.ParseableTime `json:"updated"`
	}{
		Mask: (*Mask)(device),
	}

	if err := json.Unmarshal(b, &p); err != nil {
		return err
	}

	device.Created = (*time.Time)(p.Created)
	device.Updated = (*time.Time)(p.Updated)
	return nil
}

// FirewallDeviceEntity contains information about a device associated with a Firewall
type FirewallDeviceEntity struct {
	ID    int                `json:"id"`
	Type  FirewallDeviceType `json:"type"`
	Label string             `json:"label"`
	URL   string             `json:"url"`
}

// FirewallDevicesPagedResponse represents a Linode API response for FirewallDevices
type FirewallDevicesPagedResponse struct {
	*PageOptions
	Data []FirewallDevice `json:"data"`
}

// endpoint gets the endpoint URL for FirewallDevices of a given Firewall
func (FirewallDevicesPagedResponse) endpoint(ids ...any) string {
	id, _ := ids[0].(int)
	return fmt.Sprintf("networking/firewalls/%d/devices", id)
}

func (resp *FirewallDevicesPagedResponse) castResult(r *resty.Request, e string) (int, int, error) {
	res, err := coupleAPIErrors(r.SetResult(FirewallDevicesPagedResponse{}).Get(e))
	if err != nil {
		return 0, 0, err
	}
	castedRes := res.Result().(*FirewallDevicesPagedResponse)
	resp.Data = append(resp.Data, castedRes.Data...)
	return castedRes.Pages, castedRes.Results, nil
}

// ListFirewallDevices get devices associated with a given Firewall
func (c *Client) ListFirewallDevices(ctx context.Context, firewallID int, opts *ListOptions) ([]FirewallDevice, error) {
	response := FirewallDevicesPagedResponse{}
	err := c.listHelper(ctx, &response, opts, firewallID)
	if err != nil {
		return nil, err
	}
	return response.Data, nil
}

// GetFirewallDevice gets a FirewallDevice given an ID
func (c *Client) GetFirewallDevice(ctx context.Context, firewallID, deviceID int) (*FirewallDevice, error) {
	e := fmt.Sprintf("networking/firewalls/%d/devices/%d", firewallID, deviceID)
	req := c.R(ctx).SetResult(&FirewallDevice{})
	r, err := coupleAPIErrors(req.Get(e))
	if err != nil {
		return nil, err
	}
	return r.Result().(*FirewallDevice), nil
}

// AddFirewallDevice associates a Device with a given Firewall
func (c *Client) CreateFirewallDevice(ctx context.Context, firewallID int, opts FirewallDeviceCreateOptions) (*FirewallDevice, error) {
	body, err := json.Marshal(opts)
	if err != nil {
		return nil, err
	}

	e := fmt.Sprintf("networking/firewalls/%d/devices", firewallID)
	req := c.R(ctx).SetResult(&FirewallDevice{}).SetBody(string(body))
	r, err := coupleAPIErrors(req.Post(e))
	if err != nil {
		return nil, err
	}
	return r.Result().(*FirewallDevice), nil
}

// DeleteFirewallDevice disassociates a Device with a given Firewall
func (c *Client) DeleteFirewallDevice(ctx context.Context, firewallID, deviceID int) error {
	e := fmt.Sprintf("networking/firewalls/%d/devices/%d", firewallID, deviceID)
	_, err := coupleAPIErrors(c.R(ctx).Delete(e))
	return err
}
