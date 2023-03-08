package linodego

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/go-resty/resty/v2"
)

// IPAddressesPagedResponse represents a paginated IPAddress API response
type IPAddressesPagedResponse struct {
	*PageOptions
	Data []InstanceIP `json:"data"`
}

// IPAddressUpdateOptions fields are those accepted by UpdateToken
type IPAddressUpdateOptions struct {
	// The reverse DNS assigned to this address. For public IPv4 addresses, this will be set to a default value provided by Linode if set to nil.
	RDNS *string `json:"rdns"`
}

// LinodeIPAssignment stores an assignment between an IP address and a Linode instance.
type LinodeIPAssignment struct {
	Address  string `json:"address"`
	LinodeID int    `json:"linode_id"`
}

// LinodesAssignIPsOptions fields are those accepted by InstancesAssignIPs.
type LinodesAssignIPsOptions struct {
	Region string `json:"region"`

	Assignments []LinodeIPAssignment `json:"assignments"`
}

// IPAddressesShareOptions fields are those accepted by ShareIPAddresses.
type IPAddressesShareOptions struct {
	IPs      []string `json:"ips"`
	LinodeID int      `json:"linode_id"`
}

// GetUpdateOptions converts a IPAddress to IPAddressUpdateOptions for use in UpdateIPAddress
func (i InstanceIP) GetUpdateOptions() (o IPAddressUpdateOptions) {
	o.RDNS = copyString(&i.RDNS)
	return
}

// endpoint gets the endpoint URL for IPAddress
func (IPAddressesPagedResponse) endpoint(_ ...any) string {
	return "networking/ips"
}

func (resp *IPAddressesPagedResponse) castResult(r *resty.Request, e string) (int, int, error) {
	res, err := coupleAPIErrors(r.SetResult(IPAddressesPagedResponse{}).Get(e))
	if err != nil {
		return 0, 0, err
	}
	castedRes := res.Result().(*IPAddressesPagedResponse)
	resp.Data = append(resp.Data, castedRes.Data...)
	return castedRes.Pages, castedRes.Results, nil
}

// ListIPAddresses lists IPAddresses
func (c *Client) ListIPAddresses(ctx context.Context, opts *ListOptions) ([]InstanceIP, error) {
	response := IPAddressesPagedResponse{}
	err := c.listHelper(ctx, &response, opts)
	if err != nil {
		return nil, err
	}
	return response.Data, nil
}

// GetIPAddress gets the template with the provided ID
func (c *Client) GetIPAddress(ctx context.Context, id string) (*InstanceIP, error) {
	e := fmt.Sprintf("networking/ips/%s", id)
	req := c.R(ctx).SetResult(&InstanceIP{})
	r, err := coupleAPIErrors(req.Get(e))
	if err != nil {
		return nil, err
	}
	return r.Result().(*InstanceIP), nil
}

// UpdateIPAddress updates the IPAddress with the specified id
func (c *Client) UpdateIPAddress(ctx context.Context, id string, opts IPAddressUpdateOptions) (*InstanceIP, error) {
	body, err := json.Marshal(opts)
	if err != nil {
		return nil, err
	}

	e := fmt.Sprintf("networking/ips/%s", id)
	req := c.R(ctx).SetResult(&InstanceIP{}).SetBody(string(body))
	r, err := coupleAPIErrors(req.Put(e))
	if err != nil {
		return nil, err
	}
	return r.Result().(*InstanceIP), nil
}

// InstancesAssignIPs assigns multiple IPv4 addresses and/or IPv6 ranges to multiple Linodes in one Region.
// This allows swapping, shuffling, or otherwise reorganizing IPs to your Linodes.
func (c *Client) InstancesAssignIPs(ctx context.Context, opts LinodesAssignIPsOptions) error {
	body, err := json.Marshal(opts)
	if err != nil {
		return err
	}

	e := "networking/ips/assign"
	req := c.R(ctx).SetBody(string(body))
	_, err = coupleAPIErrors(req.Post(e))
	return err
}

// ShareIPAddresses allows IP address reassignment (also referred to as IP failover)
// from one Linode to another if the primary Linode becomes unresponsive.
func (c *Client) ShareIPAddresses(ctx context.Context, opts IPAddressesShareOptions) error {
	body, err := json.Marshal(opts)
	if err != nil {
		return err
	}

	e := "networking/ips/share"
	req := c.R(ctx).SetBody(string(body))
	_, err = coupleAPIErrors(req.Post(e))
	return err
}
