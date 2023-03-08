package linodego

import (
	"context"
	"encoding/json"
	"fmt"
)

// InstanceIPAddressResponse contains the IPv4 and IPv6 details for an Instance
type InstanceIPAddressResponse struct {
	IPv4 *InstanceIPv4Response `json:"ipv4"`
	IPv6 *InstanceIPv6Response `json:"ipv6"`
}

// InstanceIPv4Response contains the details of all IPv4 addresses associated with an Instance
type InstanceIPv4Response struct {
	Public   []*InstanceIP `json:"public"`
	Private  []*InstanceIP `json:"private"`
	Shared   []*InstanceIP `json:"shared"`
	Reserved []*InstanceIP `json:"reserved"`
}

// InstanceIP represents an Instance IP with additional DNS and networking details
type InstanceIP struct {
	Address    string         `json:"address"`
	Gateway    string         `json:"gateway"`
	SubnetMask string         `json:"subnet_mask"`
	Prefix     int            `json:"prefix"`
	Type       InstanceIPType `json:"type"`
	Public     bool           `json:"public"`
	RDNS       string         `json:"rdns"`
	LinodeID   int            `json:"linode_id"`
	Region     string         `json:"region"`
}

// InstanceIPv6Response contains the IPv6 addresses and ranges for an Instance
type InstanceIPv6Response struct {
	LinkLocal *InstanceIP `json:"link_local"`
	SLAAC     *InstanceIP `json:"slaac"`
	Global    []IPv6Range `json:"global"`
}

// IPv6Range represents a range of IPv6 addresses routed to a single Linode in a given Region
type IPv6Range struct {
	Range  string `json:"range"`
	Region string `json:"region"`
	Prefix int    `json:"prefix"`

	RouteTarget string `json:"route_target"`

	// These fields are only returned by GetIPv6Range(...)
	IsBGP   bool  `json:"is_bgp"`
	Linodes []int `json:"linodes"`
}

// InstanceIPType constants start with IPType and include Linode Instance IP Types
type InstanceIPType string

// InstanceIPType constants represent the IP types an Instance IP may be
const (
	IPTypeIPv4      InstanceIPType = "ipv4"
	IPTypeIPv6      InstanceIPType = "ipv6"
	IPTypeIPv6Pool  InstanceIPType = "ipv6/pool"
	IPTypeIPv6Range InstanceIPType = "ipv6/range"
)

// GetInstanceIPAddresses gets the IPAddresses for a Linode instance
func (c *Client) GetInstanceIPAddresses(ctx context.Context, linodeID int) (*InstanceIPAddressResponse, error) {
	e := fmt.Sprintf("linode/instances/%d/ips", linodeID)
	req := c.R(ctx).SetResult(&InstanceIPAddressResponse{})
	r, err := coupleAPIErrors(req.Get(e))
	if err != nil {
		return nil, err
	}
	return r.Result().(*InstanceIPAddressResponse), nil
}

// GetInstanceIPAddress gets the IPAddress for a Linode instance matching a supplied IP address
func (c *Client) GetInstanceIPAddress(ctx context.Context, linodeID int, ipaddress string) (*InstanceIP, error) {
	e := fmt.Sprintf("linode/instances/%d/ips/%s", linodeID, ipaddress)
	req := c.R(ctx).SetResult(&InstanceIP{})
	r, err := coupleAPIErrors(req.Get(e))
	if err != nil {
		return nil, err
	}

	return r.Result().(*InstanceIP), nil
}

// AddInstanceIPAddress adds a public or private IP to a Linode instance
func (c *Client) AddInstanceIPAddress(ctx context.Context, linodeID int, public bool) (*InstanceIP, error) {
	instanceipRequest := struct {
		Type   string `json:"type"`
		Public bool   `json:"public"`
	}{"ipv4", public}

	body, err := json.Marshal(instanceipRequest)
	if err != nil {
		return nil, err
	}

	e := fmt.Sprintf("linode/instances/%d/ips", linodeID)
	req := c.R(ctx).SetResult(&InstanceIP{}).SetBody(string(body))
	r, err := coupleAPIErrors(req.Post(e))
	if err != nil {
		return nil, err
	}

	return r.Result().(*InstanceIP), nil
}

// UpdateInstanceIPAddress updates the IPAddress with the specified instance id and IP address
func (c *Client) UpdateInstanceIPAddress(ctx context.Context, linodeID int, ipAddress string, opts IPAddressUpdateOptions) (*InstanceIP, error) {
	body, err := json.Marshal(opts)
	if err != nil {
		return nil, err
	}

	e := fmt.Sprintf("linode/instances/%d/ips/%s", linodeID, ipAddress)
	req := c.R(ctx).SetResult(&InstanceIP{}).SetBody(string(body))
	r, err := coupleAPIErrors(req.Put(e))
	if err != nil {
		return nil, err
	}
	return r.Result().(*InstanceIP), nil
}

func (c *Client) DeleteInstanceIPAddress(ctx context.Context, linodeID int, ipAddress string) error {
	e := fmt.Sprintf("linode/instances/%d/ips/%s", linodeID, ipAddress)
	_, err := coupleAPIErrors(c.R(ctx).Delete(e))
	return err
}
