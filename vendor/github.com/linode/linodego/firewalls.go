package linodego

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/linode/linodego/internal/parseabletime"
)

// FirewallStatus enum type
type FirewallStatus string

// FirewallStatus enums start with Firewall
const (
	FirewallEnabled  FirewallStatus = "enabled"
	FirewallDisabled FirewallStatus = "disabled"
	FirewallDeleted  FirewallStatus = "deleted"
)

// A Firewall is a set of networking rules (iptables) applied to Devices with which it is associated
type Firewall struct {
	ID      int             `json:"id"`
	Label   string          `json:"label"`
	Status  FirewallStatus  `json:"status"`
	Tags    []string        `json:"tags,omitempty"`
	Rules   FirewallRuleSet `json:"rules"`
	Created *time.Time      `json:"-"`
	Updated *time.Time      `json:"-"`
}

// DevicesCreationOptions fields are used when adding devices during the Firewall creation process.
type DevicesCreationOptions struct {
	Linodes       []int `json:"linodes,omitempty"`
	NodeBalancers []int `json:"nodebalancers,omitempty"`
}

// FirewallCreateOptions fields are those accepted by CreateFirewall
type FirewallCreateOptions struct {
	Label   string                 `json:"label,omitempty"`
	Rules   FirewallRuleSet        `json:"rules"`
	Tags    []string               `json:"tags,omitempty"`
	Devices DevicesCreationOptions `json:"devices,omitempty"`
}

// FirewallUpdateOptions is an options struct used when Updating a Firewall
type FirewallUpdateOptions struct {
	Label  string         `json:"label,omitempty"`
	Status FirewallStatus `json:"status,omitempty"`
	Tags   *[]string      `json:"tags,omitempty"`
}

// GetUpdateOptions converts a Firewall to FirewallUpdateOptions for use in Client.UpdateFirewall.
func (f *Firewall) GetUpdateOptions() FirewallUpdateOptions {
	return FirewallUpdateOptions{
		Label:  f.Label,
		Status: f.Status,
		Tags:   &f.Tags,
	}
}

// UnmarshalJSON for Firewall responses
func (f *Firewall) UnmarshalJSON(b []byte) error {
	type Mask Firewall

	p := struct {
		*Mask
		Created *parseabletime.ParseableTime `json:"created"`
		Updated *parseabletime.ParseableTime `json:"updated"`
	}{
		Mask: (*Mask)(f),
	}

	if err := json.Unmarshal(b, &p); err != nil {
		return err
	}

	f.Created = (*time.Time)(p.Created)
	f.Updated = (*time.Time)(p.Updated)
	return nil
}

// FirewallsPagedResponse represents a Linode API response for listing of Cloud Firewalls
type FirewallsPagedResponse struct {
	*PageOptions
	Data []Firewall `json:"data"`
}

func (FirewallsPagedResponse) endpoint(_ ...any) string {
	return "networking/firewalls"
}

func (resp *FirewallsPagedResponse) castResult(r *resty.Request, e string) (int, int, error) {
	res, err := coupleAPIErrors(r.SetResult(FirewallsPagedResponse{}).Get(e))
	if err != nil {
		return 0, 0, err
	}
	castedRes := res.Result().(*FirewallsPagedResponse)
	resp.Data = append(resp.Data, castedRes.Data...)
	return castedRes.Pages, castedRes.Results, nil
}

// ListFirewalls returns a paginated list of Cloud Firewalls
func (c *Client) ListFirewalls(ctx context.Context, opts *ListOptions) ([]Firewall, error) {
	response := FirewallsPagedResponse{}

	err := c.listHelper(ctx, &response, opts)
	if err != nil {
		return nil, err
	}

	return response.Data, nil
}

// CreateFirewall creates a single Firewall with at least one set of inbound or outbound rules
func (c *Client) CreateFirewall(ctx context.Context, opts FirewallCreateOptions) (*Firewall, error) {
	body, err := json.Marshal(opts)
	if err != nil {
		return nil, err
	}

	e := "networking/firewalls"
	req := c.R(ctx).SetResult(&Firewall{}).SetBody(string(body))
	r, err := coupleAPIErrors(req.Post(e))
	if err != nil {
		return nil, err
	}

	return r.Result().(*Firewall), nil
}

// GetFirewall gets a single Firewall with the provided ID
func (c *Client) GetFirewall(ctx context.Context, firewallID int) (*Firewall, error) {
	e := fmt.Sprintf("networking/firewalls/%d", firewallID)
	req := c.R(ctx).SetResult(&Firewall{})
	r, err := coupleAPIErrors(req.Get(e))
	if err != nil {
		return nil, err
	}

	return r.Result().(*Firewall), nil
}

// UpdateFirewall updates a Firewall with the given ID
func (c *Client) UpdateFirewall(ctx context.Context, firewallID int, opts FirewallUpdateOptions) (*Firewall, error) {
	body, err := json.Marshal(opts)
	if err != nil {
		return nil, err
	}

	e := fmt.Sprintf("networking/firewalls/%d", firewallID)
	req := c.R(ctx).SetResult(&Firewall{}).SetBody(string(body))
	r, err := coupleAPIErrors(req.Put(e))
	if err != nil {
		return nil, err
	}

	return r.Result().(*Firewall), nil
}

// DeleteFirewall deletes a single Firewall with the provided ID
func (c *Client) DeleteFirewall(ctx context.Context, firewallID int) error {
	e := fmt.Sprintf("networking/firewalls/%d", firewallID)
	_, err := coupleAPIErrors(c.R(ctx).Delete(e))
	return err
}
