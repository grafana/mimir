package linodego

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/go-resty/resty/v2"
)

// OAuthClientStatus constants start with OAuthClient and include Linode API Instance Status values
type OAuthClientStatus string

// OAuthClientStatus constants reflect the current status of an OAuth Client
const (
	OAuthClientActive    OAuthClientStatus = "active"
	OAuthClientDisabled  OAuthClientStatus = "disabled"
	OAuthClientSuspended OAuthClientStatus = "suspended"
)

// OAuthClient represents a OAuthClient object
type OAuthClient struct {
	// The unique ID of this OAuth Client.
	ID string `json:"id"`

	// The location a successful log in from https://login.linode.com should be redirected to for this client. The receiver of this redirect should be ready to accept an OAuth exchange code and finish the OAuth exchange.
	RedirectURI string `json:"redirect_uri"`

	// The name of this application. This will be presented to users when they are asked to grant it access to their Account.
	Label string `json:"label"`

	// Current status of the OAuth Client, Enum: "active" "disabled" "suspended"
	Status OAuthClientStatus `json:"status"`

	// The OAuth Client secret, used in the OAuth exchange. This is returned as <REDACTED> except when an OAuth Client is created or its secret is reset. This is a secret, and should not be shared or disclosed publicly.
	Secret string `json:"secret"`

	// If this OAuth Client is public or private.
	Public bool `json:"public"`

	// The URL where this client's thumbnail may be viewed, or nil if this client does not have a thumbnail set.
	ThumbnailURL *string `json:"thumbnail_url"`
}

// OAuthClientCreateOptions fields are those accepted by CreateOAuthClient
type OAuthClientCreateOptions struct {
	// The location a successful log in from https://login.linode.com should be redirected to for this client. The receiver of this redirect should be ready to accept an OAuth exchange code and finish the OAuth exchange.
	RedirectURI string `json:"redirect_uri"`

	// The name of this application. This will be presented to users when they are asked to grant it access to their Account.
	Label string `json:"label"`

	// If this OAuth Client is public or private.
	Public bool `json:"public"`
}

// OAuthClientUpdateOptions fields are those accepted by UpdateOAuthClient
type OAuthClientUpdateOptions struct {
	// The location a successful log in from https://login.linode.com should be redirected to for this client. The receiver of this redirect should be ready to accept an OAuth exchange code and finish the OAuth exchange.
	RedirectURI string `json:"redirect_uri"`

	// The name of this application. This will be presented to users when they are asked to grant it access to their Account.
	Label string `json:"label"`

	// If this OAuth Client is public or private.
	Public bool `json:"public"`
}

// GetCreateOptions converts a OAuthClient to OAuthClientCreateOptions for use in CreateOAuthClient
func (i OAuthClient) GetCreateOptions() (o OAuthClientCreateOptions) {
	o.RedirectURI = i.RedirectURI
	o.Label = i.Label
	o.Public = i.Public

	return
}

// GetUpdateOptions converts a OAuthClient to OAuthClientUpdateOptions for use in UpdateOAuthClient
func (i OAuthClient) GetUpdateOptions() (o OAuthClientUpdateOptions) {
	o.RedirectURI = i.RedirectURI
	o.Label = i.Label
	o.Public = i.Public

	return
}

// OAuthClientsPagedResponse represents a paginated OAuthClient API response
type OAuthClientsPagedResponse struct {
	*PageOptions
	Data []OAuthClient `json:"data"`
}

// endpoint gets the endpoint URL for OAuthClient
func (OAuthClientsPagedResponse) endpoint(_ ...any) string {
	return "account/oauth-clients"
}

func (resp *OAuthClientsPagedResponse) castResult(r *resty.Request, e string) (int, int, error) {
	res, err := coupleAPIErrors(r.SetResult(OAuthClientsPagedResponse{}).Get(e))
	if err != nil {
		return 0, 0, err
	}
	castedRes := res.Result().(*OAuthClientsPagedResponse)
	resp.Data = append(resp.Data, castedRes.Data...)
	return castedRes.Pages, castedRes.Results, nil
}

// ListOAuthClients lists OAuthClients
func (c *Client) ListOAuthClients(ctx context.Context, opts *ListOptions) ([]OAuthClient, error) {
	response := OAuthClientsPagedResponse{}
	err := c.listHelper(ctx, &response, opts)
	if err != nil {
		return nil, err
	}

	return response.Data, nil
}

// GetOAuthClient gets the OAuthClient with the provided ID
func (c *Client) GetOAuthClient(ctx context.Context, clientID string) (*OAuthClient, error) {
	req := c.R(ctx).SetResult(&OAuthClient{})
	e := fmt.Sprintf("account/oauth-clients/%s", clientID)
	r, err := coupleAPIErrors(req.Get(e))
	if err != nil {
		return nil, err
	}

	return r.Result().(*OAuthClient), nil
}

// CreateOAuthClient creates an OAuthClient
func (c *Client) CreateOAuthClient(ctx context.Context, opts OAuthClientCreateOptions) (*OAuthClient, error) {
	body, err := json.Marshal(opts)
	if err != nil {
		return nil, err
	}

	req := c.R(ctx).SetResult(&OAuthClient{}).SetBody(string(body))
	e := "account/oauth-clients"
	r, err := coupleAPIErrors(req.Post(e))
	if err != nil {
		return nil, err
	}

	return r.Result().(*OAuthClient), nil
}

// UpdateOAuthClient updates the OAuthClient with the specified id
func (c *Client) UpdateOAuthClient(ctx context.Context, clientID string, opts OAuthClientUpdateOptions) (*OAuthClient, error) {
	body, err := json.Marshal(opts)
	if err != nil {
		return nil, err
	}

	req := c.R(ctx).SetResult(&OAuthClient{}).SetBody(string(body))
	e := fmt.Sprintf("account/oauth-clients/%s", clientID)
	r, err := coupleAPIErrors(req.Put(e))
	if err != nil {
		return nil, err
	}

	return r.Result().(*OAuthClient), nil
}

// DeleteOAuthClient deletes the OAuthClient with the specified id
func (c *Client) DeleteOAuthClient(ctx context.Context, clientID string) error {
	e := fmt.Sprintf("account/oauth-clients/%s", clientID)
	_, err := coupleAPIErrors(c.R(ctx).Delete(e))
	return err
}
