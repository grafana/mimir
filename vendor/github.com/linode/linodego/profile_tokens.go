package linodego

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/linode/linodego/internal/parseabletime"
)

// Token represents a Token object
type Token struct {
	// This token's unique ID, which can be used to revoke it.
	ID int `json:"id"`

	// The scopes this token was created with. These define what parts of the Account the token can be used to access. Many command-line tools, such as the Linode CLI, require tokens with access to *. Tokens with more restrictive scopes are generally more secure.
	// Valid values are "*" or a comma separated list of scopes https://developers.linode.com/api/v4/#o-auth
	Scopes string `json:"scopes"`

	// This token's label. This is for display purposes only, but can be used to more easily track what you're using each token for. (1-100 Characters)
	Label string `json:"label"`

	// The token used to access the API. When the token is created, the full token is returned here. Otherwise, only the first 16 characters are returned.
	Token string `json:"token"`

	// The date and time this token was created.
	Created *time.Time `json:"-"`

	// When this token will expire. Personal Access Tokens cannot be renewed, so after this time the token will be completely unusable and a new token will need to be generated. Tokens may be created with "null" as their expiry and will never expire unless revoked.
	Expiry *time.Time `json:"-"`
}

// TokenCreateOptions fields are those accepted by CreateToken
type TokenCreateOptions struct {
	// The scopes this token was created with. These define what parts of the Account the token can be used to access. Many command-line tools, such as the Linode CLI, require tokens with access to *. Tokens with more restrictive scopes are generally more secure.
	Scopes string `json:"scopes"`

	// This token's label. This is for display purposes only, but can be used to more easily track what you're using each token for. (1-100 Characters)
	Label string `json:"label"`

	// When this token will expire. Personal Access Tokens cannot be renewed, so after this time the token will be completely unusable and a new token will need to be generated. Tokens may be created with "null" as their expiry and will never expire unless revoked.
	Expiry *time.Time `json:"expiry"`
}

// TokenUpdateOptions fields are those accepted by UpdateToken
type TokenUpdateOptions struct {
	// This token's label. This is for display purposes only, but can be used to more easily track what you're using each token for. (1-100 Characters)
	Label string `json:"label"`
}

// UnmarshalJSON implements the json.Unmarshaler interface
func (i *Token) UnmarshalJSON(b []byte) error {
	type Mask Token

	p := struct {
		*Mask
		Created *parseabletime.ParseableTime `json:"created"`
		Expiry  *parseabletime.ParseableTime `json:"expiry"`
	}{
		Mask: (*Mask)(i),
	}

	if err := json.Unmarshal(b, &p); err != nil {
		return err
	}

	i.Created = (*time.Time)(p.Created)
	i.Expiry = (*time.Time)(p.Expiry)

	return nil
}

// GetCreateOptions converts a Token to TokenCreateOptions for use in CreateToken
func (i Token) GetCreateOptions() (o TokenCreateOptions) {
	o.Label = i.Label
	o.Expiry = copyTime(i.Expiry)
	o.Scopes = i.Scopes
	return
}

// GetUpdateOptions converts a Token to TokenUpdateOptions for use in UpdateToken
func (i Token) GetUpdateOptions() (o TokenUpdateOptions) {
	o.Label = i.Label
	return
}

// TokensPagedResponse represents a paginated Token API response
type TokensPagedResponse struct {
	*PageOptions
	Data []Token `json:"data"`
}

// endpoint gets the endpoint URL for Token
func (TokensPagedResponse) endpoint(_ ...any) string {
	return "profile/tokens"
}

func (resp *TokensPagedResponse) castResult(r *resty.Request, e string) (int, int, error) {
	res, err := coupleAPIErrors(r.SetResult(TokensPagedResponse{}).Get(e))
	if err != nil {
		return 0, 0, err
	}
	castedRes := res.Result().(*TokensPagedResponse)
	resp.Data = append(resp.Data, castedRes.Data...)
	return castedRes.Pages, castedRes.Results, nil
}

// ListTokens lists Tokens
func (c *Client) ListTokens(ctx context.Context, opts *ListOptions) ([]Token, error) {
	response := TokensPagedResponse{}
	err := c.listHelper(ctx, &response, opts)
	if err != nil {
		return nil, err
	}
	return response.Data, nil
}

// GetToken gets the token with the provided ID
func (c *Client) GetToken(ctx context.Context, tokenID int) (*Token, error) {
	e := fmt.Sprintf("profile/tokens/%d", tokenID)
	req := c.R(ctx).SetResult(&Token{})
	r, err := coupleAPIErrors(req.Get(e))
	if err != nil {
		return nil, err
	}
	return r.Result().(*Token), nil
}

// CreateToken creates a Token
func (c *Client) CreateToken(ctx context.Context, opts TokenCreateOptions) (*Token, error) {
	// Format the Time as a string to meet the ISO8601 requirement
	createOptsFixed := struct {
		Label  string  `json:"label"`
		Scopes string  `json:"scopes"`
		Expiry *string `json:"expiry"`
	}{}
	createOptsFixed.Label = opts.Label
	createOptsFixed.Scopes = opts.Scopes
	if opts.Expiry != nil {
		iso8601Expiry := opts.Expiry.UTC().Format("2006-01-02T15:04:05")
		createOptsFixed.Expiry = &iso8601Expiry
	}

	body, err := json.Marshal(createOptsFixed)
	if err != nil {
		return nil, err
	}

	e := "profile/tokens"
	req := c.R(ctx).SetResult(&Token{}).SetBody(string(body))
	r, err := coupleAPIErrors(req.Post(e))
	if err != nil {
		return nil, err
	}
	return r.Result().(*Token), nil
}

// UpdateToken updates the Token with the specified id
func (c *Client) UpdateToken(ctx context.Context, tokenID int, opts TokenUpdateOptions) (*Token, error) {
	body, err := json.Marshal(opts)
	if err != nil {
		return nil, err
	}

	e := fmt.Sprintf("profile/tokens/%d", tokenID)
	req := c.R(ctx).SetResult(&Token{}).SetBody(string(body))
	r, err := coupleAPIErrors(req.Put(e))
	if err != nil {
		return nil, err
	}
	return r.Result().(*Token), nil
}

// DeleteToken deletes the Token with the specified id
func (c *Client) DeleteToken(ctx context.Context, tokenID int) error {
	e := fmt.Sprintf("profile/tokens/%d", tokenID)
	_, err := coupleAPIErrors(c.R(ctx).Delete(e))
	return err
}
