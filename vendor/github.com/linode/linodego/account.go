package linodego

import "context"

// Account associated with the token in use.
type Account struct {
	FirstName         string      `json:"first_name"`
	LastName          string      `json:"last_name"`
	Email             string      `json:"email"`
	Company           string      `json:"company"`
	Address1          string      `json:"address_1"`
	Address2          string      `json:"address_2"`
	Balance           float32     `json:"balance"`
	BalanceUninvoiced float32     `json:"balance_uninvoiced"`
	City              string      `json:"city"`
	State             string      `json:"state"`
	Zip               string      `json:"zip"`
	Country           string      `json:"country"`
	TaxID             string      `json:"tax_id"`
	Phone             string      `json:"phone"`
	CreditCard        *CreditCard `json:"credit_card"`
}

// CreditCard information associated with the Account.
type CreditCard struct {
	LastFour string `json:"last_four"`
	Expiry   string `json:"expiry"`
}

// GetAccount gets the contact and billing information related to the Account.
func (c *Client) GetAccount(ctx context.Context) (*Account, error) {
	e := "account"
	req := c.R(ctx).SetResult(&Account{})
	r, err := coupleAPIErrors(req.Get(e))
	if err != nil {
		return nil, err
	}

	return r.Result().(*Account), nil
}
