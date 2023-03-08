package linodego

import (
	"context"
	"fmt"
	"time"

	"github.com/go-resty/resty/v2"
)

// Region-related endpoints have a custom expiry time as the
// `status` field may update for database outages.
var cacheExpiryTime = time.Minute

// Region represents a linode region object
type Region struct {
	ID           string          `json:"id"`
	Country      string          `json:"country"`
	Capabilities []string        `json:"capabilities"`
	Status       string          `json:"status"`
	Resolvers    RegionResolvers `json:"resolvers"`
}

// RegionResolvers contains the DNS resolvers of a region
type RegionResolvers struct {
	IPv4 string `json:"ipv4"`
	IPv6 string `json:"ipv6"`
}

// RegionsPagedResponse represents a linode API response for listing
type RegionsPagedResponse struct {
	*PageOptions
	Data []Region `json:"data"`
}

// endpoint gets the endpoint URL for Region
func (RegionsPagedResponse) endpoint(_ ...any) string {
	return "regions"
}

func (resp *RegionsPagedResponse) castResult(r *resty.Request, e string) (int, int, error) {
	res, err := coupleAPIErrors(r.SetResult(RegionsPagedResponse{}).Get(e))
	if err != nil {
		return 0, 0, err
	}
	castedRes := res.Result().(*RegionsPagedResponse)
	resp.Data = append(resp.Data, castedRes.Data...)
	return castedRes.Pages, castedRes.Results, nil
}

// ListRegions lists Regions. This endpoint is cached by default.
func (c *Client) ListRegions(ctx context.Context, opts *ListOptions) ([]Region, error) {
	response := RegionsPagedResponse{}

	endpoint, err := generateListCacheURL(response.endpoint(), opts)
	if err != nil {
		return nil, err
	}

	if result := c.getCachedResponse(endpoint); result != nil {
		return result.([]Region), nil
	}

	err = c.listHelper(ctx, &response, opts)
	if err != nil {
		return nil, err
	}

	c.addCachedResponse(endpoint, response.Data, &cacheExpiryTime)

	return response.Data, nil
}

// GetRegion gets the template with the provided ID. This endpoint is cached by default.
func (c *Client) GetRegion(ctx context.Context, regionID string) (*Region, error) {
	e := fmt.Sprintf("regions/%s", regionID)

	if result := c.getCachedResponse(e); result != nil {
		result := result.(Region)
		return &result, nil
	}

	req := c.R(ctx).SetResult(&Region{})
	r, err := coupleAPIErrors(req.Get(e))
	if err != nil {
		return nil, err
	}

	c.addCachedResponse(e, r.Result(), &cacheExpiryTime)

	return r.Result().(*Region), nil
}
