package linodego

/**
 * Pagination and Filtering types and helpers
 */

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/go-resty/resty/v2"
)

// PageOptions are the pagination parameters for List endpoints
type PageOptions struct {
	Page    int `url:"page,omitempty" json:"page"`
	Pages   int `url:"pages,omitempty" json:"pages"`
	Results int `url:"results,omitempty" json:"results"`
}

// ListOptions are the pagination and filtering (TODO) parameters for endpoints
type ListOptions struct {
	*PageOptions
	PageSize int
	Filter   string
}

// NewListOptions simplified construction of ListOptions using only
// the two writable properties, Page and Filter
func NewListOptions(page int, filter string) *ListOptions {
	return &ListOptions{PageOptions: &PageOptions{Page: page}, Filter: filter}
}

// Hash returns the sha256 hash of the provided ListOptions.
// This is necessary for caching purposes.
func (l ListOptions) Hash() (string, error) {
	data, err := json.Marshal(l)
	if err != nil {
		return "", fmt.Errorf("failed to cache ListOptions: %s", err)
	}

	h := sha256.New()

	h.Write(data)

	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

func applyListOptionsToRequest(opts *ListOptions, req *resty.Request) {
	if opts != nil {
		if opts.PageOptions != nil && opts.Page > 0 {
			req.SetQueryParam("page", strconv.Itoa(opts.Page))
		}

		if opts.PageSize > 0 {
			req.SetQueryParam("page_size", strconv.Itoa(opts.PageSize))
		}

		if len(opts.Filter) > 0 {
			req.SetHeader("X-Filter", opts.Filter)
		}
	}
}

type PagedResponse interface {
	endpoint(...any) string
	castResult(*resty.Request, string) (int, int, error)
}

// listHelper abstracts fetching and pagination for GET endpoints that
// do not require any Ids (top level endpoints).
// When opts (or opts.Page) is nil, all pages will be fetched and
// returned in a single (endpoint-specific)PagedResponse
// opts.results and opts.pages will be updated from the API response
func (c *Client) listHelper(ctx context.Context, pager PagedResponse, opts *ListOptions, ids ...any) error {
	req := c.R(ctx)
	applyListOptionsToRequest(opts, req)

	pages, results, err := pager.castResult(req, pager.endpoint(ids...))
	if err != nil {
		return err
	}
	if opts == nil {
		opts = &ListOptions{PageOptions: &PageOptions{Page: 0}}
	}
	if opts.PageOptions == nil {
		opts.PageOptions = &PageOptions{Page: 0}
	}
	if opts.Page == 0 {
		for page := 2; page <= pages; page++ {
			opts.Page = page
			if err := c.listHelper(ctx, pager, opts, ids...); err != nil {
				return err
			}
		}
	}

	opts.Results = results
	opts.Pages = pages
	return nil
}
