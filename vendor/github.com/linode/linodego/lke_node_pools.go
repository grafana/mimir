package linodego

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/go-resty/resty/v2"
)

// LKELinodeStatus constants start with LKELinode and include
// Linode API LKENodePool Linode Status values
type LKELinodeStatus string

// LKENodePoolStatus constants reflect the current status of an LKENodePool
const (
	LKELinodeReady    LKELinodeStatus = "ready"
	LKELinodeNotReady LKELinodeStatus = "not_ready"
)

// LKENodePoolDisk represents a Node disk in an LKENodePool object
type LKENodePoolDisk struct {
	Size int    `json:"size"`
	Type string `json:"type"`
}

type LKENodePoolAutoscaler struct {
	Enabled bool `json:"enabled"`
	Min     int  `json:"min"`
	Max     int  `json:"max"`
}

// LKENodePoolLinode represents a LKENodePoolLinode object
type LKENodePoolLinode struct {
	ID         string          `json:"id"`
	InstanceID int             `json:"instance_id"`
	Status     LKELinodeStatus `json:"status"`
}

// LKENodePool represents a LKENodePool object
type LKENodePool struct {
	ID      int                 `json:"id"`
	Count   int                 `json:"count"`
	Type    string              `json:"type"`
	Disks   []LKENodePoolDisk   `json:"disks"`
	Linodes []LKENodePoolLinode `json:"nodes"`
	Tags    []string            `json:"tags"`

	Autoscaler LKENodePoolAutoscaler `json:"autoscaler"`
}

// LKENodePoolCreateOptions fields are those accepted by CreateLKENodePool
type LKENodePoolCreateOptions struct {
	Count int               `json:"count"`
	Type  string            `json:"type"`
	Disks []LKENodePoolDisk `json:"disks"`
	Tags  []string          `json:"tags"`

	Autoscaler *LKENodePoolAutoscaler `json:"autoscaler,omitempty"`
}

// LKENodePoolUpdateOptions fields are those accepted by UpdateLKENodePoolUpdate
type LKENodePoolUpdateOptions struct {
	Count int       `json:"count,omitempty"`
	Tags  *[]string `json:"tags,omitempty"`

	Autoscaler *LKENodePoolAutoscaler `json:"autoscaler,omitempty"`
}

// GetCreateOptions converts a LKENodePool to LKENodePoolCreateOptions for
// use in CreateLKENodePool
func (l LKENodePool) GetCreateOptions() (o LKENodePoolCreateOptions) {
	o.Count = l.Count
	o.Disks = l.Disks
	o.Tags = l.Tags
	o.Autoscaler = &l.Autoscaler
	return
}

// GetUpdateOptions converts a LKENodePool to LKENodePoolUpdateOptions for use in UpdateLKENodePoolUpdate
func (l LKENodePool) GetUpdateOptions() (o LKENodePoolUpdateOptions) {
	o.Count = l.Count
	o.Tags = &l.Tags
	o.Autoscaler = &l.Autoscaler
	return
}

// LKENodePoolsPagedResponse represents a paginated LKENodePool API response
type LKENodePoolsPagedResponse struct {
	*PageOptions
	Data []LKENodePool `json:"data"`
}

// endpoint gets the endpoint URL for InstanceConfigs of a given Instance
func (LKENodePoolsPagedResponse) endpoint(ids ...any) string {
	id := ids[0].(int)
	return fmt.Sprintf("lke/clusters/%d/pools", id)
}

func (resp *LKENodePoolsPagedResponse) castResult(r *resty.Request, e string) (int, int, error) {
	res, err := coupleAPIErrors(r.SetResult(LKENodePoolsPagedResponse{}).Get(e))
	if err != nil {
		return 0, 0, err
	}
	castedRes := res.Result().(*LKENodePoolsPagedResponse)
	resp.Data = append(resp.Data, castedRes.Data...)
	return castedRes.Pages, castedRes.Results, nil
}

// ListLKENodePools lists LKENodePools
func (c *Client) ListLKENodePools(ctx context.Context, clusterID int, opts *ListOptions) ([]LKENodePool, error) {
	response := LKENodePoolsPagedResponse{}
	err := c.listHelper(ctx, &response, opts, clusterID)
	if err != nil {
		return nil, err
	}

	return response.Data, nil
}

// GetLKENodePool gets the LKENodePool with the provided ID
func (c *Client) GetLKENodePool(ctx context.Context, clusterID, poolID int) (*LKENodePool, error) {
	e := fmt.Sprintf("lke/clusters/%d/pools/%d", clusterID, poolID)
	req := c.R(ctx).SetResult(&LKENodePool{})
	r, err := coupleAPIErrors(req.Get(e))
	if err != nil {
		return nil, err
	}
	return r.Result().(*LKENodePool), nil
}

// CreateLKENodePool creates a LKENodePool
func (c *Client) CreateLKENodePool(ctx context.Context, clusterID int, opts LKENodePoolCreateOptions) (*LKENodePool, error) {
	body, err := json.Marshal(opts)
	if err != nil {
		return nil, err
	}

	e := fmt.Sprintf("lke/clusters/%d/pools", clusterID)
	req := c.R(ctx).SetResult(&LKENodePool{}).SetBody(string(body))
	r, err := coupleAPIErrors(req.Post(e))
	if err != nil {
		return nil, err
	}
	return r.Result().(*LKENodePool), nil
}

// UpdateLKENodePool updates the LKENodePool with the specified id
func (c *Client) UpdateLKENodePool(ctx context.Context, clusterID, poolID int, opts LKENodePoolUpdateOptions) (*LKENodePool, error) {
	body, err := json.Marshal(opts)
	if err != nil {
		return nil, err
	}

	e := fmt.Sprintf("lke/clusters/%d/pools/%d", clusterID, poolID)
	req := c.R(ctx).SetResult(&LKENodePool{}).SetBody(string(body))
	r, err := coupleAPIErrors(req.Put(e))
	if err != nil {
		return nil, err
	}
	return r.Result().(*LKENodePool), nil
}

// DeleteLKENodePool deletes the LKENodePool with the specified id
func (c *Client) DeleteLKENodePool(ctx context.Context, clusterID, poolID int) error {
	e := fmt.Sprintf("lke/clusters/%d/pools/%d", clusterID, poolID)
	_, err := coupleAPIErrors(c.R(ctx).Delete(e))
	return err
}

// DeleteLKENodePoolNode deletes a given node from a node pool
func (c *Client) DeleteLKENodePoolNode(ctx context.Context, clusterID int, nodeID string) error {
	e := fmt.Sprintf("lke/clusters/%d/nodes/%s", clusterID, nodeID)
	_, err := coupleAPIErrors(c.R(ctx).Delete(e))
	return err
}
