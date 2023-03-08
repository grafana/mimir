package linodego

import (
	"context"
	"fmt"

	"github.com/go-resty/resty/v2"
)

// InstanceVolumesPagedResponse represents a paginated InstanceVolume API response
type InstanceVolumesPagedResponse struct {
	*PageOptions
	Data []Volume `json:"data"`
}

// endpoint gets the endpoint URL for InstanceVolume
func (InstanceVolumesPagedResponse) endpoint(ids ...any) string {
	id := ids[0].(int)
	return fmt.Sprintf("linode/instances/%d/volumes", id)
}

func (resp *InstanceVolumesPagedResponse) castResult(r *resty.Request, e string) (int, int, error) {
	res, err := coupleAPIErrors(r.SetResult(InstanceVolumesPagedResponse{}).Get(e))
	if err != nil {
		return 0, 0, err
	}
	castedRes := res.Result().(*InstanceVolumesPagedResponse)
	resp.Data = append(resp.Data, castedRes.Data...)
	return castedRes.Pages, castedRes.Results, nil
}

// ListInstanceVolumes lists InstanceVolumes
func (c *Client) ListInstanceVolumes(ctx context.Context, linodeID int, opts *ListOptions) ([]Volume, error) {
	response := InstanceVolumesPagedResponse{}
	err := c.listHelper(ctx, &response, opts, linodeID)
	if err != nil {
		return nil, err
	}
	return response.Data, nil
}
