package linodego

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/linode/linodego/internal/parseabletime"
)

// ImageStatus represents the status of an Image.
type ImageStatus string

// ImageStatus options start with ImageStatus and include all Image statuses
const (
	ImageStatusCreating      ImageStatus = "creating"
	ImageStatusPendingUpload ImageStatus = "pending_upload"
	ImageStatusAvailable     ImageStatus = "available"
)

// Image represents a deployable Image object for use with Linode Instances
type Image struct {
	ID          string      `json:"id"`
	CreatedBy   string      `json:"created_by"`
	Label       string      `json:"label"`
	Description string      `json:"description"`
	Type        string      `json:"type"`
	Vendor      string      `json:"vendor"`
	Status      ImageStatus `json:"status"`
	Size        int         `json:"size"`
	IsPublic    bool        `json:"is_public"`
	Deprecated  bool        `json:"deprecated"`
	Created     *time.Time  `json:"-"`
	Expiry      *time.Time  `json:"-"`
}

// ImageCreateOptions fields are those accepted by CreateImage
type ImageCreateOptions struct {
	DiskID      int    `json:"disk_id"`
	Label       string `json:"label"`
	Description string `json:"description,omitempty"`
}

// ImageUpdateOptions fields are those accepted by UpdateImage
type ImageUpdateOptions struct {
	Label       string  `json:"label,omitempty"`
	Description *string `json:"description,omitempty"`
}

// ImageCreateUploadResponse fields are those returned by CreateImageUpload
type ImageCreateUploadResponse struct {
	Image    *Image `json:"image"`
	UploadTo string `json:"upload_to"`
}

// ImageCreateUploadOptions fields are those accepted by CreateImageUpload
type ImageCreateUploadOptions struct {
	Region      string `json:"region"`
	Label       string `json:"label"`
	Description string `json:"description,omitempty"`
}

// ImageUploadOptions fields are those accepted by UploadImage
type ImageUploadOptions struct {
	Region      string `json:"region"`
	Label       string `json:"label"`
	Description string `json:"description,omitempty"`
	Image       io.Reader
}

// UnmarshalJSON implements the json.Unmarshaler interface
func (i *Image) UnmarshalJSON(b []byte) error {
	type Mask Image

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

// GetUpdateOptions converts an Image to ImageUpdateOptions for use in UpdateImage
func (i Image) GetUpdateOptions() (iu ImageUpdateOptions) {
	iu.Label = i.Label
	iu.Description = copyString(&i.Description)
	return
}

// ImagesPagedResponse represents a linode API response for listing of images
type ImagesPagedResponse struct {
	*PageOptions
	Data []Image `json:"data"`
}

func (ImagesPagedResponse) endpoint(_ ...any) string {
	return "images"
}

func (resp *ImagesPagedResponse) castResult(r *resty.Request, e string) (int, int, error) {
	res, err := coupleAPIErrors(r.SetResult(ImagesPagedResponse{}).Get(e))
	if err != nil {
		return 0, 0, err
	}
	castedRes := res.Result().(*ImagesPagedResponse)
	resp.Data = append(resp.Data, castedRes.Data...)
	return castedRes.Pages, castedRes.Results, nil
}

// ListImages lists Images
func (c *Client) ListImages(ctx context.Context, opts *ListOptions) ([]Image, error) {
	response := ImagesPagedResponse{}
	err := c.listHelper(ctx, &response, opts)
	if err != nil {
		return nil, err
	}
	return response.Data, nil
}

// GetImage gets the Image with the provided ID
func (c *Client) GetImage(ctx context.Context, imageID string) (*Image, error) {
	e := fmt.Sprintf("images/%s", imageID)
	req := c.R(ctx).SetResult(&Image{})
	r, err := coupleAPIErrors(req.Get(e))
	if err != nil {
		return nil, err
	}
	return r.Result().(*Image), nil
}

// CreateImage creates a Image
func (c *Client) CreateImage(ctx context.Context, opts ImageCreateOptions) (*Image, error) {
	body, err := json.Marshal(opts)
	if err != nil {
		return nil, err
	}

	e := "images"
	req := c.R(ctx).SetResult(&Image{}).SetBody(string(body))
	r, err := coupleAPIErrors(req.Post(e))
	if err != nil {
		return nil, err
	}
	return r.Result().(*Image), nil
}

// UpdateImage updates the Image with the specified id
func (c *Client) UpdateImage(ctx context.Context, imageID string, opts ImageUpdateOptions) (*Image, error) {
	body, err := json.Marshal(opts)
	if err != nil {
		return nil, err
	}

	e := fmt.Sprintf("images/%s", imageID)
	req := c.R(ctx).SetResult(&Image{}).SetBody(string(body))
	r, err := coupleAPIErrors(req.Put(e))
	if err != nil {
		return nil, err
	}
	return r.Result().(*Image), nil
}

// DeleteImage deletes the Image with the specified id
func (c *Client) DeleteImage(ctx context.Context, imageID string) error {
	e := fmt.Sprintf("images/%s", imageID)
	_, err := coupleAPIErrors(c.R(ctx).Delete(e))
	return err
}

// CreateImageUpload creates an Image and an upload URL
func (c *Client) CreateImageUpload(ctx context.Context, opts ImageCreateUploadOptions) (*Image, string, error) {
	body, err := json.Marshal(opts)
	if err != nil {
		return nil, "", err
	}

	e := "images/upload"
	req := c.R(ctx).SetResult(&ImageCreateUploadResponse{}).SetBody(string(body))
	r, err := coupleAPIErrors(req.Post(e))
	if err != nil {
		return nil, "", err
	}

	result, ok := r.Result().(*ImageCreateUploadResponse)
	if !ok {
		return nil, "", fmt.Errorf("failed to parse result")
	}

	return result.Image, result.UploadTo, nil
}

// UploadImageToURL uploads the given image to the given upload URL
func (c *Client) UploadImageToURL(ctx context.Context, uploadURL string, image io.Reader) error {
	// Linode-specific headers do not need to be sent to this endpoint
	req := resty.New().SetDebug(c.resty.Debug).R().
		SetContext(ctx).
		SetContentLength(true).
		SetHeader("Content-Type", "application/octet-stream").
		SetBody(image)

	_, err := coupleAPIErrors(req.
		Put(uploadURL))

	return err
}

// UploadImage creates and uploads an image
func (c *Client) UploadImage(ctx context.Context, opts ImageUploadOptions) (*Image, error) {
	image, uploadURL, err := c.CreateImageUpload(ctx, ImageCreateUploadOptions{
		Label:       opts.Label,
		Region:      opts.Region,
		Description: opts.Description,
	})
	if err != nil {
		return nil, err
	}

	return image, c.UploadImageToURL(ctx, uploadURL, opts.Image)
}
