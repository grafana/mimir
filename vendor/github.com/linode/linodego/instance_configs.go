package linodego

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/linode/linodego/internal/parseabletime"
)

// InstanceConfig represents all of the settings that control the boot and run configuration of a Linode Instance
type InstanceConfig struct {
	ID          int                       `json:"id"`
	Label       string                    `json:"label"`
	Comments    string                    `json:"comments"`
	Devices     *InstanceConfigDeviceMap  `json:"devices"`
	Helpers     *InstanceConfigHelpers    `json:"helpers"`
	Interfaces  []InstanceConfigInterface `json:"interfaces"`
	MemoryLimit int                       `json:"memory_limit"`
	Kernel      string                    `json:"kernel"`
	InitRD      *int                      `json:"init_rd"`
	RootDevice  string                    `json:"root_device"`
	RunLevel    string                    `json:"run_level"`
	VirtMode    string                    `json:"virt_mode"`
	Created     *time.Time                `json:"-"`
	Updated     *time.Time                `json:"-"`
}

// InstanceConfigDevice contains either the DiskID or VolumeID assigned to a Config Device
type InstanceConfigDevice struct {
	DiskID   int `json:"disk_id,omitempty"`
	VolumeID int `json:"volume_id,omitempty"`
}

// InstanceConfigDeviceMap contains SDA-SDH InstanceConfigDevice settings
type InstanceConfigDeviceMap struct {
	SDA *InstanceConfigDevice `json:"sda,omitempty"`
	SDB *InstanceConfigDevice `json:"sdb,omitempty"`
	SDC *InstanceConfigDevice `json:"sdc,omitempty"`
	SDD *InstanceConfigDevice `json:"sdd,omitempty"`
	SDE *InstanceConfigDevice `json:"sde,omitempty"`
	SDF *InstanceConfigDevice `json:"sdf,omitempty"`
	SDG *InstanceConfigDevice `json:"sdg,omitempty"`
	SDH *InstanceConfigDevice `json:"sdh,omitempty"`
}

// InstanceConfigHelpers are Instance Config options that control Linux distribution specific tweaks
type InstanceConfigHelpers struct {
	UpdateDBDisabled  bool `json:"updatedb_disabled"`
	Distro            bool `json:"distro"`
	ModulesDep        bool `json:"modules_dep"`
	Network           bool `json:"network"`
	DevTmpFsAutomount bool `json:"devtmpfs_automount"`
}

// ConfigInterfacePurpose options start with InterfacePurpose and include all known interface purpose types
type ConfigInterfacePurpose string

const (
	InterfacePurposePublic ConfigInterfacePurpose = "public"
	InterfacePurposeVLAN   ConfigInterfacePurpose = "vlan"
)

// InstanceConfigInterface contains information about a configuration's network interface
type InstanceConfigInterface struct {
	IPAMAddress string                 `json:"ipam_address"`
	Label       string                 `json:"label"`
	Purpose     ConfigInterfacePurpose `json:"purpose"`
}

// InstanceConfigsPagedResponse represents a paginated InstanceConfig API response
type InstanceConfigsPagedResponse struct {
	*PageOptions
	Data []InstanceConfig `json:"data"`
}

// InstanceConfigCreateOptions are InstanceConfig settings that can be used at creation
type InstanceConfigCreateOptions struct {
	Label       string                    `json:"label,omitempty"`
	Comments    string                    `json:"comments,omitempty"`
	Devices     InstanceConfigDeviceMap   `json:"devices"`
	Helpers     *InstanceConfigHelpers    `json:"helpers,omitempty"`
	Interfaces  []InstanceConfigInterface `json:"interfaces"`
	MemoryLimit int                       `json:"memory_limit,omitempty"`
	Kernel      string                    `json:"kernel,omitempty"`
	InitRD      int                       `json:"init_rd,omitempty"`
	RootDevice  *string                   `json:"root_device,omitempty"`
	RunLevel    string                    `json:"run_level,omitempty"`
	VirtMode    string                    `json:"virt_mode,omitempty"`
}

// InstanceConfigUpdateOptions are InstanceConfig settings that can be used in updates
type InstanceConfigUpdateOptions struct {
	Label      string                    `json:"label,omitempty"`
	Comments   string                    `json:"comments"`
	Devices    *InstanceConfigDeviceMap  `json:"devices,omitempty"`
	Helpers    *InstanceConfigHelpers    `json:"helpers,omitempty"`
	Interfaces []InstanceConfigInterface `json:"interfaces"`
	// MemoryLimit 0 means unlimitted, this is not omitted
	MemoryLimit int    `json:"memory_limit"`
	Kernel      string `json:"kernel,omitempty"`
	// InitRD is nullable, permit the sending of null
	InitRD     *int   `json:"init_rd"`
	RootDevice string `json:"root_device,omitempty"`
	RunLevel   string `json:"run_level,omitempty"`
	VirtMode   string `json:"virt_mode,omitempty"`
}

// UnmarshalJSON implements the json.Unmarshaler interface
func (i *InstanceConfig) UnmarshalJSON(b []byte) error {
	type Mask InstanceConfig

	p := struct {
		*Mask
		Created *parseabletime.ParseableTime `json:"created"`
		Updated *parseabletime.ParseableTime `json:"updated"`
	}{
		Mask: (*Mask)(i),
	}

	if err := json.Unmarshal(b, &p); err != nil {
		return err
	}

	i.Created = (*time.Time)(p.Created)
	i.Updated = (*time.Time)(p.Updated)

	return nil
}

// GetCreateOptions converts a InstanceConfig to InstanceConfigCreateOptions for use in CreateInstanceConfig
func (i InstanceConfig) GetCreateOptions() InstanceConfigCreateOptions {
	initrd := 0
	if i.InitRD != nil {
		initrd = *i.InitRD
	}
	return InstanceConfigCreateOptions{
		Label:       i.Label,
		Comments:    i.Comments,
		Devices:     *i.Devices,
		Helpers:     i.Helpers,
		Interfaces:  i.Interfaces,
		MemoryLimit: i.MemoryLimit,
		Kernel:      i.Kernel,
		InitRD:      initrd,
		RootDevice:  copyString(&i.RootDevice),
		RunLevel:    i.RunLevel,
		VirtMode:    i.VirtMode,
	}
}

// GetUpdateOptions converts a InstanceConfig to InstanceConfigUpdateOptions for use in UpdateInstanceConfig
func (i InstanceConfig) GetUpdateOptions() InstanceConfigUpdateOptions {
	return InstanceConfigUpdateOptions{
		Label:       i.Label,
		Comments:    i.Comments,
		Devices:     i.Devices,
		Helpers:     i.Helpers,
		Interfaces:  i.Interfaces,
		MemoryLimit: i.MemoryLimit,
		Kernel:      i.Kernel,
		InitRD:      copyInt(i.InitRD),
		RootDevice:  i.RootDevice,
		RunLevel:    i.RunLevel,
		VirtMode:    i.VirtMode,
	}
}

// endpoint gets the endpoint URL for InstanceConfigs of a given Instance
func (InstanceConfigsPagedResponse) endpoint(ids ...any) string {
	id := ids[0].(int)
	return fmt.Sprintf("linode/instances/%d/configs", id)
}

func (resp *InstanceConfigsPagedResponse) castResult(r *resty.Request, e string) (int, int, error) {
	res, err := coupleAPIErrors(r.SetResult(InstanceConfigsPagedResponse{}).Get(e))
	if err != nil {
		return 0, 0, err
	}
	castedRes := res.Result().(*InstanceConfigsPagedResponse)
	resp.Data = append(resp.Data, castedRes.Data...)
	return castedRes.Pages, castedRes.Results, nil
}

// ListInstanceConfigs lists InstanceConfigs
func (c *Client) ListInstanceConfigs(ctx context.Context, linodeID int, opts *ListOptions) ([]InstanceConfig, error) {
	response := InstanceConfigsPagedResponse{}
	err := c.listHelper(ctx, &response, opts, linodeID)
	if err != nil {
		return nil, err
	}
	return response.Data, nil
}

// GetInstanceConfig gets the template with the provided ID
func (c *Client) GetInstanceConfig(ctx context.Context, linodeID int, configID int) (*InstanceConfig, error) {
	e := fmt.Sprintf("linode/instances/%d/configs/%d", linodeID, configID)
	req := c.R(ctx).SetResult(&InstanceConfig{})
	r, err := coupleAPIErrors(req.Get(e))
	if err != nil {
		return nil, err
	}
	return r.Result().(*InstanceConfig), nil
}

// CreateInstanceConfig creates a new InstanceConfig for the given Instance
func (c *Client) CreateInstanceConfig(ctx context.Context, linodeID int, opts InstanceConfigCreateOptions) (*InstanceConfig, error) {
	body, err := json.Marshal(opts)
	if err != nil {
		return nil, err
	}

	e := fmt.Sprintf("linode/instances/%d/configs", linodeID)
	req := c.R(ctx).SetResult(&InstanceConfig{}).SetBody(string(body))
	r, err := coupleAPIErrors(req.Post(e))
	if err != nil {
		return nil, err
	}

	return r.Result().(*InstanceConfig), nil
}

// UpdateInstanceConfig update an InstanceConfig for the given Instance
func (c *Client) UpdateInstanceConfig(ctx context.Context, linodeID int, configID int, opts InstanceConfigUpdateOptions) (*InstanceConfig, error) {
	body, err := json.Marshal(opts)
	if err != nil {
		return nil, err
	}

	e := fmt.Sprintf("linode/instances/%d/configs/%d", linodeID, configID)
	req := c.R(ctx).SetResult(&InstanceConfig{}).SetBody(body)
	r, err := coupleAPIErrors(req.Put(e))
	if err != nil {
		return nil, err
	}

	return r.Result().(*InstanceConfig), nil
}

// RenameInstanceConfig renames an InstanceConfig
func (c *Client) RenameInstanceConfig(ctx context.Context, linodeID int, configID int, label string) (*InstanceConfig, error) {
	return c.UpdateInstanceConfig(ctx, linodeID, configID, InstanceConfigUpdateOptions{Label: label})
}

// DeleteInstanceConfig deletes a Linode InstanceConfig
func (c *Client) DeleteInstanceConfig(ctx context.Context, linodeID int, configID int) error {
	e := fmt.Sprintf("linode/instances/%d/configs/%d", linodeID, configID)
	_, err := coupleAPIErrors(c.R(ctx).Delete(e))
	return err
}
