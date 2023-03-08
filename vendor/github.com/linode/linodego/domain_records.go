package linodego

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/go-resty/resty/v2"
)

// DomainRecord represents a DomainRecord object
type DomainRecord struct {
	ID       int              `json:"id"`
	Type     DomainRecordType `json:"type"`
	Name     string           `json:"name"`
	Target   string           `json:"target"`
	Priority int              `json:"priority"`
	Weight   int              `json:"weight"`
	Port     int              `json:"port"`
	Service  *string          `json:"service"`
	Protocol *string          `json:"protocol"`
	TTLSec   int              `json:"ttl_sec"`
	Tag      *string          `json:"tag"`
}

// DomainRecordCreateOptions fields are those accepted by CreateDomainRecord
type DomainRecordCreateOptions struct {
	Type     DomainRecordType `json:"type"`
	Name     string           `json:"name"`
	Target   string           `json:"target"`
	Priority *int             `json:"priority,omitempty"`
	Weight   *int             `json:"weight,omitempty"`
	Port     *int             `json:"port,omitempty"`
	Service  *string          `json:"service,omitempty"`
	Protocol *string          `json:"protocol,omitempty"`
	TTLSec   int              `json:"ttl_sec,omitempty"` // 0 is not accepted by Linode, so can be omitted
	Tag      *string          `json:"tag,omitempty"`
}

// DomainRecordUpdateOptions fields are those accepted by UpdateDomainRecord
type DomainRecordUpdateOptions struct {
	Type     DomainRecordType `json:"type,omitempty"`
	Name     string           `json:"name,omitempty"`
	Target   string           `json:"target,omitempty"`
	Priority *int             `json:"priority,omitempty"` // 0 is valid, so omit only nil values
	Weight   *int             `json:"weight,omitempty"`   // 0 is valid, so omit only nil values
	Port     *int             `json:"port,omitempty"`     // 0 is valid to spec, so omit only nil values
	Service  *string          `json:"service,omitempty"`
	Protocol *string          `json:"protocol,omitempty"`
	TTLSec   int              `json:"ttl_sec,omitempty"` // 0 is not accepted by Linode, so can be omitted
	Tag      *string          `json:"tag,omitempty"`
}

// DomainRecordType constants start with RecordType and include Linode API Domain Record Types
type DomainRecordType string

// DomainRecordType contants are the DNS record types a DomainRecord can assign
const (
	RecordTypeA     DomainRecordType = "A"
	RecordTypeAAAA  DomainRecordType = "AAAA"
	RecordTypeNS    DomainRecordType = "NS"
	RecordTypeMX    DomainRecordType = "MX"
	RecordTypeCNAME DomainRecordType = "CNAME"
	RecordTypeTXT   DomainRecordType = "TXT"
	RecordTypeSRV   DomainRecordType = "SRV"
	RecordTypePTR   DomainRecordType = "PTR"
	RecordTypeCAA   DomainRecordType = "CAA"
)

// GetUpdateOptions converts a DomainRecord to DomainRecordUpdateOptions for use in UpdateDomainRecord
func (d DomainRecord) GetUpdateOptions() (du DomainRecordUpdateOptions) {
	du.Type = d.Type
	du.Name = d.Name
	du.Target = d.Target
	du.Priority = copyInt(&d.Priority)
	du.Weight = copyInt(&d.Weight)
	du.Port = copyInt(&d.Port)
	du.Service = copyString(d.Service)
	du.Protocol = copyString(d.Protocol)
	du.TTLSec = d.TTLSec
	du.Tag = copyString(d.Tag)

	return
}

// DomainRecordsPagedResponse represents a paginated DomainRecord API response
type DomainRecordsPagedResponse struct {
	*PageOptions
	Data []DomainRecord `json:"data"`
}

// endpoint gets the endpoint URL for InstanceConfig
func (DomainRecordsPagedResponse) endpoint(ids ...any) string {
	id, _ := ids[0].(int)
	return fmt.Sprintf("domains/%d/records", id)
}

func (resp *DomainRecordsPagedResponse) castResult(r *resty.Request, e string) (int, int, error) {
	res, err := coupleAPIErrors(r.SetResult(DomainRecordsPagedResponse{}).Get(e))
	if err != nil {
		return 0, 0, err
	}
	castedRes := res.Result().(*DomainRecordsPagedResponse)
	resp.Data = append(resp.Data, castedRes.Data...)
	return castedRes.Pages, castedRes.Results, nil
}

// ListDomainRecords lists DomainRecords
func (c *Client) ListDomainRecords(ctx context.Context, domainID int, opts *ListOptions) ([]DomainRecord, error) {
	response := DomainRecordsPagedResponse{}
	err := c.listHelper(ctx, &response, opts, domainID)
	if err != nil {
		return nil, err
	}
	return response.Data, nil
}

// GetDomainRecord gets the domainrecord with the provided ID
func (c *Client) GetDomainRecord(ctx context.Context, domainID int, recordID int) (*DomainRecord, error) {
	req := c.R(ctx).SetResult(&DomainRecord{})
	e := fmt.Sprintf("domains/%d/records/%d", domainID, recordID)
	r, err := coupleAPIErrors(req.Get(e))
	if err != nil {
		return nil, err
	}
	return r.Result().(*DomainRecord), nil
}

// CreateDomainRecord creates a DomainRecord
func (c *Client) CreateDomainRecord(ctx context.Context, domainID int, opts DomainRecordCreateOptions) (*DomainRecord, error) {
	body, err := json.Marshal(opts)
	if err != nil {
		return nil, err
	}

	e := fmt.Sprintf("domains/%d/records", domainID)
	req := c.R(ctx).SetResult(&DomainRecord{}).SetBody(string(body))
	r, err := coupleAPIErrors(req.Post(e))
	if err != nil {
		return nil, err
	}

	return r.Result().(*DomainRecord), nil
}

// UpdateDomainRecord updates the DomainRecord with the specified id
func (c *Client) UpdateDomainRecord(ctx context.Context, domainID int, recordID int, opts DomainRecordUpdateOptions) (*DomainRecord, error) {
	body, err := json.Marshal(opts)
	if err != nil {
		return nil, err
	}

	e := fmt.Sprintf("domains/%d/records/%d", domainID, recordID)
	req := c.R(ctx).SetResult(&DomainRecord{}).SetBody(string(body))
	r, err := coupleAPIErrors(req.Put(e))
	if err != nil {
		return nil, err
	}

	return r.Result().(*DomainRecord), nil
}

// DeleteDomainRecord deletes the DomainRecord with the specified id
func (c *Client) DeleteDomainRecord(ctx context.Context, domainID int, recordID int) error {
	e := fmt.Sprintf("domains/%d/records/%d", domainID, recordID)
	_, err := coupleAPIErrors(c.R(ctx).Delete(e))
	return err
}
