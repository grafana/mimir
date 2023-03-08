package linodego

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/linode/linodego/internal/parseabletime"
)

type MySQLDatabaseTarget string

type MySQLDatabaseMaintenanceWindow = DatabaseMaintenanceWindow

const (
	MySQLDatabaseTargetPrimary   MySQLDatabaseTarget = "primary"
	MySQLDatabaseTargetSecondary MySQLDatabaseTarget = "secondary"
)

// A MySQLDatabase is a instance of Linode MySQL Managed Databases
type MySQLDatabase struct {
	ID              int                       `json:"id"`
	Status          DatabaseStatus            `json:"status"`
	Label           string                    `json:"label"`
	Hosts           DatabaseHost              `json:"hosts"`
	Region          string                    `json:"region"`
	Type            string                    `json:"type"`
	Engine          string                    `json:"engine"`
	Version         string                    `json:"version"`
	ClusterSize     int                       `json:"cluster_size"`
	ReplicationType string                    `json:"replication_type"`
	SSLConnection   bool                      `json:"ssl_connection"`
	Encrypted       bool                      `json:"encrypted"`
	AllowList       []string                  `json:"allow_list"`
	InstanceURI     string                    `json:"instance_uri"`
	Created         *time.Time                `json:"-"`
	Updated         *time.Time                `json:"-"`
	Updates         DatabaseMaintenanceWindow `json:"updates"`
}

func (d *MySQLDatabase) UnmarshalJSON(b []byte) error {
	type Mask MySQLDatabase

	p := struct {
		*Mask
		Created *parseabletime.ParseableTime `json:"created"`
		Updated *parseabletime.ParseableTime `json:"updated"`
	}{
		Mask: (*Mask)(d),
	}

	if err := json.Unmarshal(b, &p); err != nil {
		return err
	}

	d.Created = (*time.Time)(p.Created)
	d.Updated = (*time.Time)(p.Updated)
	return nil
}

// MySQLCreateOptions fields are used when creating a new MySQL Database
type MySQLCreateOptions struct {
	Label           string   `json:"label"`
	Region          string   `json:"region"`
	Type            string   `json:"type"`
	Engine          string   `json:"engine"`
	AllowList       []string `json:"allow_list,omitempty"`
	ReplicationType string   `json:"replication_type,omitempty"`
	ClusterSize     int      `json:"cluster_size,omitempty"`
	Encrypted       bool     `json:"encrypted,omitempty"`
	SSLConnection   bool     `json:"ssl_connection,omitempty"`
}

// MySQLUpdateOptions fields are used when altering the existing MySQL Database
type MySQLUpdateOptions struct {
	Label     string                     `json:"label,omitempty"`
	AllowList *[]string                  `json:"allow_list,omitempty"`
	Updates   *DatabaseMaintenanceWindow `json:"updates,omitempty"`
}

// MySQLDatabaseBackup is information for interacting with a backup for the existing MySQL Database
type MySQLDatabaseBackup struct {
	ID      int        `json:"id"`
	Label   string     `json:"label"`
	Type    string     `json:"type"`
	Created *time.Time `json:"-"`
}

// MySQLBackupCreateOptions are options used for CreateMySQLDatabaseBackup(...)
type MySQLBackupCreateOptions struct {
	Label  string              `json:"label"`
	Target MySQLDatabaseTarget `json:"target"`
}

func (d *MySQLDatabaseBackup) UnmarshalJSON(b []byte) error {
	type Mask MySQLDatabaseBackup

	p := struct {
		*Mask
		Created *parseabletime.ParseableTime `json:"created"`
	}{
		Mask: (*Mask)(d),
	}

	if err := json.Unmarshal(b, &p); err != nil {
		return err
	}

	d.Created = (*time.Time)(p.Created)
	return nil
}

type MySQLDatabasesPagedResponse struct {
	*PageOptions
	Data []MySQLDatabase `json:"data"`
}

func (MySQLDatabasesPagedResponse) endpoint(_ ...any) string {
	return "databases/mysql/instances"
}

func (resp *MySQLDatabasesPagedResponse) castResult(r *resty.Request, e string) (int, int, error) {
	res, err := coupleAPIErrors(r.SetResult(MySQLDatabasesPagedResponse{}).Get(e))
	if err != nil {
		return 0, 0, err
	}
	castedRes := res.Result().(*MySQLDatabasesPagedResponse)
	resp.Data = append(resp.Data, castedRes.Data...)
	return castedRes.Pages, castedRes.Results, nil
}

// MySQLDatabaseCredential is the Root Credentials to access the Linode Managed Database
type MySQLDatabaseCredential struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

// MySQLDatabaseSSL is the SSL Certificate to access the Linode Managed MySQL Database
type MySQLDatabaseSSL struct {
	CACertificate []byte `json:"ca_certificate"`
}

// ListMySQLDatabases lists all MySQL Databases associated with the account
func (c *Client) ListMySQLDatabases(ctx context.Context, opts *ListOptions) ([]MySQLDatabase, error) {
	response := MySQLDatabasesPagedResponse{}

	err := c.listHelper(ctx, &response, opts)
	if err != nil {
		return nil, err
	}

	return response.Data, nil
}

type MySQLDatabaseBackupsPagedResponse struct {
	*PageOptions
	Data []MySQLDatabaseBackup `json:"data"`
}

func (MySQLDatabaseBackupsPagedResponse) endpoint(ids ...any) string {
	id := ids[0].(int)
	return fmt.Sprintf("databases/mysql/instances/%d/backups", id)
}

func (resp *MySQLDatabaseBackupsPagedResponse) castResult(r *resty.Request, e string) (int, int, error) {
	res, err := coupleAPIErrors(r.SetResult(MySQLDatabaseBackupsPagedResponse{}).Get(e))
	if err != nil {
		return 0, 0, err
	}
	castedRes := res.Result().(*MySQLDatabaseBackupsPagedResponse)
	resp.Data = append(resp.Data, castedRes.Data...)
	return castedRes.Pages, castedRes.Results, nil
}

// ListMySQLDatabaseBackups lists all MySQL Database Backups associated with the given MySQL Database
func (c *Client) ListMySQLDatabaseBackups(ctx context.Context, databaseID int, opts *ListOptions) ([]MySQLDatabaseBackup, error) {
	response := MySQLDatabaseBackupsPagedResponse{}

	err := c.listHelper(ctx, &response, opts, databaseID)
	if err != nil {
		return nil, err
	}

	return response.Data, nil
}

// GetMySQLDatabase returns a single MySQL Database matching the id
func (c *Client) GetMySQLDatabase(ctx context.Context, databaseID int) (*MySQLDatabase, error) {
	e := fmt.Sprintf("databases/mysql/instances/%d", databaseID)
	req := c.R(ctx).SetResult(&MySQLDatabase{})
	r, err := coupleAPIErrors(req.Get(e))
	if err != nil {
		return nil, err
	}

	return r.Result().(*MySQLDatabase), nil
}

// CreateMySQLDatabase creates a new MySQL Database using the createOpts as configuration, returns the new MySQL Database
func (c *Client) CreateMySQLDatabase(ctx context.Context, opts MySQLCreateOptions) (*MySQLDatabase, error) {
	body, err := json.Marshal(opts)
	if err != nil {
		return nil, err
	}

	e := "databases/mysql/instances"
	req := c.R(ctx).SetResult(&MySQLDatabase{}).SetBody(string(body))
	r, err := coupleAPIErrors(req.Post(e))
	if err != nil {
		return nil, err
	}
	return r.Result().(*MySQLDatabase), nil
}

// DeleteMySQLDatabase deletes an existing MySQL Database with the given id
func (c *Client) DeleteMySQLDatabase(ctx context.Context, databaseID int) error {
	e := fmt.Sprintf("databases/mysql/instances/%d", databaseID)
	_, err := coupleAPIErrors(c.R(ctx).Delete(e))
	return err
}

// UpdateMySQLDatabase updates the given MySQL Database with the provided opts, returns the MySQLDatabase with the new settings
func (c *Client) UpdateMySQLDatabase(ctx context.Context, databaseID int, opts MySQLUpdateOptions) (*MySQLDatabase, error) {
	body, err := json.Marshal(opts)
	if err != nil {
		return nil, err
	}

	e := fmt.Sprintf("databases/mysql/instances/%d", databaseID)
	req := c.R(ctx).SetResult(&MySQLDatabase{}).SetBody(string(body))
	r, err := coupleAPIErrors(req.Put(e))
	if err != nil {
		return nil, err
	}

	return r.Result().(*MySQLDatabase), nil
}

// GetMySQLDatabaseSSL returns the SSL Certificate for the given MySQL Database
func (c *Client) GetMySQLDatabaseSSL(ctx context.Context, databaseID int) (*MySQLDatabaseSSL, error) {
	e := fmt.Sprintf("databases/mysql/instances/%d/ssl", databaseID)
	req := c.R(ctx).SetResult(&MySQLDatabaseSSL{})
	r, err := coupleAPIErrors(req.Get(e))
	if err != nil {
		return nil, err
	}

	return r.Result().(*MySQLDatabaseSSL), nil
}

// GetMySQLDatabaseCredentials returns the Root Credentials for the given MySQL Database
func (c *Client) GetMySQLDatabaseCredentials(ctx context.Context, databaseID int) (*MySQLDatabaseCredential, error) {
	e := fmt.Sprintf("databases/mysql/instances/%d/credentials", databaseID)
	req := c.R(ctx).SetResult(&MySQLDatabaseCredential{})
	r, err := coupleAPIErrors(req.Get(e))
	if err != nil {
		return nil, err
	}

	return r.Result().(*MySQLDatabaseCredential), nil
}

// ResetMySQLDatabaseCredentials returns the Root Credentials for the given MySQL Database (may take a few seconds to work)
func (c *Client) ResetMySQLDatabaseCredentials(ctx context.Context, databaseID int) error {
	e := fmt.Sprintf("databases/mysql/instances/%d/credentials/reset", databaseID)
	_, err := coupleAPIErrors(c.R(ctx).Post(e))
	return err
}

// GetMySQLDatabaseBackup returns a specific MySQL Database Backup with the given ids
func (c *Client) GetMySQLDatabaseBackup(ctx context.Context, databaseID int, backupID int) (*MySQLDatabaseBackup, error) {
	e := fmt.Sprintf("databases/mysql/instances/%d/backups/%d", databaseID, backupID)
	req := c.R(ctx).SetResult(&MySQLDatabaseBackup{})
	r, err := coupleAPIErrors(req.Get(e))
	if err != nil {
		return nil, err
	}

	return r.Result().(*MySQLDatabaseBackup), nil
}

// RestoreMySQLDatabaseBackup returns the given MySQL Database with the given Backup
func (c *Client) RestoreMySQLDatabaseBackup(ctx context.Context, databaseID int, backupID int) error {
	e := fmt.Sprintf("databases/mysql/instances/%d/backups/%d/restore", databaseID, backupID)
	_, err := coupleAPIErrors(c.R(ctx).Post(e))
	return err
}

// CreateMySQLDatabaseBackup creates a snapshot for the given MySQL database
func (c *Client) CreateMySQLDatabaseBackup(ctx context.Context, databaseID int, opts MySQLBackupCreateOptions) error {
	body, err := json.Marshal(opts)
	if err != nil {
		return err
	}

	e := fmt.Sprintf("databases/mysql/instances/%d/backups", databaseID)
	_, err = coupleAPIErrors(c.R(ctx).SetBody(string(body)).Post(e))
	return err
}

// PatchMySQLDatabase applies security patches and updates to the underlying operating system of the Managed MySQL Database
func (c *Client) PatchMySQLDatabase(ctx context.Context, databaseID int) error {
	e := fmt.Sprintf("databases/mysql/instances/%d/patch", databaseID)
	_, err := coupleAPIErrors(c.R(ctx).Post(e))
	return err
}
