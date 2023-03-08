package linodego

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/linode/linodego/internal/parseabletime"
)

type MongoDatabaseTarget string

const (
	MongoDatabaseTargetPrimary   MongoDatabaseTarget = "primary"
	MongoDatabaseTargetSecondary MongoDatabaseTarget = "secondary"
)

type MongoCompressionType string

const (
	MongoCompressionNone   MongoCompressionType = "none"
	MongoCompressionSnappy MongoCompressionType = "snappy"
	MongoCompressionZlib   MongoCompressionType = "zlib"
)

type MongoStorageEngine string

const (
	MongoStorageWiredTiger MongoStorageEngine = "wiredtiger"
	MongoStorageMmapv1     MongoStorageEngine = "mmapv1"
)

// A MongoDatabase is a instance of Linode Mongo Managed Databases
type MongoDatabase struct {
	ID              int                       `json:"id"`
	Status          DatabaseStatus            `json:"status"`
	Label           string                    `json:"label"`
	Region          string                    `json:"region"`
	Type            string                    `json:"type"`
	Engine          string                    `json:"engine"`
	Version         string                    `json:"version"`
	Encrypted       bool                      `json:"encrypted"`
	AllowList       []string                  `json:"allow_list"`
	Peers           []string                  `json:"peers"`
	Port            int                       `json:"port"`
	ReplicaSet      string                    `json:"replica_set"`
	SSLConnection   bool                      `json:"ssl_connection"`
	ClusterSize     int                       `json:"cluster_size"`
	Hosts           DatabaseHost              `json:"hosts"`
	CompressionType MongoCompressionType      `json:"compression_type"`
	StorageEngine   MongoStorageEngine        `json:"storage_engine"`
	Updates         DatabaseMaintenanceWindow `json:"updates"`
	Created         *time.Time                `json:"-"`
	Updated         *time.Time                `json:"-"`
}

func (d *MongoDatabase) UnmarshalJSON(b []byte) error {
	type Mask MongoDatabase

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

// MongoCreateOptions fields are used when creating a new Mongo Database
type MongoCreateOptions struct {
	Label           string               `json:"label"`
	Region          string               `json:"region"`
	Type            string               `json:"type"`
	Engine          string               `json:"engine"`
	AllowList       []string             `json:"allow_list,omitempty"`
	ClusterSize     int                  `json:"cluster_size,omitempty"`
	Encrypted       bool                 `json:"encrypted,omitempty"`
	SSLConnection   bool                 `json:"ssl_connection,omitempty"`
	CompressionType MongoCompressionType `json:"compression_type,omitempty"`
	StorageEngine   MongoStorageEngine   `json:"storage_engine,omitempty"`
}

// MongoUpdateOptions fields are used when altering the existing Mongo Database
type MongoUpdateOptions struct {
	Label     string                     `json:"label,omitempty"`
	AllowList *[]string                  `json:"allow_list,omitempty"`
	Updates   *DatabaseMaintenanceWindow `json:"updates,omitempty"`
}

// MongoDatabaseSSL is the SSL Certificate to access the Linode Managed Mongo Database
type MongoDatabaseSSL struct {
	CACertificate []byte `json:"ca_certificate"`
}

// MongoDatabaseCredential is the Root Credentials to access the Linode Managed Database
type MongoDatabaseCredential struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type MongoDatabasesPagedResponse struct {
	*PageOptions
	Data []MongoDatabase `json:"data"`
}

func (MongoDatabasesPagedResponse) endpoint(_ ...any) string {
	return "databases/mongodb/instances"
}

func (resp *MongoDatabasesPagedResponse) castResult(r *resty.Request, e string) (int, int, error) {
	res, err := coupleAPIErrors(r.SetResult(MongoDatabasesPagedResponse{}).Get(e))
	if err != nil {
		return 0, 0, err
	}
	castedRes := res.Result().(*MongoDatabasesPagedResponse)
	resp.Data = append(resp.Data, castedRes.Data...)
	return castedRes.Pages, castedRes.Results, nil
}

// ListMongoDatabases lists all Mongo Databases associated with the account
func (c *Client) ListMongoDatabases(ctx context.Context, opts *ListOptions) ([]MongoDatabase, error) {
	response := MongoDatabasesPagedResponse{}

	err := c.listHelper(ctx, &response, opts)
	if err != nil {
		return nil, err
	}

	return response.Data, nil
}

// MongoDatabaseBackup is information for interacting with a backup for the existing Mongo Database
type MongoDatabaseBackup struct {
	ID      int        `json:"id"`
	Label   string     `json:"label"`
	Type    string     `json:"type"`
	Created *time.Time `json:"-"`
}

func (d *MongoDatabaseBackup) UnmarshalJSON(b []byte) error {
	type Mask MongoDatabaseBackup

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

// MongoBackupCreateOptions are options used for CreateMongoDatabaseBackup(...)
type MongoBackupCreateOptions struct {
	Label  string              `json:"label"`
	Target MongoDatabaseTarget `json:"target"`
}

type MongoDatabaseBackupsPagedResponse struct {
	*PageOptions
	Data []MongoDatabaseBackup `json:"data"`
}

func (MongoDatabaseBackupsPagedResponse) endpoint(ids ...any) string {
	id := ids[0].(int)
	return fmt.Sprintf("databases/mongodb/instances/%d/backups", id)
}

func (resp *MongoDatabaseBackupsPagedResponse) castResult(r *resty.Request, e string) (int, int, error) {
	res, err := coupleAPIErrors(r.SetResult(MongoDatabaseBackupsPagedResponse{}).Get(e))
	if err != nil {
		return 0, 0, err
	}
	castedRes := res.Result().(*MongoDatabaseBackupsPagedResponse)
	resp.Data = append(resp.Data, castedRes.Data...)
	return castedRes.Pages, castedRes.Results, nil
}

// ListMongoDatabaseBackups lists all Mongo Database Backups associated with the given Mongo Database
func (c *Client) ListMongoDatabaseBackups(ctx context.Context, databaseID int, opts *ListOptions) ([]MongoDatabaseBackup, error) {
	response := MongoDatabaseBackupsPagedResponse{}

	err := c.listHelper(ctx, &response, opts, databaseID)
	if err != nil {
		return nil, err
	}

	return response.Data, nil
}

// GetMongoDatabase returns a single Mongo Database matching the id
func (c *Client) GetMongoDatabase(ctx context.Context, databaseID int) (*MongoDatabase, error) {
	e := fmt.Sprintf("databases/mongodb/instances/%d", databaseID)
	req := c.R(ctx).SetResult(&MongoDatabase{})
	r, err := coupleAPIErrors(req.Get(e))
	if err != nil {
		return nil, err
	}

	return r.Result().(*MongoDatabase), nil
}

// CreateMongoDatabase creates a new Mongo Database using the createOpts as configuration, returns the new Mongo Database
func (c *Client) CreateMongoDatabase(ctx context.Context, opts MongoCreateOptions) (*MongoDatabase, error) {
	body, err := json.Marshal(opts)
	if err != nil {
		return nil, err
	}

	e := "databases/mongodb/instances"
	req := c.R(ctx).SetResult(&MongoDatabase{}).SetBody(string(body))
	r, err := coupleAPIErrors(req.Post(e))
	if err != nil {
		return nil, err
	}
	return r.Result().(*MongoDatabase), nil
}

// DeleteMongoDatabase deletes an existing Mongo Database with the given id
func (c *Client) DeleteMongoDatabase(ctx context.Context, databaseID int) error {
	e := fmt.Sprintf("databases/mongodb/instances/%d", databaseID)
	_, err := coupleAPIErrors(c.R(ctx).Delete(e))
	return err
}

// UpdateMongoDatabase updates the given Mongo Database with the provided opts, returns the MongoDatabase with the new settings
func (c *Client) UpdateMongoDatabase(ctx context.Context, databaseID int, opts MongoUpdateOptions) (*MongoDatabase, error) {
	body, err := json.Marshal(opts)
	if err != nil {
		return nil, err
	}

	e := fmt.Sprintf("databases/mongodb/instances/%d", databaseID)
	req := c.R(ctx).SetResult(&MongoDatabase{}).SetBody(string(body))
	r, err := coupleAPIErrors(req.Put(e))
	if err != nil {
		return nil, err
	}

	return r.Result().(*MongoDatabase), nil
}

// PatchMongoDatabase applies security patches and updates to the underlying operating system of the Managed Mongo Database
func (c *Client) PatchMongoDatabase(ctx context.Context, databaseID int) error {
	e := fmt.Sprintf("databases/mongodb/instances/%d/patch", databaseID)
	_, err := coupleAPIErrors(c.R(ctx).Post(e))
	return err
}

// GetMongoDatabaseCredentials returns the Root Credentials for the given Mongo Database
func (c *Client) GetMongoDatabaseCredentials(ctx context.Context, databaseID int) (*MongoDatabaseCredential, error) {
	e := fmt.Sprintf("databases/mongodb/instances/%d/credentials", databaseID)
	req := c.R(ctx).SetResult(&MongoDatabaseCredential{})
	r, err := coupleAPIErrors(req.Get(e))
	if err != nil {
		return nil, err
	}

	return r.Result().(*MongoDatabaseCredential), nil
}

// ResetMongoDatabaseCredentials returns the Root Credentials for the given Mongo Database (may take a few seconds to work)
func (c *Client) ResetMongoDatabaseCredentials(ctx context.Context, databaseID int) error {
	e := fmt.Sprintf("databases/mongodb/instances/%d/credentials/reset", databaseID)
	_, err := coupleAPIErrors(c.R(ctx).Post(e))
	return err
}

// GetMongoDatabaseSSL returns the SSL Certificate for the given Mongo Database
func (c *Client) GetMongoDatabaseSSL(ctx context.Context, databaseID int) (*MongoDatabaseSSL, error) {
	e := fmt.Sprintf("databases/mongodb/instances/%d/ssl", databaseID)
	req := c.R(ctx).SetResult(&MongoDatabaseSSL{})
	r, err := coupleAPIErrors(req.Get(e))
	if err != nil {
		return nil, err
	}

	return r.Result().(*MongoDatabaseSSL), nil
}

// GetMongoDatabaseBackup returns a specific Mongo Database Backup with the given ids
func (c *Client) GetMongoDatabaseBackup(ctx context.Context, databaseID int, backupID int) (*MongoDatabaseBackup, error) {
	e := fmt.Sprintf("databases/mongodb/instances/%d/backups/%d", databaseID, backupID)
	req := c.R(ctx).SetResult(&MongoDatabaseBackup{})
	r, err := coupleAPIErrors(req.Get(e))
	if err != nil {
		return nil, err
	}

	return r.Result().(*MongoDatabaseBackup), nil
}

// RestoreMongoDatabaseBackup returns the given Mongo Database with the given Backup
func (c *Client) RestoreMongoDatabaseBackup(ctx context.Context, databaseID int, backupID int) error {
	e := fmt.Sprintf("databases/mongodb/instances/%d/backups/%d/restore", databaseID, backupID)
	_, err := coupleAPIErrors(c.R(ctx).Post(e))
	return err
}

// CreateMongoDatabaseBackup creates a snapshot for the given Mongo database
func (c *Client) CreateMongoDatabaseBackup(ctx context.Context, databaseID int, opts MongoBackupCreateOptions) error {
	body, err := json.Marshal(opts)
	if err != nil {
		return err
	}
	e := fmt.Sprintf("databases/mongodb/instances/%d/backups", databaseID)
	_, err = coupleAPIErrors(c.R(ctx).SetBody(string(body)).Post(e))
	return err
}
