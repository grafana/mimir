package linodego

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/linode/linodego/internal/parseabletime"
)

type PostgresDatabaseTarget string

const (
	PostgresDatabaseTargetPrimary   PostgresDatabaseTarget = "primary"
	PostgresDatabaseTargetSecondary PostgresDatabaseTarget = "secondary"
)

type PostgresCommitType string

const (
	PostgresCommitTrue        PostgresCommitType = "true"
	PostgresCommitFalse       PostgresCommitType = "false"
	PostgresCommitLocal       PostgresCommitType = "local"
	PostgresCommitRemoteWrite PostgresCommitType = "remote_write"
	PostgresCommitRemoteApply PostgresCommitType = "remote_apply"
)

type PostgresReplicationType string

const (
	PostgresReplicationNone      PostgresReplicationType = "none"
	PostgresReplicationAsynch    PostgresReplicationType = "asynch"
	PostgresReplicationSemiSynch PostgresReplicationType = "semi_synch"
)

// A PostgresDatabase is a instance of Linode Postgres Managed Databases
type PostgresDatabase struct {
	ID                    int                       `json:"id"`
	Status                DatabaseStatus            `json:"status"`
	Label                 string                    `json:"label"`
	Region                string                    `json:"region"`
	Type                  string                    `json:"type"`
	Engine                string                    `json:"engine"`
	Version               string                    `json:"version"`
	Encrypted             bool                      `json:"encrypted"`
	AllowList             []string                  `json:"allow_list"`
	Port                  int                       `json:"port"`
	SSLConnection         bool                      `json:"ssl_connection"`
	ClusterSize           int                       `json:"cluster_size"`
	ReplicationCommitType PostgresCommitType        `json:"replication_commit_type"`
	ReplicationType       PostgresReplicationType   `json:"replication_type"`
	Hosts                 DatabaseHost              `json:"hosts"`
	Updates               DatabaseMaintenanceWindow `json:"updates"`
	Created               *time.Time                `json:"-"`
	Updated               *time.Time                `json:"-"`
}

func (d *PostgresDatabase) UnmarshalJSON(b []byte) error {
	type Mask PostgresDatabase

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

// PostgresCreateOptions fields are used when creating a new Postgres Database
type PostgresCreateOptions struct {
	Label                 string                  `json:"label"`
	Region                string                  `json:"region"`
	Type                  string                  `json:"type"`
	Engine                string                  `json:"engine"`
	AllowList             []string                `json:"allow_list,omitempty"`
	ClusterSize           int                     `json:"cluster_size,omitempty"`
	Encrypted             bool                    `json:"encrypted,omitempty"`
	SSLConnection         bool                    `json:"ssl_connection,omitempty"`
	ReplicationType       PostgresReplicationType `json:"replication_type,omitempty"`
	ReplicationCommitType PostgresCommitType      `json:"replication_commit_type,omitempty"`
}

// PostgresUpdateOptions fields are used when altering the existing Postgres Database
type PostgresUpdateOptions struct {
	Label     string                     `json:"label,omitempty"`
	AllowList *[]string                  `json:"allow_list,omitempty"`
	Updates   *DatabaseMaintenanceWindow `json:"updates,omitempty"`
}

// PostgresDatabaseSSL is the SSL Certificate to access the Linode Managed Postgres Database
type PostgresDatabaseSSL struct {
	CACertificate []byte `json:"ca_certificate"`
}

// PostgresDatabaseCredential is the Root Credentials to access the Linode Managed Database
type PostgresDatabaseCredential struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type PostgresDatabasesPagedResponse struct {
	*PageOptions
	Data []PostgresDatabase `json:"data"`
}

func (PostgresDatabasesPagedResponse) endpoint(_ ...any) string {
	return "databases/postgresql/instances"
}

func (resp *PostgresDatabasesPagedResponse) castResult(r *resty.Request, e string) (int, int, error) {
	res, err := coupleAPIErrors(r.SetResult(PostgresDatabasesPagedResponse{}).Get(e))
	if err != nil {
		return 0, 0, err
	}
	castedRes := res.Result().(*PostgresDatabasesPagedResponse)
	resp.Data = append(resp.Data, castedRes.Data...)
	return castedRes.Pages, castedRes.Results, nil
}

// ListPostgresDatabases lists all Postgres Databases associated with the account
func (c *Client) ListPostgresDatabases(ctx context.Context, opts *ListOptions) ([]PostgresDatabase, error) {
	response := PostgresDatabasesPagedResponse{}

	err := c.listHelper(ctx, &response, opts)
	if err != nil {
		return nil, err
	}

	return response.Data, nil
}

// PostgresDatabaseBackup is information for interacting with a backup for the existing Postgres Database
type PostgresDatabaseBackup struct {
	ID      int        `json:"id"`
	Label   string     `json:"label"`
	Type    string     `json:"type"`
	Created *time.Time `json:"-"`
}

func (d *PostgresDatabaseBackup) UnmarshalJSON(b []byte) error {
	type Mask PostgresDatabaseBackup

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

// PostgresBackupCreateOptions are options used for CreatePostgresDatabaseBackup(...)
type PostgresBackupCreateOptions struct {
	Label  string                 `json:"label"`
	Target PostgresDatabaseTarget `json:"target"`
}

type PostgresDatabaseBackupsPagedResponse struct {
	*PageOptions
	Data []PostgresDatabaseBackup `json:"data"`
}

func (PostgresDatabaseBackupsPagedResponse) endpoint(ids ...any) string {
	id := ids[0].(int)
	return fmt.Sprintf("databases/postgresql/instances/%d/backups", id)
}

func (resp *PostgresDatabaseBackupsPagedResponse) castResult(r *resty.Request, e string) (int, int, error) {
	res, err := coupleAPIErrors(r.SetResult(PostgresDatabaseBackupsPagedResponse{}).Get(e))
	if err != nil {
		return 0, 0, err
	}
	castedRes := res.Result().(*PostgresDatabaseBackupsPagedResponse)
	resp.Data = append(resp.Data, castedRes.Data...)
	return castedRes.Pages, castedRes.Results, nil
}

// ListPostgresDatabaseBackups lists all Postgres Database Backups associated with the given Postgres Database
func (c *Client) ListPostgresDatabaseBackups(ctx context.Context, databaseID int, opts *ListOptions) ([]PostgresDatabaseBackup, error) {
	response := PostgresDatabaseBackupsPagedResponse{}

	err := c.listHelper(ctx, &response, opts, databaseID)
	if err != nil {
		return nil, err
	}

	return response.Data, nil
}

// GetPostgresDatabase returns a single Postgres Database matching the id
func (c *Client) GetPostgresDatabase(ctx context.Context, databaseID int) (*PostgresDatabase, error) {
	e := fmt.Sprintf("databases/postgresql/instances/%d", databaseID)
	req := c.R(ctx).SetResult(&PostgresDatabase{})
	r, err := coupleAPIErrors(req.Get(e))
	if err != nil {
		return nil, err
	}

	return r.Result().(*PostgresDatabase), nil
}

// CreatePostgresDatabase creates a new Postgres Database using the createOpts as configuration, returns the new Postgres Database
func (c *Client) CreatePostgresDatabase(ctx context.Context, opts PostgresCreateOptions) (*PostgresDatabase, error) {
	body, err := json.Marshal(opts)
	if err != nil {
		return nil, err
	}

	e := "databases/postgresql/instances"
	req := c.R(ctx).SetResult(&PostgresDatabase{}).SetBody(string(body))
	r, err := coupleAPIErrors(req.Post(e))
	if err != nil {
		return nil, err
	}
	return r.Result().(*PostgresDatabase), nil
}

// DeletePostgresDatabase deletes an existing Postgres Database with the given id
func (c *Client) DeletePostgresDatabase(ctx context.Context, databaseID int) error {
	e := fmt.Sprintf("databases/postgresql/instances/%d", databaseID)
	_, err := coupleAPIErrors(c.R(ctx).Delete(e))
	return err
}

// UpdatePostgresDatabase updates the given Postgres Database with the provided opts, returns the PostgresDatabase with the new settings
func (c *Client) UpdatePostgresDatabase(ctx context.Context, databaseID int, opts PostgresUpdateOptions) (*PostgresDatabase, error) {
	body, err := json.Marshal(opts)
	if err != nil {
		return nil, NewError(err)
	}

	e := fmt.Sprintf("databases/postgresql/instances/%d", databaseID)
	req := c.R(ctx).SetResult(&PostgresDatabase{}).SetBody(string(body))
	r, err := coupleAPIErrors(req.Put(e))
	if err != nil {
		return nil, err
	}

	return r.Result().(*PostgresDatabase), nil
}

// PatchPostgresDatabase applies security patches and updates to the underlying operating system of the Managed Postgres Database
func (c *Client) PatchPostgresDatabase(ctx context.Context, databaseID int) error {
	e := fmt.Sprintf("databases/postgresql/instances/%d/patch", databaseID)
	_, err := coupleAPIErrors(c.R(ctx).Post(e))
	return err
}

// GetPostgresDatabaseCredentials returns the Root Credentials for the given Postgres Database
func (c *Client) GetPostgresDatabaseCredentials(ctx context.Context, databaseID int) (*PostgresDatabaseCredential, error) {
	e := fmt.Sprintf("databases/postgresql/instances/%d/credentials", databaseID)
	req := c.R(ctx).SetResult(&PostgresDatabaseCredential{})
	r, err := coupleAPIErrors(req.Get(e))
	if err != nil {
		return nil, err
	}

	return r.Result().(*PostgresDatabaseCredential), nil
}

// ResetPostgresDatabaseCredentials returns the Root Credentials for the given Postgres Database (may take a few seconds to work)
func (c *Client) ResetPostgresDatabaseCredentials(ctx context.Context, databaseID int) error {
	e := fmt.Sprintf("databases/postgresql/instances/%d/credentials/reset", databaseID)
	_, err := coupleAPIErrors(c.R(ctx).Post(e))
	return err
}

// GetPostgresDatabaseSSL returns the SSL Certificate for the given Postgres Database
func (c *Client) GetPostgresDatabaseSSL(ctx context.Context, databaseID int) (*PostgresDatabaseSSL, error) {
	e := fmt.Sprintf("databases/postgresql/instances/%d/ssl", databaseID)
	req := c.R(ctx).SetResult(&PostgresDatabaseSSL{})
	r, err := coupleAPIErrors(req.Get(e))
	if err != nil {
		return nil, err
	}

	return r.Result().(*PostgresDatabaseSSL), nil
}

// GetPostgresDatabaseBackup returns a specific Postgres Database Backup with the given ids
func (c *Client) GetPostgresDatabaseBackup(ctx context.Context, databaseID int, backupID int) (*PostgresDatabaseBackup, error) {
	e := fmt.Sprintf("databases/postgresql/instances/%d/backups/%d", databaseID, backupID)
	req := c.R(ctx).SetResult(&PostgresDatabaseBackup{})
	r, err := coupleAPIErrors(req.Get(e))
	if err != nil {
		return nil, err
	}

	return r.Result().(*PostgresDatabaseBackup), nil
}

// RestorePostgresDatabaseBackup returns the given Postgres Database with the given Backup
func (c *Client) RestorePostgresDatabaseBackup(ctx context.Context, databaseID int, backupID int) error {
	e := fmt.Sprintf("databases/postgresql/instances/%d/backups/%d/restore", databaseID, backupID)
	_, err := coupleAPIErrors(c.R(ctx).Post(e))
	return err
}

// CreatePostgresDatabaseBackup creates a snapshot for the given Postgres database
func (c *Client) CreatePostgresDatabaseBackup(ctx context.Context, databaseID int, opts PostgresBackupCreateOptions) error {
	body, err := json.Marshal(opts)
	if err != nil {
		return err
	}

	e := fmt.Sprintf("databases/postgresql/instances/%d/backups", databaseID)
	_, err = coupleAPIErrors(c.R(ctx).SetBody(string(body)).Post(e))
	return err
}
