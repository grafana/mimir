package cassandra

import (
	"bytes"
	"crypto/tls"
	"io/ioutil"
	"strings"

	"github.com/grafana/mimir/pkg/chunk"
	"github.com/grafana/mimir/pkg/chunk/cassandra"
	util_log "github.com/grafana/mimir/pkg/util/log"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/semaphore"
)

func session(cfg cassandra.Config, name string, reg prometheus.Registerer) (*gocql.Session, error) {
	cluster := gocql.NewCluster(strings.Split(cfg.Addresses, ",")...)
	cluster.Port = cfg.Port
	cluster.Keyspace = cfg.Keyspace
	cluster.Timeout = cfg.Timeout
	cluster.ConnectTimeout = cfg.ConnectTimeout
	cluster.ReconnectInterval = cfg.ReconnectInterval
	cluster.NumConns = cfg.NumConnections
	cluster.Logger = log.With(util_log.Logger, "module", "gocql", "client", name)
	cluster.Registerer = prometheus.WrapRegistererWith(
		prometheus.Labels{"client": name}, reg)
	if cfg.Retries > 0 {
		cluster.RetryPolicy = &gocql.ExponentialBackoffRetryPolicy{
			NumRetries: cfg.Retries,
			Min:        cfg.MinBackoff,
			Max:        cfg.MaxBackoff,
		}
	}
	if !cfg.ConvictHosts {
		cluster.ConvictionPolicy = noopConvictionPolicy{}
	}
	if err := setClusterConfig(cfg, cluster); err != nil {
		return nil, errors.WithStack(err)
	}

	session, err := cluster.CreateSession()
	if err == nil {
		return session, nil
	}
	// ErrNoConnectionsStarted will be returned if keyspace don't exist or is invalid.
	// ref. https://github.com/gocql/gocql/blob/07ace3bab0f84bb88477bab5d79ba1f7e1da0169/cassandra_test.go#L85-L97
	if err != gocql.ErrNoConnectionsStarted {
		return nil, errors.WithStack(err)
	}

	session, err = cluster.CreateSession()
	return session, errors.WithStack(err)
}

// apply config settings to a cassandra ClusterConfig
func setClusterConfig(cfg cassandra.Config, cluster *gocql.ClusterConfig) error {
	consistency, err := gocql.ParseConsistencyWrapper(cfg.Consistency)
	if err != nil {
		return errors.Wrap(err, "unable to parse the configured consistency")
	}

	cluster.Consistency = consistency
	cluster.DisableInitialHostLookup = cfg.DisableInitialHostLookup

	if cfg.SSL {
		if cfg.HostVerification {
			cluster.SslOpts = &gocql.SslOptions{
				CaPath:                 cfg.CAPath,
				EnableHostVerification: true,
				Config: &tls.Config{
					ServerName: strings.Split(cfg.Addresses, ",")[0],
				},
			}
		} else {
			cluster.SslOpts = &gocql.SslOptions{
				EnableHostVerification: false,
			}
		}
	}
	if cfg.Auth {
		password := cfg.Password.Value
		if cfg.PasswordFile != "" {
			passwordBytes, err := ioutil.ReadFile(cfg.PasswordFile)
			if err != nil {
				return errors.Errorf("Could not read Cassandra password file: %v", err)
			}
			passwordBytes = bytes.TrimRight(passwordBytes, "\n")
			password = string(passwordBytes)
		}
		if len(cfg.CustomAuthenticators) != 0 {
			cluster.Authenticator = cassandra.CustomPasswordAuthenticator{
				ApprovedAuthenticators: cfg.CustomAuthenticators,
				Username:               cfg.Username,
				Password:               password,
			}
			return nil
		}
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: cfg.Username,
			Password: password,
		}
	}
	return nil
}

// StorageClient implements chunk.IndexClient and chunk.ObjectClient for Cassandra.
type StorageClient struct {
	cfg            cassandra.Config
	schemaCfg      chunk.SchemaConfig
	readSession    *gocql.Session
	writeSession   *gocql.Session
	querySemaphore *semaphore.Weighted
}

// NewStorageClient returns a new StorageClient.
func NewStorageClient(cfg cassandra.Config, schemaCfg chunk.SchemaConfig, registerer prometheus.Registerer) (*StorageClient, error) {
	readSession, err := session(cfg, "index-read", registerer)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	writeSession, err := session(cfg, "index-write", registerer)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	var querySemaphore *semaphore.Weighted
	if cfg.QueryConcurrency > 0 {
		querySemaphore = semaphore.NewWeighted(int64(cfg.QueryConcurrency))
	}

	client := &StorageClient{
		cfg:            cfg,
		schemaCfg:      schemaCfg,
		readSession:    readSession,
		writeSession:   writeSession,
		querySemaphore: querySemaphore,
	}
	return client, nil
}

// Stop implement chunk.IndexClient.
func (s *StorageClient) Stop() {
	s.readSession.Close()
	s.writeSession.Close()
}

// ObjectClient implements chunk.ObjectClient for Cassandra.
type ObjectClient struct {
	cfg            cassandra.Config
	schemaCfg      chunk.SchemaConfig
	readSession    *gocql.Session
	writeSession   *gocql.Session
	querySemaphore *semaphore.Weighted
}

// NewObjectClient returns a new ObjectClient.
func NewObjectClient(cfg cassandra.Config, schemaCfg chunk.SchemaConfig, registerer prometheus.Registerer) (*ObjectClient, error) {
	readSession, err := session(cfg, "chunks-read", registerer)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	writeSession, err := session(cfg, "chunks-write", registerer)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	var querySemaphore *semaphore.Weighted
	if cfg.QueryConcurrency > 0 {
		querySemaphore = semaphore.NewWeighted(int64(cfg.QueryConcurrency))
	}

	client := &ObjectClient{
		cfg:            cfg,
		schemaCfg:      schemaCfg,
		readSession:    readSession,
		writeSession:   writeSession,
		querySemaphore: querySemaphore,
	}
	return client, nil
}

type noopConvictionPolicy struct{}

// AddFailure should return `true` if the host should be convicted, `false` otherwise.
// Convicted means connections are removed - we don't want that.
// Implementats gocql.ConvictionPolicy.
func (noopConvictionPolicy) AddFailure(err error, host *gocql.HostInfo) bool {
	level.Error(util_log.Logger).Log("msg", "Cassandra host failure", "err", err, "host", host.String())
	return false
}

// Implementats gocql.ConvictionPolicy.
func (noopConvictionPolicy) Reset(host *gocql.HostInfo) {}
