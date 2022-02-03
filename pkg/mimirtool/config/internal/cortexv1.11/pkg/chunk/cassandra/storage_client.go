// SPDX-License-Identifier: AGPL-3.0-only

package cassandra

import (
	"flag"
	"time"

	"github.com/grafana/dskit/flagext"
)

type Config struct {
	Addresses                string              `yaml:"addresses"`
	Port                     int                 `yaml:"port"`
	Keyspace                 string              `yaml:"keyspace"`
	Consistency              string              `yaml:"consistency"`
	ReplicationFactor        int                 `yaml:"replication_factor"`
	DisableInitialHostLookup bool                `yaml:"disable_initial_host_lookup"`
	SSL                      bool                `yaml:"SSL"`
	HostVerification         bool                `yaml:"host_verification"`
	HostSelectionPolicy      string              `yaml:"host_selection_policy"`
	CAPath                   string              `yaml:"CA_path"`
	CertPath                 string              `yaml:"tls_cert_path"`
	KeyPath                  string              `yaml:"tls_key_path"`
	Auth                     bool                `yaml:"auth"`
	Username                 string              `yaml:"username"`
	Password                 flagext.Secret      `yaml:"password"`
	PasswordFile             string              `yaml:"password_file"`
	CustomAuthenticators     flagext.StringSlice `yaml:"custom_authenticators"`
	Timeout                  time.Duration       `yaml:"timeout"`
	ConnectTimeout           time.Duration       `yaml:"connect_timeout"`
	ReconnectInterval        time.Duration       `yaml:"reconnect_interval"`
	Retries                  int                 `yaml:"max_retries"`
	MaxBackoff               time.Duration       `yaml:"retry_max_backoff"`
	MinBackoff               time.Duration       `yaml:"retry_min_backoff"`
	QueryConcurrency         int                 `yaml:"query_concurrency"`
	NumConnections           int                 `yaml:"num_connections"`
	ConvictHosts             bool                `yaml:"convict_hosts_on_failure"`
	TableOptions             string              `yaml:"table_options"`
}

const (
	HostPolicyRoundRobin = "round-robin"
	HostPolicyTokenAware = "token-aware"
)

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&cfg.Addresses, "cassandra.addresses", "", "Comma-separated hostnames or IPs of Cassandra instances.")
	f.IntVar(&cfg.Port, "cassandra.port", 9042, "Port that Cassandra is running on")
	f.StringVar(&cfg.Keyspace, "cassandra.keyspace", "", "Keyspace to use in Cassandra.")
	f.StringVar(&cfg.Consistency, "cassandra.consistency", "QUORUM", "Consistency level for Cassandra.")
	f.IntVar(&cfg.ReplicationFactor, "cassandra.replication-factor", 3, "Replication factor to use in Cassandra.")
	f.BoolVar(&cfg.DisableInitialHostLookup, "cassandra.disable-initial-host-lookup", false, "Instruct the cassandra driver to not attempt to get host info from the system.peers table.")
	f.BoolVar(&cfg.SSL, "cassandra.ssl", false, "Use SSL when connecting to cassandra instances.")
	f.BoolVar(&cfg.HostVerification, "cassandra.host-verification", true, "Require SSL certificate validation.")
	f.StringVar(&cfg.HostSelectionPolicy, "cassandra.host-selection-policy", HostPolicyRoundRobin, "Policy for selecting Cassandra host. Supported values are: round-robin, token-aware.")
	f.StringVar(&cfg.CAPath, "cassandra.ca-path", "", "Path to certificate file to verify the peer.")
	f.StringVar(&cfg.CertPath, "cassandra.tls-cert-path", "", "Path to certificate file used by TLS.")
	f.StringVar(&cfg.KeyPath, "cassandra.tls-key-path", "", "Path to private key file used by TLS.")
	f.BoolVar(&cfg.Auth, "cassandra.auth", false, "Enable password authentication when connecting to cassandra.")
	f.StringVar(&cfg.Username, "cassandra.username", "", "Username to use when connecting to cassandra.")
	f.Var(&cfg.Password, "cassandra.password", "Password to use when connecting to cassandra.")
	f.StringVar(&cfg.PasswordFile, "cassandra.password-file", "", "File containing password to use when connecting to cassandra.")
	f.Var(&cfg.CustomAuthenticators, "cassandra.custom-authenticator", "If set, when authenticating with cassandra a custom authenticator will be expected during the handshake. This flag can be set multiple times.")
	f.DurationVar(&cfg.Timeout, "cassandra.timeout", 2*time.Second, "Timeout when connecting to cassandra.")
	f.DurationVar(&cfg.ConnectTimeout, "cassandra.connect-timeout", 5*time.Second, "Initial connection timeout, used during initial dial to server.")
	f.DurationVar(&cfg.ReconnectInterval, "cassandra.reconnent-interval", 1*time.Second, "Interval to retry connecting to cassandra nodes marked as DOWN.")
	f.IntVar(&cfg.Retries, "cassandra.max-retries", 0, "Number of retries to perform on a request. Set to 0 to disable retries.")
	f.DurationVar(&cfg.MinBackoff, "cassandra.retry-min-backoff", 100*time.Millisecond, "Minimum time to wait before retrying a failed request.")
	f.DurationVar(&cfg.MaxBackoff, "cassandra.retry-max-backoff", 10*time.Second, "Maximum time to wait before retrying a failed request.")
	f.IntVar(&cfg.QueryConcurrency, "cassandra.query-concurrency", 0, "Limit number of concurrent queries to Cassandra. Set to 0 to disable the limit.")
	f.IntVar(&cfg.NumConnections, "cassandra.num-connections", 2, "Number of TCP connections per host.")
	f.BoolVar(&cfg.ConvictHosts, "cassandra.convict-hosts-on-failure", true, "Convict hosts of being down on failure.")
	f.StringVar(&cfg.TableOptions, "cassandra.table-options", "", "Table options used to create index or chunk tables. This value is used as plain text in the table `WITH` like this, \"CREATE TABLE <generated_by_cortex> (...) WITH <cassandra.table-options>\". For details, see https://cortexmetrics.io/docs/production/cassandra. By default it will use the default table options of your Cassandra cluster.")
}
