// SPDX-License-Identifier: AGPL-3.0-only

package vault

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	hashivault "github.com/hashicorp/vault/api"
	"github.com/hashicorp/vault/api/auth/approle"
	"github.com/hashicorp/vault/api/auth/kubernetes"
	"github.com/hashicorp/vault/api/auth/userpass"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Config for the Vault used to fetch secrets
type Config struct {
	Enabled bool `yaml:"enabled" category:"experimental"`

	URL       string     `yaml:"url" category:"experimental"`
	MountPath string     `yaml:"mount_path" category:"experimental"`
	Auth      AuthConfig `yaml:"auth" category:"experimental"`

	Mock SecretsEngine `yaml:"-"`
}

func (cfg *Config) Validate() error {
	if !cfg.Enabled {
		return nil
	}

	if cfg.URL == "" {
		return errors.New("empty vault URL supplied")
	}

	if cfg.MountPath == "" {
		return errors.New("empty vault mount path supplied")
	}

	if _, err := cfg.Auth.authMethod(); err != nil {
		return fmt.Errorf("invalid vault auth supplied: %w", err)
	}

	return nil
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.BoolVar(&cfg.Enabled, "vault.enabled", false, "Enables fetching of keys and certificates from Vault")
	f.StringVar(&cfg.URL, "vault.url", "", "Location of the Vault server")
	f.StringVar(&cfg.MountPath, "vault.mount-path", "", "Location of secrets engine within Vault")
	cfg.Auth.RegisterFlagsWithPrefix(f, "vault.auth.")
}

type SecretsEngine interface {
	Get(ctx context.Context, path string) (*hashivault.KVSecret, error)
}

type Vault struct {
	kvStore SecretsEngine
	auth    AuthConfig

	client *hashivault.Client
	token  *hashivault.Secret
	logger log.Logger

	authLeaseRenewalActive       prometheus.Gauge
	authLeaseRenewalSuccessTotal prometheus.Counter
	authSuccessTotal             prometheus.Counter
}

func NewVault(cfg Config, l log.Logger, registerer prometheus.Registerer) (*Vault, error) {
	if cfg.Mock != nil {
		return &Vault{
			kvStore: cfg.Mock,
		}, nil
	}

	config := hashivault.DefaultConfig()
	config.Address = cfg.URL

	client, err := hashivault.NewClient(config)
	if err != nil {
		return nil, err
	}

	authToken, err := getAuthToken(context.Background(), &cfg.Auth, client)
	if err != nil {
		return nil, fmt.Errorf("failed to get auth token from vault: %v", err)
	}

	vault := &Vault{
		kvStore: client.KVv2(cfg.MountPath),
		auth:    cfg.Auth,
		token:   authToken,
		client:  client,
		logger:  l,
		authLeaseRenewalActive: promauto.With(registerer).NewGauge(prometheus.GaugeOpts{
			Name: "cortex_vault_token_lease_renewal_active",
			Help: "Gauge to check whether token renewal is active or not (0 active, 1 inactive).",
		}),
		authLeaseRenewalSuccessTotal: promauto.With(registerer).NewCounter(prometheus.CounterOpts{
			Name: "cortex_vault_token_lease_renewal_success_total",
			Help: "Number of successful token lease renewals. Token is renewed as it approaches the end of its ttl.",
		}),
		authSuccessTotal: promauto.With(registerer).NewCounter(prometheus.CounterOpts{
			Name: "cortex_vault_auth_success_total",
			Help: "Number of successful authentications. Authentication occurs when the token has reached its max_ttl and the lease can no longer be renewed.",
		}),
	}
	vault.authSuccessTotal.Inc()

	return vault, nil
}

func (v *Vault) ReadSecret(path string) ([]byte, error) {
	secret, err := v.kvStore.Get(context.Background(), path)

	if err != nil {
		return nil, fmt.Errorf("unable to read secret from vault: %v", err)
	}

	if secret == nil || secret.Data == nil {
		return nil, errors.New("secret data is nil")
	}

	data, ok := secret.Data["value"].(string)
	if !ok {
		return nil, fmt.Errorf("secret data type is not string, found %T value: %#v at path: %s", secret.Data["value"], secret.Data["value"], path)
	}

	return []byte(data), nil
}

func (v *Vault) manageTokenLifecycle(ctx context.Context, authTokenWatcher *hashivault.LifetimeWatcher) {
	go authTokenWatcher.Start()
	defer authTokenWatcher.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-authTokenWatcher.DoneCh():
			// Token failed to renew (e.g expired), re-auth required
			return

		case renewalInfo := <-authTokenWatcher.RenewCh():
			// Token was successfully renewed
			if renewalInfo.Secret.Auth != nil {
				level.Debug(v.logger).Log("msg", "token renewed", "leaseDuration", time.Duration(renewalInfo.Secret.Auth.LeaseDuration)*time.Second)
				v.authLeaseRenewalSuccessTotal.Inc()
			}
		}
	}
}

func (v *Vault) KeepRenewingTokenLease(ctx context.Context) error {
	v.authLeaseRenewalActive.Inc()
	defer v.authLeaseRenewalActive.Dec()

	for ctx.Err() == nil {
		authTokenWatcher, err := v.client.NewLifetimeWatcher(&hashivault.LifetimeWatcherInput{
			Secret: v.token,
		})
		if err != nil {
			return fmt.Errorf("error initializing auth token lifetime watcher: %v", err)
		}

		v.manageTokenLifecycle(ctx, authTokenWatcher)

		if ctx.Err() != nil {
			return ctx.Err()
		}

		newAuthToken, err := getAuthToken(ctx, &v.auth, v.client)
		if err != nil {
			level.Error(v.logger).Log("msg", "error during re-authentication after token expiry", "err", err)
			return err
		}

		v.authSuccessTotal.Inc()
		v.token = newAuthToken
	}

	return nil
}

type authFactoryReal struct{}

func getAuthToken(ctx context.Context, authCfg *AuthConfig, client *hashivault.Client) (*hashivault.Secret, error) {
	am, err := authCfg.authMethod()
	if err != nil {
		return nil, err
	}

	return am.authenticate(ctx, &authFactoryReal{}, client)
}

func (af *authFactoryReal) NewAppRoleAuth(roleID string, secretID *approle.SecretID, opts ...approle.LoginOption) (*approle.AppRoleAuth, error) {
	return approle.NewAppRoleAuth(
		roleID,
		secretID,
		opts...,
	)
}

func (af *authFactoryReal) NewKubernetesAuth(roleName string, opts ...kubernetes.LoginOption) (*kubernetes.KubernetesAuth, error) {
	return kubernetes.NewKubernetesAuth(
		roleName,
		opts...,
	)
}

func (af *authFactoryReal) NewUserpassAuth(username string, password string, opts ...userpass.LoginOption) (*userpass.UserpassAuth, error) {
	return userpass.NewUserpassAuth(
		username,
		&userpass.Password{FromString: password},
		opts...,
	)
}
