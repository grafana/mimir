// SPDX-License-Identifier: AGPL-3.0-only

package vault

import (
	"context"
	"errors"
	"flag"
	"fmt"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	hashivault "github.com/hashicorp/vault/api"
	"github.com/hashicorp/vault/api/auth/approle"
	"github.com/hashicorp/vault/api/auth/kubernetes"
	"github.com/hashicorp/vault/api/auth/userpass"
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
	KVStore SecretsEngine
	Auth    AuthConfig

	client *hashivault.Client
	token  *hashivault.Secret
	logger log.Logger
}

func NewVault(cfg Config, l log.Logger) (*Vault, error) {
	if cfg.Mock != nil {
		return &Vault{
			KVStore: cfg.Mock,
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
		KVStore: client.KVv2(cfg.MountPath),
		Auth:    cfg.Auth,
		token:   authToken,
		client:  client,
		logger:  l,
	}

	return vault, nil
}

func (v *Vault) ReadSecret(path string) ([]byte, error) {
	secret, err := v.KVStore.Get(context.Background(), path)

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

func (v *Vault) manageTokenLifecycle(ctx context.Context, authToken *hashivault.Secret) (bool, error) {
	authTokenWatcher, err := v.client.NewLifetimeWatcher(&hashivault.LifetimeWatcherInput{
		Secret: v.token,
	})
	if err != nil {
		return false, fmt.Errorf("error initializing auth token lifetime watcher: %v", err)
	}

	go authTokenWatcher.Start()
	defer authTokenWatcher.Stop()

	for {
		select {
		case <-ctx.Done():
			return false, nil

		case <-authTokenWatcher.DoneCh():
			// Token failed to renew (e.g expired), re-auth required
			return true, nil

		case renewalInfo := <-authTokenWatcher.RenewCh():
			// Token was successfully renewed
			if renewalInfo.Secret.Auth != nil {
				level.Debug(v.logger).Log("msg", fmt.Sprintf("token renewed, new lease: %d", renewalInfo.Secret.Auth.LeaseDuration))
			}
		}
	}
}

func (v *Vault) RenewTokenLease(ctx context.Context) error {
	for {
		reAuthRequired, lfcErr := v.manageTokenLifecycle(ctx, v.token)
		if lfcErr != nil {
			return fmt.Errorf("unable to manage token lifecycle: %v", lfcErr)
		}

		if reAuthRequired {
			newAuthToken, err := getAuthToken(ctx, &v.Auth, v.client)
			if err != nil {
				return fmt.Errorf("error during re-authentication after token expiry: %v", err)
			}
			v.token = newAuthToken
		}
	}
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
