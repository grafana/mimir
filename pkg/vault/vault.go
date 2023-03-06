// SPDX-License-Identifier: AGPL-3.0-only

package vault

import (
	"context"
	"flag"
	"fmt"

	hashivault "github.com/hashicorp/vault/api"
)

// Config for the Vault used to fetch secrets
type Config struct {
	URL       string `yaml:"url" category:"advanced"`
	Token     string `yaml:"token" category:"advanced"`
	MountPath string `yaml:"mount_path" category:"advanced"`
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&cfg.URL, "vault.url", "", "Location of the Vault server")
	f.StringVar(&cfg.Token, "vault.token", "", "Token used to authenticate with Vault")
	f.StringVar(&cfg.MountPath, "vault.mount-path", "", "")
}

type Vault struct {
	client *hashivault.Client
	config Config
}

func NewVault(cfg Config) (*Vault, error) {
	config := hashivault.DefaultConfig()
	config.Address = cfg.URL

	client, err := hashivault.NewClient(config)
	if err != nil {
		return nil, err
	}

	client.SetToken(cfg.Token)
	vault := &Vault{
		client: client,
		config: cfg,
	}

	return vault, nil
}

func (v *Vault) ReadSecret(path string) ([]byte, error) {
	secret, err := v.client.KVv2(v.config.MountPath).Get(context.Background(), path)
	if err != nil {
		return nil, fmt.Errorf("unable to read secret from vault: %v", err)
	}

	data := []byte(secret.Data["value"].(string))
	return data, nil
}
