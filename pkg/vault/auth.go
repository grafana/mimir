// SPDX-License-Identifier: AGPL-3.0-only

package vault

import (
	"context"
	"flag"
	"fmt"
	"strings"

	"github.com/grafana/dskit/flagext"
	hashivault "github.com/hashicorp/vault/api"
	"github.com/hashicorp/vault/api/auth/approle"
	"github.com/hashicorp/vault/api/auth/kubernetes"
	"github.com/hashicorp/vault/api/auth/userpass"
)

const (
	AppRole    = "approle"
	Kubernetes = "kubernetes"
	UserPass   = "userpass"
	Token      = "token"
)

type authMethod interface {
	authenticate(client *hashivault.Client) (*hashivault.Secret, error)
}

type AuthConfig struct {
	AuthType string `yaml:"type"`

	AuthAppRole    AuthAppRole    `yaml:"app_role,omitempty"`
	AuthKubernetes AuthKubernetes `yaml:"kubernetes,omitempty"`
	AuthUserPass   AuthUserPass   `yaml:"user_pass,omitempty"`
	AuthToken      AuthToken      `yaml:"token,omitempty"`
}

func (cfg *AuthConfig) supportedMethods() []string {
	return []string{AppRole, Kubernetes, UserPass, Token}
}

func (cfg *AuthConfig) RegisterFlagsWithPrefix(f *flag.FlagSet, prefix string) {
	f.StringVar(&cfg.AuthType, prefix+"type", "", fmt.Sprintf("Authentication type to use. Supported types are: %s", strings.Join(cfg.supportedMethods(), ", ")))
	cfg.AuthAppRole.RegisterFlagsWithPrefix(f, prefix)
	cfg.AuthKubernetes.RegisterFlagsWithPrefix(f, prefix)
	cfg.AuthUserPass.RegisterFlagsWithPrefix(f, prefix)
	cfg.AuthToken.RegisterFlagsWithPrefix(f, prefix)
}

func (am *AuthConfig) authMethod() (authMethod, error) {
	switch am.AuthType {
	case AppRole:
		return &am.AuthAppRole, nil
	case Kubernetes:
		return &am.AuthKubernetes, nil
	case UserPass:
		return &am.AuthUserPass, nil
	case Token:
		return &am.AuthToken, nil
	}

	return nil, fmt.Errorf("unsupported auth method")
}

type AuthAppRole struct {
	RoleID        string         `yaml:"role_id"`
	SecretID      flagext.Secret `yaml:"secret_id"`
	WrappingToken string         `yaml:"wrapping_token,omitempty"`
	MountPath     string         `yaml:"mount_path,omitempty"`
}

func (cfg *AuthAppRole) RegisterFlagsWithPrefix(f *flag.FlagSet, prefix string) {
	f.StringVar(&cfg.RoleID, prefix+"approle.role-id", "", "Role ID of the AppRole")
	f.Var(&cfg.SecretID, prefix+"approle.secret-id", "Secret ID issues against the AppRole")
	f.StringVar(&cfg.WrappingToken, prefix+"approle.wrapping-token", "", "Response wrapping token if the Secret ID is response wrapped")
	f.StringVar(&cfg.MountPath, prefix+"approle.mount-path", "", "Path if the Vault backend was mounted using a non-default path")
}

func (a *AuthAppRole) authenticate(client *hashivault.Client) (*hashivault.Secret, error) {
	secretID := &approle.SecretID{FromString: a.SecretID.String()}

	var opts []approle.LoginOption
	if a.WrappingToken != "" {
		opts = append(opts, approle.WithWrappingToken())
	}
	if a.MountPath != "" {
		opts = append(opts, approle.WithMountPath(a.MountPath))
	}

	appRoleAuth, err := approle.NewAppRoleAuth(
		a.RoleID,
		secretID,
		opts...,
	)
	if err != nil {
		return nil, fmt.Errorf("unable to initialize approle authentication: %w", err)
	}

	secret, err := client.Auth().Login(context.Background(), appRoleAuth)
	if err != nil {
		return nil, fmt.Errorf("unable to log in with approle authentication: %w", err)
	}

	return secret, nil
}

type AuthKubernetes struct {
	RoleName                string         `yaml:"role_name"`
	ServiceAccountToken     flagext.Secret `yaml:"service_account_token,omitempty"`
	ServiceAccountTokenPath string         `yaml:"service_account_token_path,omitempty"`
	MountPath               string         `yaml:"mount_path,omitempty"`
}

func (cfg *AuthKubernetes) RegisterFlagsWithPrefix(f *flag.FlagSet, prefix string) {
	f.StringVar(&cfg.RoleName, prefix+"kubernetes.role-name", "", "The Kubernetes named role")
	f.Var(&cfg.ServiceAccountToken, prefix+"kubernetes.service-account-token", "The Service Account JWT")
	f.StringVar(&cfg.ServiceAccountTokenPath, prefix+"kubernetes.service-account-token-path", "", "Path to where the Kubernetes service account token is mounted. By default it lives at /var/run/secrets/kubernetes.io/serviceaccount/token. Field will be used if the ServiceAccountToken is not specified.")
	f.StringVar(&cfg.MountPath, prefix+"kubernetes.mount-path", "", "Path if the Vault backend was mounted using a non-default path")
}

func (a *AuthKubernetes) authenticate(client *hashivault.Client) (*hashivault.Secret, error) {
	var opts []kubernetes.LoginOption
	if a.ServiceAccountToken.String() != "" {
		opts = append(opts, kubernetes.WithServiceAccountToken(a.ServiceAccountToken.String()))
	}
	if a.ServiceAccountTokenPath != "" {
		opts = append(opts, kubernetes.WithServiceAccountTokenPath(a.ServiceAccountTokenPath))
	}
	if a.MountPath != "" {
		opts = append(opts, kubernetes.WithMountPath(a.MountPath))
	}

	k8sAuth, err := kubernetes.NewKubernetesAuth(
		a.RoleName,
		opts...,
	)
	if err != nil {
		return nil, fmt.Errorf("unable to initialize kubernetes authentication: %w", err)
	}

	secret, err := client.Auth().Login(context.Background(), k8sAuth)
	if err != nil {
		return nil, fmt.Errorf("unable to log in with kubernetes authentication: %w", err)
	}

	return secret, nil
}

type AuthUserPass struct {
	Username  string         `yaml:"username"`
	Password  flagext.Secret `yaml:"password"`
	MountPath string         `yaml:"mount_path,omitempty"`
}

func (cfg *AuthUserPass) RegisterFlagsWithPrefix(f *flag.FlagSet, prefix string) {
	f.StringVar(&cfg.Username, prefix+"userpass.username", "", "The userpass auth method username")
	f.Var(&cfg.Password, prefix+"userpass.password", "The userpass auth method password")
	f.StringVar(&cfg.MountPath, prefix+"userpass.mount-path", "", "Path if the Vault backend was mounted using a non-default path")
}

func (a *AuthUserPass) authenticate(client *hashivault.Client) (*hashivault.Secret, error) {
	var opts []userpass.LoginOption
	if a.MountPath != "" {
		opts = append(opts, userpass.WithMountPath(a.MountPath))
	}

	userpassAuth, err := userpass.NewUserpassAuth(
		a.Username,
		&userpass.Password{FromString: a.Password.String()},
		opts...,
	)
	if err != nil {
		return nil, fmt.Errorf("unable to initialize userpass authentication: %w", err)
	}

	secret, err := client.Auth().Login(context.Background(), userpassAuth)
	if err != nil {
		return nil, fmt.Errorf("unable to log in with userpass authentication: %w", err)
	}

	return secret, nil
}

type AuthToken struct {
	Token flagext.Secret `yaml:"token"`
}

func (cfg *AuthToken) RegisterFlagsWithPrefix(f *flag.FlagSet, prefix string) {
	f.Var(&cfg.Token, prefix+"token", "The token used to authenticate against Vault")
}

func (a *AuthToken) authenticate(client *hashivault.Client) (*hashivault.Secret, error) {
	client.SetToken(a.Token.String())

	return nil, nil
}
