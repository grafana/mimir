// SPDX-License-Identifier: AGPL-3.0-only

package vault

import (
	"context"
	"testing"

	"github.com/grafana/dskit/flagext"
	"github.com/hashicorp/vault/api"
	"github.com/hashicorp/vault/api/auth/approle"
	"github.com/hashicorp/vault/api/auth/kubernetes"
	"github.com/hashicorp/vault/api/auth/userpass"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type authFactoryMock struct {
	mock.Mock
}

func (m *authFactoryMock) NewAppRoleAuth(roleID string, secretID *approle.SecretID, opts ...approle.LoginOption) (*approle.AppRoleAuth, error) {
	argsToPass := make([]any, 0, len(opts)+2)
	argsToPass = append(argsToPass, roleID, secretID)
	for _, opt := range opts {
		argsToPass = append(argsToPass, opt)
	}

	args := m.Called(argsToPass...)
	return args.Get(0).(*approle.AppRoleAuth), args.Error(1)
}

func (m *authFactoryMock) NewKubernetesAuth(roleName string, opts ...kubernetes.LoginOption) (*kubernetes.KubernetesAuth, error) {
	argsToPass := make([]any, 0, len(opts)+1)
	argsToPass = append(argsToPass, roleName)
	for _, opt := range opts {
		argsToPass = append(argsToPass, opt)
	}

	args := m.Called(argsToPass...)
	return args.Get(0).(*kubernetes.KubernetesAuth), args.Error(1)
}

func (m *authFactoryMock) NewUserpassAuth(username string, password string, opts ...userpass.LoginOption) (*userpass.UserpassAuth, error) {
	argsToPass := make([]any, 0, len(opts)+2)
	argsToPass = append(argsToPass, username, password)
	for _, opt := range opts {
		argsToPass = append(argsToPass, opt)
	}

	args := m.Called(argsToPass...)
	return args.Get(0).(*userpass.UserpassAuth), args.Error(1)
}

func TestAppRoleAuthenticate(t *testing.T) {
	testRoleID := "testRoleID"
	testSecretID := "testSecretID"
	testMountPath := "testMountPath"

	cfg := Config{
		Enabled: true,
		Auth: AuthConfig{
			AuthType: AppRole,
			AuthAppRole: AuthAppRole{
				RoleID:        testRoleID,
				SecretID:      flagext.SecretWithValue(testSecretID),
				WrappingToken: true,
				MountPath:     testMountPath,
			},
		},
	}

	authMethod, err := cfg.Auth.authMethod()
	require.NoError(t, err)

	factoryMock := authFactoryMock{}
	factoryMock.On("NewAppRoleAuth", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&approle.AppRoleAuth{}, nil)

	client, err := api.NewClient(nil)
	require.NoError(t, err)
	authMethod.authenticate(context.Background(), &factoryMock, client)
	factoryMock.AssertCalled(t, "NewAppRoleAuth", testRoleID, &approle.SecretID{FromString: testSecretID}, mock.AnythingOfType("approle.LoginOption"), mock.AnythingOfType("approle.LoginOption"))
}

func TestAppRoleAuthenticateSingleLoginOption(t *testing.T) {
	testRoleID := "testRoleID"
	testSecretID := "testSecretID"

	cfg := Config{
		Enabled: true,
		Auth: AuthConfig{
			AuthType: AppRole,
			AuthAppRole: AuthAppRole{
				RoleID:        testRoleID,
				SecretID:      flagext.SecretWithValue(testSecretID),
				WrappingToken: true,
			},
		},
	}

	authMethod, err := cfg.Auth.authMethod()
	require.NoError(t, err)

	factoryMock := authFactoryMock{}
	factoryMock.On("NewAppRoleAuth", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&approle.AppRoleAuth{}, nil)

	client, err := api.NewClient(nil)
	require.NoError(t, err)
	authMethod.authenticate(context.Background(), &factoryMock, client)
	factoryMock.AssertCalled(t, "NewAppRoleAuth", testRoleID, &approle.SecretID{FromString: testSecretID}, mock.AnythingOfType("approle.LoginOption"))
}

func TestKubernetesAuthenticate(t *testing.T) {
	testRoleName := "testRoleName"
	testToken := "testToken"
	testPath := "testPath"
	testMountPath := "testMountPath"

	cfg := Config{
		Enabled: true,
		Auth: AuthConfig{
			AuthType: Kubernetes,
			AuthKubernetes: AuthKubernetes{
				RoleName:                testRoleName,
				ServiceAccountToken:     flagext.SecretWithValue(testToken),
				ServiceAccountTokenPath: testPath,
				MountPath:               testMountPath,
			},
		},
	}

	authMethod, err := cfg.Auth.authMethod()
	require.NoError(t, err)

	factoryMock := authFactoryMock{}
	factoryMock.On("NewKubernetesAuth", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&kubernetes.KubernetesAuth{}, nil)

	client, err := api.NewClient(nil)
	require.NoError(t, err)
	authMethod.authenticate(context.Background(), &factoryMock, client)
	factoryMock.AssertCalled(t, "NewKubernetesAuth", testRoleName, mock.AnythingOfType("kubernetes.LoginOption"), mock.AnythingOfType("kubernetes.LoginOption"), mock.AnythingOfType("kubernetes.LoginOption"))
}

func TestKubernetesAuthenticateSingleLoginOption(t *testing.T) {
	testRoleName := "testRoleName"
	testToken := "testToken"
	testMountPath := "testMountPath"

	cfg := Config{
		Enabled: true,
		Auth: AuthConfig{
			AuthType: Kubernetes,
			AuthKubernetes: AuthKubernetes{
				RoleName:            testRoleName,
				ServiceAccountToken: flagext.SecretWithValue(testToken),
				MountPath:           testMountPath,
			},
		},
	}

	authMethod, err := cfg.Auth.authMethod()
	require.NoError(t, err)

	factoryMock := authFactoryMock{}
	factoryMock.On("NewKubernetesAuth", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&kubernetes.KubernetesAuth{}, nil)

	client, err := api.NewClient(nil)
	require.NoError(t, err)
	authMethod.authenticate(context.Background(), &factoryMock, client)
	factoryMock.AssertCalled(t, "NewKubernetesAuth", testRoleName, mock.AnythingOfType("kubernetes.LoginOption"), mock.AnythingOfType("kubernetes.LoginOption"))
}

func TestUserpassAuthenticate(t *testing.T) {
	testUsername := "testUser"
	testPassword := "testPW"
	testMountPath := "testMountPath"

	cfg := Config{
		Enabled: true,
		Auth: AuthConfig{
			AuthType: UserPass,
			AuthUserPass: AuthUserPass{
				Username:  testUsername,
				Password:  flagext.SecretWithValue(testPassword),
				MountPath: testMountPath,
			},
		},
	}

	authMethod, err := cfg.Auth.authMethod()
	require.NoError(t, err)

	factoryMock := authFactoryMock{}
	factoryMock.On("NewUserpassAuth", mock.Anything, mock.Anything, mock.Anything).Return(&userpass.UserpassAuth{}, nil)

	client, err := api.NewClient(nil)
	require.NoError(t, err)
	authMethod.authenticate(context.Background(), &factoryMock, client)
	factoryMock.AssertCalled(t, "NewUserpassAuth", testUsername, testPassword, mock.AnythingOfType("userpass.LoginOption"))
}
