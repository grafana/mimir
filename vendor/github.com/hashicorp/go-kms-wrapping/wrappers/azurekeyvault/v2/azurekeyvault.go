package azurekeyvault

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync/atomic"

	"github.com/Azure/azure-sdk-for-go/services/keyvault/v7.1/keyvault"
	"github.com/Azure/go-autorest/autorest"
	"github.com/Azure/go-autorest/autorest/azure"
	"github.com/Azure/go-autorest/autorest/azure/auth"
	"github.com/Azure/go-autorest/autorest/to"
	"github.com/hashicorp/go-hclog"
	wrapping "github.com/hashicorp/go-kms-wrapping/v2"
)

const (
	EnvAzureKeyVaultWrapperVaultName = "AZUREKEYVAULT_WRAPPER_VAULT_NAME"
	EnvVaultAzureKeyVaultVaultName   = "VAULT_AZUREKEYVAULT_VAULT_NAME"

	EnvAzureKeyVaultWrapperKeyName = "AZUREKEYVAULT_WRAPPER_KEY_NAME"
	EnvVaultAzureKeyVaultKeyName   = "VAULT_AZUREKEYVAULT_KEY_NAME"
)

// Wrapper is an Wrapper that uses Azure Key Vault
// for crypto operations.  Azure Key Vault currently does not support
// keys that can encrypt long data (RSA keys).  Due to this fact, we generate
// and AES key and wrap the key using Key Vault and store it with the
// data
type Wrapper struct {
	tenantID     string
	clientID     string
	clientSecret string
	vaultName    string
	keyName      string

	currentKeyId *atomic.Value

	environment    azure.Environment
	resource       string
	client         *keyvault.BaseClient
	logger         hclog.Logger
	keyNotRequired bool
	baseURL        string
}

// Ensure that we are implementing Wrapper
var _ wrapping.Wrapper = (*Wrapper)(nil)

// NewWrapper creates a new wrapper with the given options
func NewWrapper() *Wrapper {
	v := &Wrapper{
		currentKeyId: new(atomic.Value),
	}
	v.currentKeyId.Store("")
	return v
}

// SetConfig sets the fields on the Wrapper object based on
// values from the config parameter.
//
// Order of precedence:
// * Environment variable
// * Passed in config map
// * Managed Service Identity for instance
func (v *Wrapper) SetConfig(_ context.Context, opt ...wrapping.Option) (*wrapping.WrapperConfig, error) {
	opts, err := getOpts(opt...)
	if err != nil {
		return nil, err
	}

	v.keyNotRequired = opts.withKeyNotRequired
	v.logger = opts.withLogger

	switch {
	case os.Getenv("AZURE_TENANT_ID") != "" && !opts.withDisallowEnvVars:
		v.tenantID = os.Getenv("AZURE_TENANT_ID")
	case opts.withTenantId != "":
		v.tenantID = opts.withTenantId
	}

	switch {
	case os.Getenv("AZURE_CLIENT_ID") != "" && !opts.withDisallowEnvVars:
		v.clientID = os.Getenv("AZURE_CLIENT_ID")
	case opts.withClientId != "":
		v.clientID = opts.withClientId
	}

	switch {
	case os.Getenv("AZURE_CLIENT_SECRET") != "" && !opts.withDisallowEnvVars:
		v.clientSecret = os.Getenv("AZURE_CLIENT_SECRET")
	case opts.withClientSecret != "":
		v.clientSecret = opts.withClientSecret
	}

	var envName string
	if !opts.withDisallowEnvVars {
		envName = os.Getenv("AZURE_ENVIRONMENT")
	}
	if envName == "" {
		envName = opts.withEnvironment
	}
	if envName == "" {
		v.environment = azure.PublicCloud
	} else {
		var err error
		v.environment, err = azure.EnvironmentFromName(envName)
		if err != nil {
			return nil, err
		}
	}

	var azResource string
	if !opts.withDisallowEnvVars {
		azResource = os.Getenv("AZURE_AD_RESOURCE")
	}
	if azResource == "" {
		azResource = opts.withResource
		if azResource == "" {
			azResource = v.environment.KeyVaultDNSSuffix
		}
	}
	v.environment.KeyVaultDNSSuffix = azResource
	v.resource = "https://" + azResource + "/"
	v.environment.KeyVaultEndpoint = v.resource

	switch {
	case os.Getenv(EnvAzureKeyVaultWrapperVaultName) != "" && !opts.withDisallowEnvVars:
		v.vaultName = os.Getenv(EnvAzureKeyVaultWrapperVaultName)
	case os.Getenv(EnvVaultAzureKeyVaultVaultName) != "" && !opts.withDisallowEnvVars:
		v.vaultName = os.Getenv(EnvVaultAzureKeyVaultVaultName)
	case opts.withVaultName != "":
		v.vaultName = opts.withVaultName
	default:
		return nil, errors.New("vault name is required")
	}

	switch {
	case os.Getenv(EnvAzureKeyVaultWrapperKeyName) != "" && !opts.withDisallowEnvVars:
		v.keyName = os.Getenv(EnvAzureKeyVaultWrapperKeyName)
	case os.Getenv(EnvVaultAzureKeyVaultKeyName) != "" && !opts.withDisallowEnvVars:
		v.keyName = os.Getenv(EnvVaultAzureKeyVaultKeyName)
	case opts.withKeyName != "":
		v.keyName = opts.withKeyName
	case v.keyNotRequired:
		// key not required to set config
	default:
		return nil, errors.New("key name is required")
	}

	// Set the base URL
	v.baseURL = v.buildBaseURL()

	if v.client == nil {
		client, err := v.getKeyVaultClient()
		if err != nil {
			return nil, fmt.Errorf("error initializing Azure Key Vault wrapper client: %w", err)
		}

		if !v.keyNotRequired {
			// Test the client connection using provided key ID
			keyInfo, err := client.GetKey(context.Background(), v.baseURL, v.keyName, "")
			if err != nil {
				return nil, fmt.Errorf("error fetching Azure Key Vault wrapper key information: %w", err)
			}
			if keyInfo.Key == nil {
				return nil, errors.New("no key information returned")
			}
			v.currentKeyId.Store(ParseKeyVersion(to.String(keyInfo.Key.Kid)))
		}

		v.client = client
	}

	// Map that holds non-sensitive configuration info
	wrapConfig := new(wrapping.WrapperConfig)
	wrapConfig.Metadata = make(map[string]string)
	wrapConfig.Metadata["environment"] = v.environment.Name
	wrapConfig.Metadata["vault_name"] = v.vaultName
	wrapConfig.Metadata["key_name"] = v.keyName
	wrapConfig.Metadata["resource"] = v.resource

	return wrapConfig, nil
}

// Type returns the type for this particular Wrapper implementation
func (v *Wrapper) Type(_ context.Context) (wrapping.WrapperType, error) {
	return wrapping.WrapperTypeAzureKeyVault, nil
}

// KeyId returns the last known key id
func (v *Wrapper) KeyId(_ context.Context) (string, error) {
	return v.currentKeyId.Load().(string), nil
}

// Encrypt is used to encrypt using Azure Key Vault.
// This returns the ciphertext, and/or any errors from this
// call.
func (v *Wrapper) Encrypt(ctx context.Context, plaintext []byte, opt ...wrapping.Option) (*wrapping.BlobInfo, error) {
	if plaintext == nil {
		return nil, errors.New("given plaintext for encryption is nil")
	}

	env, err := wrapping.EnvelopeEncrypt(plaintext, opt...)
	if err != nil {
		return nil, fmt.Errorf("error wrapping dat: %w", err)
	}

	// Encrypt the DEK using Key Vault
	params := keyvault.KeyOperationsParameters{
		Algorithm: keyvault.RSAOAEP256,
		Value:     to.StringPtr(base64.URLEncoding.WithPadding(base64.NoPadding).EncodeToString(env.Key)),
	}
	// Wrap key with the latest version for the key name
	resp, err := v.client.WrapKey(ctx, v.buildBaseURL(), v.keyName, "", params)
	if err != nil {
		return nil, err
	}

	// Store the current key version
	keyVersion := ParseKeyVersion(to.String(resp.Kid))
	v.currentKeyId.Store(keyVersion)

	ret := &wrapping.BlobInfo{
		Ciphertext: env.Ciphertext,
		Iv:         env.Iv,
		KeyInfo: &wrapping.KeyInfo{
			KeyId:      keyVersion,
			WrappedKey: []byte(to.String(resp.Result)),
		},
	}

	return ret, nil
}

// Decrypt is used to decrypt the ciphertext
func (v *Wrapper) Decrypt(ctx context.Context, in *wrapping.BlobInfo, opt ...wrapping.Option) ([]byte, error) {
	if in == nil {
		return nil, errors.New("given input for decryption is nil")
	}

	if in.KeyInfo == nil {
		return nil, errors.New("key info is nil")
	}

	// Unwrap the key
	params := keyvault.KeyOperationsParameters{
		Algorithm: keyvault.RSAOAEP256,
		Value:     to.StringPtr(string(in.KeyInfo.WrappedKey)),
	}
	resp, err := v.client.UnwrapKey(ctx, v.buildBaseURL(), v.keyName, in.KeyInfo.KeyId, params)
	if err != nil {
		return nil, err
	}

	keyBytes, err := base64.URLEncoding.WithPadding(base64.NoPadding).DecodeString(to.String(resp.Result))
	if err != nil {
		return nil, err
	}

	// XXX: Workaround: Azure Managed HSM KeyVault's REST API request parser
	// changes the encrypted key to include an extra NULL byte at the end.
	// This looks like the base64 of the symmetric AES wrapping key above is
	// changed from ...= to ...A. You'll get the error (when running Vault
	// init / unseal operation):
	// > failed to unseal barrier: failed to check for keyring: failed to create cipher: crypto/aes: invalid key size 33
	// until this is fixed.
	//  -> 16-byte / 128-bit AES key gets two padding characters, resulting
	//     in two null bytes.
	//  -> 24-byte / 196-bit AES key gets no padding and no null bytes.
	//  -> 32-byte / 256-bit AES key (default) gets one padding character,
	//     resulting in one null bytes.
	if len(keyBytes) == 18 && keyBytes[16] == 0 && keyBytes[17] == 0 {
		keyBytes = keyBytes[:16]
	} else if len(keyBytes) == 33 && keyBytes[32] == 0 {
		keyBytes = keyBytes[:32]
	}

	envInfo := &wrapping.EnvelopeInfo{
		Key:        keyBytes,
		Iv:         in.Iv,
		Ciphertext: in.Ciphertext,
	}
	return wrapping.EnvelopeDecrypt(envInfo, opt...)
}

func (v *Wrapper) buildBaseURL() string {
	return fmt.Sprintf("https://%s.%s/", v.vaultName, v.environment.KeyVaultDNSSuffix)
}

func (v *Wrapper) getKeyVaultClient() (*keyvault.BaseClient, error) {
	var authorizer autorest.Authorizer
	var err error

	switch {
	case v.clientID != "" && v.clientSecret != "":
		config := auth.NewClientCredentialsConfig(v.clientID, v.clientSecret, v.tenantID)
		config.AADEndpoint = v.environment.ActiveDirectoryEndpoint
		config.Resource = strings.TrimSuffix(v.resource, "/")
		authorizer, err = config.Authorizer()
		if err != nil {
			return nil, err
		}
	// By default use MSI
	default:
		config := auth.NewMSIConfig()
		config.Resource = strings.TrimSuffix(v.resource, "/")
		if v.clientID != "" {
			config.ClientID = v.clientID
		}
		authorizer, err = config.Authorizer()
		if err != nil {
			return nil, err
		}
	}

	client := keyvault.New()
	client.Authorizer = authorizer
	return &client, nil
}

// Client returns the AzureKeyVault client used by the wrapper.
func (v *Wrapper) Client() *keyvault.BaseClient {
	return v.client
}

// Logger returns the logger used by the wrapper.
func (v *Wrapper) Logger() hclog.Logger {
	return v.logger
}

// BaseURL returns the base URL for key management operation requests based
// on the Azure Vault name and environment.
func (v *Wrapper) BaseURL() string {
	return v.baseURL
}

// Kid gets returned as a full URL, get the last bit which is just
// the version
func ParseKeyVersion(kid string) string {
	keyVersionParts := strings.Split(kid, "/")
	return keyVersionParts[len(keyVersionParts)-1]
}
