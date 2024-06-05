// Copied from https://github.com/grafana/grafana-aws-sdk/pkg/awsds/settings.go
package sns

import (
	"crypto/sha256"
	"fmt"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/defaults"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/gtime"
	"github.com/grafana/grafana-plugin-sdk-go/backend/proxy"
	"github.com/grafana/grafana-plugin-sdk-go/build"
)

const (
	// AllowedAuthProvidersEnvVarKeyName is the string literal for the aws allowed auth providers environment variable key name
	AllowedAuthProvidersEnvVarKeyName = "AWS_AUTH_AllowedAuthProviders"

	// AssumeRoleEnabledEnvVarKeyName is the string literal for the aws assume role enabled environment variable key name
	AssumeRoleEnabledEnvVarKeyName = "AWS_AUTH_AssumeRoleEnabled"

	// SessionDurationEnvVarKeyName is the string literal for the session duration variable key name
	SessionDurationEnvVarKeyName = "AWS_AUTH_SESSION_DURATION"

	// GrafanaAssumeRoleExternalIDKeyName is the string literal for the grafana assume role external id environment variable key name
	GrafanaAssumeRoleExternalIDKeyName = "AWS_AUTH_EXTERNAL_ID"

	// ListMetricsPageLimitKeyName is the string literal for the cloudwatch list metrics page limit key name
	ListMetricsPageLimitKeyName = "AWS_CW_LIST_METRICS_PAGE_LIMIT"

	// SigV4AuthEnabledEnvVarKeyName is the string literal for the sigv4 auth enabled environment variable key name
	SigV4AuthEnabledEnvVarKeyName = "AWS_SIGV4_AUTH_ENABLED"

	// SigV4VerboseLoggingEnvVarKeyName is the string literal for the sigv4 verbose logging environment variable key name
	SigV4VerboseLoggingEnvVarKeyName = "AWS_SIGV4_VERBOSE_LOGGING"

	defaultRegion                    = "default"
	defaultAssumeRoleEnabled         = true
	defaultListMetricsPageLimit      = 500
	defaultSecureSocksDSProxyEnabled = false

	// path to the shared credentials file in the instance for the aws/aws-sdk
	// if empty string, the path is ~/.aws/credentials
	CredentialsPath = ""

	// the profile containing credentials for GrafanaAssueRole auth type in the shared credentials file
	ProfileName = "assume_role_credentials"
)

type AuthType int

const (
	AuthTypeDefault AuthType = iota
	AuthTypeSharedCreds
	AuthTypeKeys
	AuthTypeEC2IAMRole
	AuthTypeGrafanaAssumeRole //cloud only
)

// EC2 + ECS role credentials factory.
// Stubbable by tests.
var newRemoteCredentials = func(sess *session.Session) *credentials.Credentials {
	return credentials.NewCredentials(defaults.RemoteCredProvider(*sess.Config, sess.Handlers))
}

func (at AuthType) String() string {
	switch at {
	case AuthTypeDefault:
		return "default"
	case AuthTypeSharedCreds:
		return "credentials"
	case AuthTypeKeys:
		return "keys"
	case AuthTypeEC2IAMRole:
		return "ec2_iam_role"
	case AuthTypeGrafanaAssumeRole:
		return "grafana_assume_role"
	default:
		panic(fmt.Sprintf("Unrecognized auth type %d", at))
	}
}

// DatasourceSettings holds basic connection info
type AWSDatasourceSettings struct {
	Profile       string   `json:"profile"`
	Region        string   `json:"region"`
	AuthType      AuthType `json:"authType"`
	AssumeRoleARN string   `json:"assumeRoleARN"`
	ExternalID    string   `json:"externalId"`

	// Override the client endpoint
	Endpoint string `json:"endpoint"`

	//go:deprecated Use Region instead
	DefaultRegion string `json:"defaultRegion"`

	// Loaded from DecryptedSecureJSONData (not the json object)
	AccessKey    string `json:"-"`
	SecretKey    string `json:"-"`
	SessionToken string `json:"-"`
}

// AuthSettings stores the AWS settings from Grafana
type AuthSettings struct {
	AllowedAuthProviders []string
	AssumeRoleEnabled    bool
	SessionDuration      *time.Duration
	ExternalID           string
	ListMetricsPageLimit int

	// necessary for a work around until https://github.com/grafana/grafana/issues/39089 is implemented
	SecureSocksDSProxyEnabled bool
}

type SessionConfig struct {
	Settings      AWSDatasourceSettings
	HTTPClient    *http.Client
	UserAgentName *string
	AuthSettings  *AuthSettings
}

type envelope struct {
	session    *session.Session
	expiration time.Time
}

// SessionCache cache sessions for a while
type SessionCache struct {
	sessCache     map[string]envelope
	sessCacheLock sync.RWMutex
}

// GetSession returns a session from the config and possible region overrides -- implements AmazonSessionProvider
func (sc *SessionCache) GetSession(c SessionConfig) (*session.Session, error) {
	if c.Settings.Region == "" && c.Settings.DefaultRegion != "" {
		// DefaultRegion is deprecated, Region should be used instead
		c.Settings.Region = c.Settings.DefaultRegion
	}

	// If the datasource calling GetSession is getting the settings from the contexts, they'll pass
	// the values through AuthSettings. Otherwise, we need to get them from the env variables.
	if c.AuthSettings == nil {
		c.AuthSettings = ReadAuthSettingsFromEnvironmentVariables()
	}

	authTypeAllowed := false
	for _, provider := range c.AuthSettings.AllowedAuthProviders {
		if provider == c.Settings.AuthType.String() {
			authTypeAllowed = true
			break
		}
	}

	if !authTypeAllowed {
		return nil, fmt.Errorf("attempting to use an auth type that is not allowed: %q", c.Settings.AuthType.String())
	}

	if c.Settings.AssumeRoleARN != "" && !c.AuthSettings.AssumeRoleEnabled {
		return nil, fmt.Errorf("attempting to use assume role (ARN) which is disabled in grafana.ini")
	}

	// Hash the settings to use as a cache key
	bldr := strings.Builder{}
	for i, s := range []string{
		c.Settings.AuthType.String(), c.Settings.AccessKey, c.Settings.SecretKey, c.Settings.Profile, c.Settings.AssumeRoleARN, c.Settings.Region, c.Settings.Endpoint,
	} {
		if i != 0 {
			bldr.WriteString(":")
		}
		bldr.WriteString(strings.ReplaceAll(s, ":", `\:`))
	}

	hashedSettings := sha256.Sum256([]byte(bldr.String()))
	cacheKey := fmt.Sprintf("%v", hashedSettings)

	// Check if we have a valid session in the cache, if so return it
	sc.sessCacheLock.RLock()
	if env, ok := sc.sessCache[cacheKey]; ok {
		if env.expiration.After(time.Now().UTC()) {
			sc.sessCacheLock.RUnlock()
			return env.session, nil
		}
	}
	sc.sessCacheLock.RUnlock()

	cfgs := []*aws.Config{
		{
			CredentialsChainVerboseErrors: aws.Bool(true),
			HTTPClient:                    c.HTTPClient,
		},
	}

	var regionCfg *aws.Config
	if c.Settings.Region == defaultRegion {
		backend.Logger.Warn("Region is set to \"default\", which is unsupported")
		c.Settings.Region = ""
	}
	if c.Settings.Region != "" {
		if c.Settings.AssumeRoleARN != "" && c.AuthSettings.AssumeRoleEnabled && isOptInRegion(c.Settings.Region) {
			// When assuming a role, the real region is set later in a new session
			// so we use a well-known region here (not opt-in) to obtain valid credentials
			regionCfg = &aws.Config{Region: aws.String("us-east-1")}

			// set regional endpoint flag to obtain credentials that can be used in opt-in regions as well
			optInRegionCfg := &aws.Config{STSRegionalEndpoint: endpoints.RegionalSTSEndpoint}

			cfgs = append(cfgs, regionCfg, optInRegionCfg)
		} else {
			regionCfg = &aws.Config{Region: aws.String(c.Settings.Region)}
			cfgs = append(cfgs, regionCfg)
		}
	}

	switch c.Settings.AuthType {
	case AuthTypeSharedCreds:
		backend.Logger.Debug("Authenticating towards AWS with shared credentials", "profile", c.Settings.Profile,
			"region", c.Settings.Region)
		cfgs = append(cfgs, &aws.Config{
			Credentials: credentials.NewSharedCredentials(CredentialsPath, c.Settings.Profile),
		})
	case AuthTypeKeys:
		backend.Logger.Debug("Authenticating towards AWS with an access key pair", "region", c.Settings.Region)
		cfgs = append(cfgs, &aws.Config{
			Credentials: credentials.NewStaticCredentials(c.Settings.AccessKey, c.Settings.SecretKey, c.Settings.SessionToken),
		})
	case AuthTypeDefault:
		backend.Logger.Debug("Authenticating towards AWS with default SDK method", "region", c.Settings.Region)
	case AuthTypeEC2IAMRole:
		backend.Logger.Debug("Authenticating towards AWS with IAM Role", "region", c.Settings.Region)
		sess, err := session.NewSession(cfgs...)
		if err != nil {
			return nil, err
		}
		cfgs = append(cfgs, &aws.Config{Credentials: newRemoteCredentials(sess)})
	case AuthTypeGrafanaAssumeRole:
		backend.Logger.Debug("Authenticating towards AWS with Grafana Assume Role", "region", c.Settings.Region)
		cfgs = append(cfgs, &aws.Config{
			Credentials: credentials.NewSharedCredentials(CredentialsPath, ProfileName),
		})
	default:
		panic(fmt.Sprintf("Unrecognized authType: %d", c.Settings.AuthType))
	}

	duration := stscreds.DefaultDuration
	if c.AuthSettings.SessionDuration != nil {
		duration = *c.AuthSettings.SessionDuration
	}
	expiration := time.Now().UTC().Add(duration)
	if c.Settings.AssumeRoleARN != "" && c.AuthSettings.AssumeRoleEnabled {
		// We should assume a role in AWS
		backend.Logger.Debug("Trying to assume role in AWS", "arn", c.Settings.AssumeRoleARN)

		if c.Settings.Endpoint != "" {
			cfgs = append(cfgs, &aws.Config{Endpoint: aws.String(getSTSEndpoint(c.Settings.Endpoint))})
		}

		sess, err := session.NewSession(cfgs...)
		if err != nil {
			return nil, err
		}

		cfgs = []*aws.Config{
			{
				CredentialsChainVerboseErrors: aws.Bool(true),
			},
			{
				// The previous session is used to obtain STS Credentials
				Credentials: stscreds.NewCredentials(sess, c.Settings.AssumeRoleARN, func(p *stscreds.AssumeRoleProvider) {
					// Not sure if this is necessary, overlaps with p.Duration and is undocumented
					p.Expiry.SetExpiration(expiration, 0)
					p.Duration = duration
					if c.Settings.AuthType == AuthTypeGrafanaAssumeRole {
						p.ExternalID = aws.String(c.AuthSettings.ExternalID)
					} else if c.Settings.ExternalID != "" {
						p.ExternalID = aws.String(c.Settings.ExternalID)
					}
				}),
			},
		}

		if c.Settings.Region != "" {
			regionCfg = &aws.Config{Region: aws.String(c.Settings.Region)}
			cfgs = append(cfgs, regionCfg)
		}
	}

	if c.Settings.Endpoint != "" {
		cfgs = append(cfgs, &aws.Config{Endpoint: aws.String(c.Settings.Endpoint)})
	}

	sess, err := session.NewSession(cfgs...)
	if err != nil {
		return nil, err
	}

	if c.UserAgentName != nil {
		sess.Handlers.Send.PushFront(func(r *request.Request) {
			r.HTTPRequest.Header.Set("User-Agent", GetUserAgentString(*c.UserAgentName))
		})
	}

	backend.Logger.Debug("Successfully created AWS session")

	sc.sessCacheLock.Lock()
	sc.sessCache[cacheKey] = envelope{
		session:    sess,
		expiration: expiration,
	}
	sc.sessCacheLock.Unlock()

	return sess, nil
}

// ReadAuthSettingsFromEnvironmentVariables gets the Grafana auth settings from the environment variables
// Deprecated: Use ReadAuthSettingsFromContext instead
func ReadAuthSettingsFromEnvironmentVariables() *AuthSettings {
	authSettings := &AuthSettings{}
	allowedAuthProviders := []string{}
	providers := os.Getenv(AllowedAuthProvidersEnvVarKeyName)
	for _, authProvider := range strings.Split(providers, ",") {
		authProvider = strings.TrimSpace(authProvider)
		if authProvider != "" {
			allowedAuthProviders = append(allowedAuthProviders, authProvider)
		}
	}

	if len(allowedAuthProviders) == 0 {
		allowedAuthProviders = []string{"default", "keys", "credentials"}
		backend.Logger.Warn("could not find allowed auth providers. falling back to 'default, keys, credentials'")
	}
	authSettings.AllowedAuthProviders = allowedAuthProviders

	assumeRoleEnabledString := os.Getenv(AssumeRoleEnabledEnvVarKeyName)
	if len(assumeRoleEnabledString) == 0 {
		backend.Logger.Warn("environment variable missing. falling back to enable assume role", "var", AssumeRoleEnabledEnvVarKeyName)
		assumeRoleEnabledString = "true"
	}

	var err error
	authSettings.AssumeRoleEnabled, err = strconv.ParseBool(assumeRoleEnabledString)
	if err != nil {
		backend.Logger.Error("could not parse env variable", "var", AssumeRoleEnabledEnvVarKeyName)
		authSettings.AssumeRoleEnabled = defaultAssumeRoleEnabled
	}

	authSettings.ExternalID = os.Getenv(GrafanaAssumeRoleExternalIDKeyName)

	listMetricsPageLimitString := os.Getenv(ListMetricsPageLimitKeyName)
	if len(listMetricsPageLimitString) == 0 {
		backend.Logger.Warn("environment variable missing. falling back to default page limit", "var", ListMetricsPageLimitKeyName)
		listMetricsPageLimitString = "500"
	}

	authSettings.ListMetricsPageLimit, err = strconv.Atoi(listMetricsPageLimitString)
	if err != nil {
		backend.Logger.Error("could not parse env variable", "var", ListMetricsPageLimitKeyName)
		authSettings.ListMetricsPageLimit = defaultListMetricsPageLimit
	}

	sessionDurationString := os.Getenv(SessionDurationEnvVarKeyName)
	if sessionDurationString != "" {
		sessionDuration, err := gtime.ParseDuration(sessionDurationString)
		if err != nil {
			backend.Logger.Error("could not parse env variable", "var", SessionDurationEnvVarKeyName)
		} else {
			authSettings.SessionDuration = &sessionDuration
		}
	}

	proxyEnabledString := os.Getenv(proxy.PluginSecureSocksProxyEnabled)
	if len(proxyEnabledString) == 0 {
		backend.Logger.Warn("environment variable missing. falling back to proxy disabled", "var", proxy.PluginSecureSocksProxyEnabled)
		proxyEnabledString = "false"
	}

	authSettings.SecureSocksDSProxyEnabled, err = strconv.ParseBool(proxyEnabledString)
	if err != nil {
		backend.Logger.Error("could not parse env variable", "var", proxy.PluginSecureSocksProxyEnabled)
		authSettings.SecureSocksDSProxyEnabled = defaultSecureSocksDSProxyEnabled
	}

	return authSettings
}

// NewSessionCache creates a new session cache using the default settings loaded from environment variables
func NewSessionCache() *SessionCache {
	return &SessionCache{
		sessCache: map[string]envelope{},
	}
}

func isOptInRegion(region string) bool {
	// Opt-in region from https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-regions-availability-zones.html#concepts-available-regions
	regions := map[string]bool{
		"af-south-1":     true,
		"ap-east-1":      true,
		"ap-south-2":     true,
		"ap-southeast-3": true,
		"ap-southeast-4": true,
		"ca-west-1":      true,
		"eu-central-2":   true,
		"eu-south-1":     true,
		"eu-south-2":     true,
		"il-central-1":   true,
		"me-central-1":   true,
		"me-south-1":     true,
		// The rest of regions will return false
	}
	return regions[region]
}

// getSTSEndpoint checks if the set endpoint is a fips endpoint, and if so, returns the STS fips endpoint for the same region
func getSTSEndpoint(endpoint string) string {
	if endpoint == "" {
		return ""
	}
	if strings.Contains(endpoint, "fips") {
		switch {
		case strings.Contains(endpoint, "us-east-1"):
			return "sts-fips.us-east-1.amazonaws.com"
		case strings.Contains(endpoint, "us-east-2"):
			return "sts-fips.us-east-2.amazonaws.com"
		case strings.Contains(endpoint, "us-west-1"):
			return "sts-fips.us-west-1.amazonaws.com"
		case strings.Contains(endpoint, "us-west-2"):
			return "sts-fips.us-west-2.amazonaws.com"
		}
	}

	if strings.Contains(endpoint, "us-gov-east-1") {
		return "sts.us-gov-east-1.amazonaws.com"
	}
	if strings.Contains(endpoint, "us-gov-west-1") {
		return "sts.us-gov-west-1.amazonaws.com"
	}
	return ""
}

// GetUserAgentString returns an agent that can be parsed in server logs
func GetUserAgentString(name string) string {
	// Build info is set from compile time flags
	buildInfo, err := build.GetBuildInfo()
	if err != nil {
		buildInfo.Version = "dev"
		buildInfo.Hash = "?"
	}

	if len(buildInfo.Hash) > 8 {
		buildInfo.Hash = buildInfo.Hash[0:8]
	}

	grafanaVersion := os.Getenv("GF_VERSION")
	if grafanaVersion == "" {
		grafanaVersion = "?"
	}

	// Determine if running in an Amazon Managed Grafana environment
	_, amgEnv := os.LookupEnv("AMAZON_MANAGED_GRAFANA")

	return fmt.Sprintf("%s/%s (%s; %s;) %s/%s-%s Grafana/%s AMG/%s",
		aws.SDKName,
		aws.SDKVersion,
		runtime.Version(),
		runtime.GOOS,
		name,
		buildInfo.Version,
		buildInfo.Hash,
		grafanaVersion,
		strconv.FormatBool(amgEnv))
}
