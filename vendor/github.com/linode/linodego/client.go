package linodego

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"strconv"
	"sync"
	"time"

	"github.com/go-resty/resty/v2"
)

const (
	// APIConfigEnvVar environment var to get path to Linode config
	APIConfigEnvVar = "LINODE_CONFIG"
	// APIConfigProfileEnvVar specifies the profile to use when loading from a Linode config
	APIConfigProfileEnvVar = "LINODE_PROFILE"
	// APIHost Linode API hostname
	APIHost = "api.linode.com"
	// APIHostVar environment var to check for alternate API URL
	APIHostVar = "LINODE_URL"
	// APIHostCert environment var containing path to CA cert to validate against
	APIHostCert = "LINODE_CA"
	// APIVersion Linode API version
	APIVersion = "v4"
	// APIVersionVar environment var to check for alternate API Version
	APIVersionVar = "LINODE_API_VERSION"
	// APIProto connect to API with http(s)
	APIProto = "https"
	// APIEnvVar environment var to check for API token
	APIEnvVar = "LINODE_TOKEN"
	// APISecondsPerPoll how frequently to poll for new Events or Status in WaitFor functions
	APISecondsPerPoll = 3
	// Maximum wait time for retries
	APIRetryMaxWaitTime = time.Duration(30) * time.Second
)

var envDebug = false

// Client is a wrapper around the Resty client
type Client struct {
	resty             *resty.Client
	userAgent         string
	debug             bool
	retryConditionals []RetryConditional

	millisecondsPerPoll time.Duration

	baseURL         string
	apiVersion      string
	apiProto        string
	selectedProfile string
	loadedProfile   string

	configProfiles map[string]ConfigProfile

	// Fields for caching endpoint responses
	shouldCache     bool
	cacheExpiration time.Duration
	cachedEntries   map[string]clientCacheEntry
	cachedEntryLock *sync.RWMutex
}

type EnvDefaults struct {
	Token   string
	Profile string
}

type clientCacheEntry struct {
	Created time.Time
	Data    any
	// If != nil, use this instead of the
	// global expiry
	ExpiryOverride *time.Duration
}

type Request = resty.Request

func init() {
	// Wether or not we will enable Resty debugging output
	if apiDebug, ok := os.LookupEnv("LINODE_DEBUG"); ok {
		if parsed, err := strconv.ParseBool(apiDebug); err == nil {
			envDebug = parsed
			log.Println("[INFO] LINODE_DEBUG being set to", envDebug)
		} else {
			log.Println("[WARN] LINODE_DEBUG should be an integer, 0 or 1")
		}
	}
}

// SetUserAgent sets a custom user-agent for HTTP requests
func (c *Client) SetUserAgent(ua string) *Client {
	c.userAgent = ua
	c.resty.SetHeader("User-Agent", c.userAgent)

	return c
}

// R wraps resty's R method
func (c *Client) R(ctx context.Context) *resty.Request {
	return c.resty.R().
		ExpectContentType("application/json").
		SetHeader("Content-Type", "application/json").
		SetContext(ctx).
		SetError(APIError{})
}

// SetDebug sets the debug on resty's client
func (c *Client) SetDebug(debug bool) *Client {
	c.debug = debug
	c.resty.SetDebug(debug)

	return c
}

// OnBeforeRequest adds a handler to the request body to run before the request is sent
func (c *Client) OnBeforeRequest(m func(request *Request) error) {
	c.resty.OnBeforeRequest(func(client *resty.Client, req *resty.Request) error {
		return m(req)
	})
}

// SetBaseURL sets the base URL of the Linode v4 API (https://api.linode.com/v4)
func (c *Client) SetBaseURL(baseURL string) *Client {
	baseURLPath, _ := url.Parse(baseURL)

	c.baseURL = path.Join(baseURLPath.Host, baseURLPath.Path)
	c.apiProto = baseURLPath.Scheme

	c.updateHostURL()

	return c
}

// SetAPIVersion sets the version of the API to interface with
func (c *Client) SetAPIVersion(apiVersion string) *Client {
	c.apiVersion = apiVersion

	c.updateHostURL()

	return c
}

func (c *Client) updateHostURL() {
	apiProto := APIProto
	baseURL := APIHost
	apiVersion := APIVersion

	if c.baseURL != "" {
		baseURL = c.baseURL
	}

	if c.apiVersion != "" {
		apiVersion = c.apiVersion
	}

	if c.apiProto != "" {
		apiProto = c.apiProto
	}

	c.resty.SetHostURL(fmt.Sprintf("%s://%s/%s", apiProto, baseURL, apiVersion))
}

// SetRootCertificate adds a root certificate to the underlying TLS client config
func (c *Client) SetRootCertificate(path string) *Client {
	c.resty.SetRootCertificate(path)
	return c
}

// SetToken sets the API token for all requests from this client
// Only necessary if you haven't already provided an http client to NewClient() configured with the token.
func (c *Client) SetToken(token string) *Client {
	c.resty.SetHeader("Authorization", fmt.Sprintf("Bearer %s", token))
	return c
}

// SetRetries adds retry conditions for "Linode Busy." errors and 429s.
func (c *Client) SetRetries() *Client {
	c.
		addRetryConditional(linodeBusyRetryCondition).
		addRetryConditional(tooManyRequestsRetryCondition).
		addRetryConditional(serviceUnavailableRetryCondition).
		addRetryConditional(requestTimeoutRetryCondition).
		addRetryConditional(requestGOAWAYRetryCondition).
		addRetryConditional(requestNGINXRetryCondition).
		SetRetryMaxWaitTime(APIRetryMaxWaitTime)
	configureRetries(c)
	return c
}

// AddRetryCondition adds a RetryConditional function to the Client
func (c *Client) AddRetryCondition(retryCondition RetryConditional) *Client {
	c.resty.AddRetryCondition(resty.RetryConditionFunc(retryCondition))
	return c
}

func (c *Client) addRetryConditional(retryConditional RetryConditional) *Client {
	c.retryConditionals = append(c.retryConditionals, retryConditional)
	return c
}

func (c *Client) addCachedResponse(endpoint string, response any, expiry *time.Duration) {
	if !c.shouldCache {
		return
	}

	responseValue := reflect.ValueOf(response)

	entry := clientCacheEntry{
		Created:        time.Now(),
		ExpiryOverride: expiry,
	}

	switch responseValue.Kind() {
	case reflect.Ptr:
		// We want to automatically deref pointers to
		// avoid caching mutable data.
		entry.Data = responseValue.Elem().Interface()
	default:
		entry.Data = response
	}

	c.cachedEntryLock.Lock()
	defer c.cachedEntryLock.Unlock()

	c.cachedEntries[endpoint] = entry
}

func (c *Client) getCachedResponse(endpoint string) any {
	if !c.shouldCache {
		return nil
	}

	c.cachedEntryLock.RLock()

	// Hacky logic to dynamically RUnlock
	// only if it is still locked by the
	// end of the function.
	// This is necessary as we take write
	// access if the entry has expired.
	rLocked := true
	defer func() {
		if rLocked {
			c.cachedEntryLock.RUnlock()
		}
	}()

	entry, ok := c.cachedEntries[endpoint]
	if !ok {
		return nil
	}

	// Handle expired entries
	elapsedTime := time.Since(entry.Created)

	hasExpired := elapsedTime > c.cacheExpiration
	if entry.ExpiryOverride != nil {
		hasExpired = elapsedTime > *entry.ExpiryOverride
	}

	if hasExpired {
		// We need to give up our read access and request read-write access
		c.cachedEntryLock.RUnlock()
		rLocked = false

		c.cachedEntryLock.Lock()
		defer c.cachedEntryLock.Unlock()

		delete(c.cachedEntries, endpoint)
		return nil
	}

	return c.cachedEntries[endpoint].Data
}

// InvalidateCache clears all cached responses for all endpoints.
func (c *Client) InvalidateCache() {
	c.cachedEntryLock.Lock()
	defer c.cachedEntryLock.Unlock()

	// GC will handle the old map
	c.cachedEntries = make(map[string]clientCacheEntry)
}

// InvalidateCacheEndpoint invalidates a single cached endpoint.
func (c *Client) InvalidateCacheEndpoint(endpoint string) error {
	u, err := url.Parse(endpoint)
	if err != nil {
		return fmt.Errorf("failed to parse URL for caching: %s", err)
	}

	c.cachedEntryLock.Lock()
	defer c.cachedEntryLock.Unlock()

	delete(c.cachedEntries, u.Path)

	return nil
}

// SetGlobalCacheExpiration sets the desired time for any cached response
// to be valid for.
func (c *Client) SetGlobalCacheExpiration(expiryTime time.Duration) {
	c.cacheExpiration = expiryTime
}

// UseCache sets whether response caching should be used
func (c *Client) UseCache(value bool) {
	c.shouldCache = value
}

// SetRetryMaxWaitTime sets the maximum delay before retrying a request.
func (c *Client) SetRetryMaxWaitTime(max time.Duration) *Client {
	c.resty.SetRetryMaxWaitTime(max)
	return c
}

// SetRetryWaitTime sets the default (minimum) delay before retrying a request.
func (c *Client) SetRetryWaitTime(min time.Duration) *Client {
	c.resty.SetRetryWaitTime(min)
	return c
}

// SetRetryAfter sets the callback function to be invoked with a failed request
// to determine wben it should be retried.
func (c *Client) SetRetryAfter(callback RetryAfter) *Client {
	c.resty.SetRetryAfter(resty.RetryAfterFunc(callback))
	return c
}

// SetRetryCount sets the maximum retry attempts before aborting.
func (c *Client) SetRetryCount(count int) *Client {
	c.resty.SetRetryCount(count)
	return c
}

// SetPollDelay sets the number of milliseconds to wait between events or status polls.
// Affects all WaitFor* functions and retries.
func (c *Client) SetPollDelay(delay time.Duration) *Client {
	c.millisecondsPerPoll = delay
	return c
}

// GetPollDelay gets the number of milliseconds to wait between events or status polls.
// Affects all WaitFor* functions and retries.
func (c *Client) GetPollDelay() time.Duration {
	return c.millisecondsPerPoll
}

// NewClient factory to create new Client struct
func NewClient(hc *http.Client) (client Client) {
	if hc != nil {
		client.resty = resty.NewWithClient(hc)
	} else {
		client.resty = resty.New()
	}

	client.shouldCache = true
	client.cacheExpiration = time.Minute * 15
	client.cachedEntries = make(map[string]clientCacheEntry)
	client.cachedEntryLock = &sync.RWMutex{}

	client.SetUserAgent(DefaultUserAgent)

	baseURL, baseURLExists := os.LookupEnv(APIHostVar)

	if baseURLExists {
		client.SetBaseURL(baseURL)
	}
	apiVersion, apiVersionExists := os.LookupEnv(APIVersionVar)
	if apiVersionExists {
		client.SetAPIVersion(apiVersion)
	} else {
		client.SetAPIVersion(APIVersion)
	}

	certPath, certPathExists := os.LookupEnv(APIHostCert)

	if certPathExists {
		cert, err := ioutil.ReadFile(filepath.Clean(certPath))
		if err != nil {
			log.Fatalf("[ERROR] Error when reading cert at %s: %s\n", certPath, err.Error())
		}

		client.SetRootCertificate(certPath)

		if envDebug {
			log.Printf("[DEBUG] Set API root certificate to %s with contents %s\n", certPath, cert)
		}
	}

	client.
		SetRetryWaitTime((1000 * APISecondsPerPoll) * time.Millisecond).
		SetPollDelay(1000 * APISecondsPerPoll).
		SetRetries().
		SetDebug(envDebug)

	return
}

// NewClientFromEnv creates a Client and initializes it with values
// from the LINODE_CONFIG file and the LINODE_TOKEN environment variable.
func NewClientFromEnv(hc *http.Client) (*Client, error) {
	client := NewClient(hc)

	// Users are expected to chain NewClient(...) and LoadConfig(...) to customize these options
	configPath, err := resolveValidConfigPath()
	if err != nil {
		return nil, err
	}

	// Populate the token from the environment.
	// Tokens should be first priority to maintain backwards compatibility
	if token, ok := os.LookupEnv(APIEnvVar); ok && token != "" {
		client.SetToken(token)
		return &client, nil
	}

	if p, ok := os.LookupEnv(APIConfigEnvVar); ok {
		configPath = p
	} else if !ok && configPath == "" {
		return nil, fmt.Errorf("no linode config file or token found")
	}

	configProfile := DefaultConfigProfile

	if p, ok := os.LookupEnv(APIConfigProfileEnvVar); ok {
		configProfile = p
	}

	client.selectedProfile = configProfile

	// We should only load the config if the config file exists
	if _, err := os.Stat(configPath); err != nil {
		return nil, fmt.Errorf("error loading config file %s: %s", configPath, err)
	}

	err = client.preLoadConfig(configPath)
	return &client, err
}

func (c *Client) preLoadConfig(configPath string) error {
	if envDebug {
		log.Printf("[INFO] Loading profile from %s\n", configPath)
	}

	if err := c.LoadConfig(&LoadConfigOptions{
		Path:            configPath,
		SkipLoadProfile: true,
	}); err != nil {
		return err
	}

	// We don't want to load the profile until the user is actually making requests
	c.OnBeforeRequest(func(request *Request) error {
		if c.loadedProfile != c.selectedProfile {
			if err := c.UseProfile(c.selectedProfile); err != nil {
				return err
			}
		}

		return nil
	})

	return nil
}

func copyBool(bPtr *bool) *bool {
	if bPtr == nil {
		return nil
	}

	t := *bPtr

	return &t
}

func copyInt(iPtr *int) *int {
	if iPtr == nil {
		return nil
	}

	t := *iPtr

	return &t
}

func copyString(sPtr *string) *string {
	if sPtr == nil {
		return nil
	}

	t := *sPtr

	return &t
}

func copyTime(tPtr *time.Time) *time.Time {
	if tPtr == nil {
		return nil
	}

	t := *tPtr

	return &t
}

func generateListCacheURL(endpoint string, opts *ListOptions) (string, error) {
	if opts == nil {
		return "", nil
	}

	hashedOpts, err := opts.Hash()
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%s:%s", endpoint, hashedOpts), nil
}
