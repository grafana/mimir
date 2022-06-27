// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/integration/e2ecortex/client.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package e2emimir

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	alertConfig "github.com/prometheus/alertmanager/config"
	"github.com/prometheus/alertmanager/types"
	promapi "github.com/prometheus/client_golang/api"
	promv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/rulefmt"
	"github.com/prometheus/prometheus/prompb"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric" // OTLP protos are not compatible with gogo
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	yaml "gopkg.in/yaml.v3"

	"github.com/grafana/mimir/pkg/ruler"
)

var ErrNotFound = errors.New("not found")

// Client is a client used to interact with Mimir in integration tests
type Client struct {
	alertmanagerClient  promapi.Client
	querierAddress      string
	alertmanagerAddress string
	rulerAddress        string
	distributorAddress  string
	timeout             time.Duration
	httpClient          *http.Client
	querierClient       promv1.API
	orgID               string
}

// NewClient makes a new Mimir client
func NewClient(
	distributorAddress string,
	querierAddress string,
	alertmanagerAddress string,
	rulerAddress string,
	orgID string,
) (*Client, error) {
	// Disable compression in querier client so it's easier to debug issue looking at the HTTP responses
	// logged by the querier.
	querierTransport := http.DefaultTransport.(*http.Transport).Clone()
	querierTransport.DisableCompression = true

	// Create querier API client
	querierAPIClient, err := promapi.NewClient(promapi.Config{
		Address:      "http://" + querierAddress + "/prometheus",
		RoundTripper: &addOrgIDRoundTripper{orgID: orgID, next: querierTransport},
	})
	if err != nil {
		return nil, err
	}

	c := &Client{
		distributorAddress:  distributorAddress,
		querierAddress:      querierAddress,
		alertmanagerAddress: alertmanagerAddress,
		rulerAddress:        rulerAddress,
		timeout:             5 * time.Second,
		httpClient:          &http.Client{},
		querierClient:       promv1.NewAPI(querierAPIClient),
		orgID:               orgID,
	}

	if alertmanagerAddress != "" {
		alertmanagerAPIClient, err := promapi.NewClient(promapi.Config{
			Address:      "http://" + alertmanagerAddress,
			RoundTripper: &addOrgIDRoundTripper{orgID: orgID, next: http.DefaultTransport},
		})
		if err != nil {
			return nil, err
		}
		c.alertmanagerClient = alertmanagerAPIClient
	}

	return c, nil
}

// Push the input timeseries to the remote endpoint
func (c *Client) Push(timeseries []prompb.TimeSeries) (*http.Response, error) {
	// Create write request
	data, err := proto.Marshal(&prompb.WriteRequest{Timeseries: timeseries})
	if err != nil {
		return nil, err
	}

	// Create HTTP request
	compressed := snappy.Encode(nil, data)
	req, err := http.NewRequest("POST", fmt.Sprintf("http://%s/api/v1/push", c.distributorAddress), bytes.NewReader(compressed))
	if err != nil {
		return nil, err
	}

	req.Header.Add("Content-Encoding", "snappy")
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("X-Prometheus-Remote-Write-Version", "0.1.0")
	req.Header.Set("X-Scope-OrgID", c.orgID)

	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	// Execute HTTP request
	res, err := c.httpClient.Do(req.WithContext(ctx))
	if err != nil {
		return nil, err
	}

	defer res.Body.Close()
	return res, nil
}

// OTLPPush the input timeseries to the remote endpoint in OTLP format
func (c *Client) OTLPPush(timeseries []prompb.TimeSeries) (*http.Response, error) {
	// Create write request
	otlpRequest := TimeseriesToOTLPRequest(timeseries)

	data, err := otlpRequest.MarshalProto()
	if err != nil {
		return nil, err
	}

	// Create HTTP request
	req, err := http.NewRequest("POST", fmt.Sprintf("http://%s/api/v1/push/otlp/v1/metrics", c.distributorAddress), bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("X-Scope-OrgID", c.orgID)

	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	// Execute HTTP request
	res, err := c.httpClient.Do(req.WithContext(ctx))
	if err != nil {
		return nil, err
	}

	defer res.Body.Close()
	return res, nil
}

// Query runs an instant query.
func (c *Client) Query(query string, ts time.Time) (model.Value, error) {
	value, _, err := c.querierClient.Query(context.Background(), query, ts)
	return value, err
}

// Query runs a query range.
func (c *Client) QueryRange(query string, start, end time.Time, step time.Duration) (model.Value, error) {
	value, _, err := c.querierClient.QueryRange(context.Background(), query, promv1.Range{
		Start: start,
		End:   end,
		Step:  step,
	})
	return value, err
}

// QueryRangeRaw runs a ranged query directly against the querier API.
func (c *Client) QueryRangeRaw(query string, start, end time.Time, step time.Duration) (*http.Response, []byte, error) {
	addr := fmt.Sprintf(
		"http://%s/prometheus/api/v1/query_range?query=%s&start=%s&end=%s&step=%s",
		c.querierAddress,
		url.QueryEscape(query),
		FormatTime(start),
		FormatTime(end),
		strconv.FormatFloat(step.Seconds(), 'f', -1, 64),
	)

	return c.DoGetBody(addr)
}

// QueryExemplars runs an exemplar query.
func (c *Client) QueryExemplars(query string, start, end time.Time) ([]promv1.ExemplarQueryResult, error) {
	return c.querierClient.QueryExemplars(context.Background(), query, start, end)
}

// QuerierAddress returns the address of the querier
func (c *Client) QuerierAddress() string {
	return c.querierAddress
}

// QueryRaw runs a query directly against the querier API.
func (c *Client) QueryRaw(query string) (*http.Response, []byte, error) {
	addr := fmt.Sprintf("http://%s/prometheus/api/v1/query?query=%s", c.querierAddress, url.QueryEscape(query))

	return c.DoGetBody(addr)
}

// Series finds series by label matchers.
func (c *Client) Series(matches []string, start, end time.Time) ([]model.LabelSet, error) {
	result, _, err := c.querierClient.Series(context.Background(), matches, start, end)
	return result, err
}

// LabelValues gets label values
func (c *Client) LabelValues(label string, start, end time.Time, matches []string) (model.LabelValues, error) {
	result, _, err := c.querierClient.LabelValues(context.Background(), label, matches, start, end)
	return result, err
}

// LabelNames gets label names
func (c *Client) LabelNames(start, end time.Time) ([]string, error) {
	result, _, err := c.querierClient.LabelNames(context.Background(), nil, start, end)
	return result, err
}

// LabelNamesAndValues returns distinct label values per label name.
func (c *Client) LabelNamesAndValues(selector string, limit int) (*http.Response, error) {
	body := make(url.Values)
	if len(selector) > 0 {
		body.Set("selector", selector)
	}
	if limit > 0 {
		body.Set("limit", strconv.Itoa(limit))
	}

	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("http://%s/prometheus/api/v1/cardinality/label_names", c.querierAddress), strings.NewReader(body.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Scope-OrgID", c.orgID)
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Add("Content-Length", strconv.Itoa(len(body.Encode())))

	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	// Execute HTTP request
	return c.httpClient.Do(req.WithContext(ctx))
}

// LabelValuesCardinality returns all values and series total count for each label name.
func (c *Client) LabelValuesCardinality(labelNames []string, selector string, limit int) (*http.Response, error) {
	body := make(url.Values)
	if len(selector) > 0 {
		body.Set("selector", selector)
	}
	if limit > 0 {
		body.Set("limit", strconv.Itoa(limit))
	}
	for _, lbName := range labelNames {
		body.Add("label_names[]", lbName)
	}

	req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("http://%s/prometheus/api/v1/cardinality/label_values", c.querierAddress), strings.NewReader(body.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Scope-OrgID", c.orgID)
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Add("Content-Length", strconv.Itoa(len(body.Encode())))

	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	// Execute HTTP request
	return c.httpClient.Do(req.WithContext(ctx))
}

type addOrgIDRoundTripper struct {
	orgID string
	next  http.RoundTripper
}

func (r *addOrgIDRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	req.Header.Set("X-Scope-OrgID", r.orgID)

	return r.next.RoundTrip(req)
}

// ServerStatus represents a Alertmanager status response
// TODO: Upgrade to Alertmanager v0.20.0+ and utilize vendored structs
type ServerStatus struct {
	Data struct {
		ConfigYaml string `json:"configYAML"`
	} `json:"data"`
}

// GetPrometheusRules fetches the rules from the Prometheus endpoint /api/v1/rules.
func (c *Client) GetPrometheusRules() ([]*ruler.RuleGroup, error) {
	// Create HTTP request
	req, err := http.NewRequest("GET", fmt.Sprintf("http://%s/prometheus/api/v1/rules", c.rulerAddress), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Scope-OrgID", c.orgID)

	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	// Execute HTTP request
	res, err := c.httpClient.Do(req.WithContext(ctx))
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	// Decode the response.
	type response struct {
		Status string              `json:"status"`
		Data   ruler.RuleDiscovery `json:"data"`
	}

	decoded := &response{}
	if err := json.Unmarshal(body, decoded); err != nil {
		return nil, err
	}

	if decoded.Status != "success" {
		return nil, fmt.Errorf("unexpected response status '%s'", decoded.Status)
	}

	return decoded.Data.RuleGroups, nil
}

// GetRuleGroups gets the configured rule groups from the ruler.
func (c *Client) GetRuleGroups() (map[string][]rulefmt.RuleGroup, error) {
	// Create HTTP request
	req, err := http.NewRequest("GET", fmt.Sprintf("http://%s/prometheus/config/v1/rules", c.rulerAddress), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Scope-OrgID", c.orgID)

	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	// Execute HTTP request
	res, err := c.httpClient.Do(req.WithContext(ctx))
	if err != nil {
		return nil, err
	}

	defer res.Body.Close()
	rgs := map[string][]rulefmt.RuleGroup{}

	data, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	err = yaml.Unmarshal(data, rgs)
	if err != nil {
		return nil, err
	}

	return rgs, nil
}

// SetRuleGroup configures the provided rulegroup to the ruler.
func (c *Client) SetRuleGroup(rulegroup rulefmt.RuleGroup, namespace string) error {
	// Create write request
	data, err := yaml.Marshal(rulegroup)
	if err != nil {
		return err
	}

	// Create HTTP request
	req, err := http.NewRequest("POST", fmt.Sprintf("http://%s/prometheus/config/v1/rules/%s", c.rulerAddress, url.PathEscape(namespace)), bytes.NewReader(data))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/yaml")
	req.Header.Set("X-Scope-OrgID", c.orgID)

	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	// Execute HTTP request
	res, err := c.httpClient.Do(req.WithContext(ctx))
	if err != nil {
		return err
	}

	defer res.Body.Close()

	if res.StatusCode != 202 {
		return fmt.Errorf("unexpected status code: %d", res.StatusCode)
	}

	return nil
}

// GetRuleGroup gets a rule group.
func (c *Client) GetRuleGroup(namespace string, groupName string) (*http.Response, error) {
	// Create HTTP request
	req, err := http.NewRequest("GET", fmt.Sprintf("http://%s/prometheus/config/v1/rules/%s/%s", c.rulerAddress, url.PathEscape(namespace), url.PathEscape(groupName)), nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/yaml")
	req.Header.Set("X-Scope-OrgID", c.orgID)

	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	// Execute HTTP request
	return c.httpClient.Do(req.WithContext(ctx))
}

// DeleteRuleGroup deletes a rule group.
func (c *Client) DeleteRuleGroup(namespace string, groupName string) error {
	// Create HTTP request
	req, err := http.NewRequest("DELETE", fmt.Sprintf("http://%s/prometheus/config/v1/rules/%s/%s", c.rulerAddress, url.PathEscape(namespace), url.PathEscape(groupName)), nil)
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/yaml")
	req.Header.Set("X-Scope-OrgID", c.orgID)

	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	// Execute HTTP request
	res, err := c.httpClient.Do(req.WithContext(ctx))
	if err != nil {
		return err
	}

	defer res.Body.Close()
	return nil
}

// DeleteRuleNamespace deletes all the rule groups (and the namespace itself).
func (c *Client) DeleteRuleNamespace(namespace string) error {
	// Create HTTP request
	req, err := http.NewRequest("DELETE", fmt.Sprintf("http://%s/prometheus/config/v1/rules/%s", c.rulerAddress, url.PathEscape(namespace)), nil)
	if err != nil {
		return err
	}

	req.Header.Set("X-Scope-OrgID", c.orgID)

	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	// Execute HTTP request
	_, err = c.httpClient.Do(req.WithContext(ctx))
	if err != nil {
		return err
	}

	return nil
}

// userConfig is used to communicate a users alertmanager configs
type userConfig struct {
	TemplateFiles      map[string]string `yaml:"template_files"`
	AlertmanagerConfig string            `yaml:"alertmanager_config"`
}

// GetAlertmanagerStatusPage gets the status page of alertmanager.
func (c *Client) GetAlertmanagerStatusPage(ctx context.Context) ([]byte, error) {
	return c.getRawPage(ctx, "http://"+c.alertmanagerAddress+"/multitenant_alertmanager/status")
}

func (c *Client) getRawPage(ctx context.Context, url string) ([]byte, error) {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.httpClient.Do(req.WithContext(ctx))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	content, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("fetching page failed with status %d and content %v", resp.StatusCode, string(content))
	}
	return content, nil
}

// GetAlertmanager performs a GET request on a per-tenant alertmanager endpoint.
func (c *Client) GetAlertmanager(ctx context.Context, path string) (string, error) {
	return c.doAlertmanagerRequest(ctx, http.MethodGet, path)
}

// PostAlertmanager performs a POST request on a per-tenant alertmanager endpoint.
func (c *Client) PostAlertmanager(ctx context.Context, path string) error {
	_, err := c.doAlertmanagerRequest(ctx, http.MethodPost, path)
	return err
}

func (c *Client) doAlertmanagerRequest(ctx context.Context, method string, path string) (string, error) {
	u := c.alertmanagerClient.URL(fmt.Sprintf("alertmanager/%s", path), nil)

	req, err := http.NewRequest(method, u.String(), nil)
	if err != nil {
		return "", fmt.Errorf("error creating request: %v", err)
	}

	resp, body, err := c.alertmanagerClient.Do(ctx, req)
	if err != nil {
		return "", err
	}

	if resp.StatusCode == http.StatusNotFound {
		return "", ErrNotFound
	}

	if resp.StatusCode/100 != 2 {
		return "", fmt.Errorf("getting %s failed with status %d and error %v", path, resp.StatusCode, string(body))
	}
	return string(body), nil
}

// GetAlertmanagerConfig gets the status of an alertmanager instance
func (c *Client) GetAlertmanagerConfig(ctx context.Context) (*alertConfig.Config, error) {
	u := c.alertmanagerClient.URL("/alertmanager/api/v1/status", nil)

	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request: %v", err)
	}

	resp, body, err := c.alertmanagerClient.Do(ctx, req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrNotFound
	}

	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("getting config failed with status %d and error %v", resp.StatusCode, string(body))
	}

	var ss *ServerStatus
	err = json.Unmarshal(body, &ss)
	if err != nil {
		return nil, err
	}

	cfg := &alertConfig.Config{}
	err = yaml.Unmarshal([]byte(ss.Data.ConfigYaml), cfg)

	return cfg, err
}

// SetAlertmanagerConfig gets the status of an alertmanager instance
func (c *Client) SetAlertmanagerConfig(ctx context.Context, amConfig string, templates map[string]string) error {
	u := c.alertmanagerClient.URL("/api/v1/alerts", nil)

	data, err := yaml.Marshal(&userConfig{
		AlertmanagerConfig: amConfig,
		TemplateFiles:      templates,
	})
	if err != nil {
		return err
	}

	req, err := http.NewRequest(http.MethodPost, u.String(), bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("error creating request: %v", err)
	}

	resp, body, err := c.alertmanagerClient.Do(ctx, req)
	if err != nil {
		return err
	}

	if resp.StatusCode == http.StatusNotFound {
		return ErrNotFound
	}

	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("setting config failed with status %d and error %v", resp.StatusCode, string(body))
	}

	return nil
}

// DeleteAlertmanagerConfig gets the status of an alertmanager instance
func (c *Client) DeleteAlertmanagerConfig(ctx context.Context) error {
	u := c.alertmanagerClient.URL("/api/v1/alerts", nil)
	req, err := http.NewRequest(http.MethodDelete, u.String(), nil)
	if err != nil {
		return fmt.Errorf("error creating request: %v", err)
	}

	resp, body, err := c.alertmanagerClient.Do(ctx, req)
	if err != nil {
		return err
	}

	if resp.StatusCode == http.StatusNotFound {
		return ErrNotFound
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("deleting config failed with status %d and error %v", resp.StatusCode, string(body))
	}

	return nil
}

// SendAlertToAlermanager sends alerts to the Alertmanager API
func (c *Client) SendAlertToAlermanager(ctx context.Context, alert *model.Alert) error {
	u := c.alertmanagerClient.URL("/alertmanager/api/v1/alerts", nil)

	data, err := json.Marshal([]types.Alert{{Alert: *alert}})
	if err != nil {
		return fmt.Errorf("error marshaling the alert: %v", err)
	}

	req, err := http.NewRequest(http.MethodPost, u.String(), bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("error creating request: %v", err)
	}

	resp, body, err := c.alertmanagerClient.Do(ctx, req)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("sending alert failed with status %d and error %v", resp.StatusCode, string(body))
	}

	return nil
}

func (c *Client) GetAlertsV1(ctx context.Context) ([]model.Alert, error) {
	u := c.alertmanagerClient.URL("alertmanager/api/v1/alerts", nil)

	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request: %v", err)
	}

	resp, body, err := c.alertmanagerClient.Do(ctx, req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrNotFound
	}

	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("getting alerts failed with status %d and error %v", resp.StatusCode, string(body))
	}

	type response struct {
		Status string        `json:"status"`
		Data   []model.Alert `json:"data"`
	}

	decoded := &response{}
	if err := json.Unmarshal(body, decoded); err != nil {
		return nil, err
	}

	if decoded.Status != "success" {
		return nil, fmt.Errorf("unexpected response status '%s'", decoded.Status)
	}

	return decoded.Data, nil
}

func (c *Client) GetAlertsV2(ctx context.Context) ([]model.Alert, error) {
	u := c.alertmanagerClient.URL("alertmanager/api/v2/alerts", nil)

	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request: %v", err)
	}

	resp, body, err := c.alertmanagerClient.Do(ctx, req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrNotFound
	}

	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("getting alerts failed with status %d and error %v", resp.StatusCode, string(body))
	}

	decoded := []model.Alert{}
	if err := json.Unmarshal(body, &decoded); err != nil {
		return nil, err
	}

	return decoded, nil
}

type AlertGroup struct {
	Labels model.LabelSet `json:"labels"`
	Alerts []model.Alert  `json:"alerts"`
}

func (c *Client) GetAlertGroups(ctx context.Context) ([]AlertGroup, error) {
	u := c.alertmanagerClient.URL("alertmanager/api/v2/alerts/groups", nil)

	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request: %v", err)
	}

	resp, body, err := c.alertmanagerClient.Do(ctx, req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrNotFound
	}

	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("getting alert groups failed with status %d and error %v", resp.StatusCode, string(body))
	}

	decoded := []AlertGroup{}
	if err := json.Unmarshal(body, &decoded); err != nil {
		return nil, err
	}
	return decoded, nil
}

// CreateSilence creates a new silence and returns the unique identifier of the silence.
func (c *Client) CreateSilence(ctx context.Context, silence types.Silence) (string, error) {
	u := c.alertmanagerClient.URL("alertmanager/api/v1/silences", nil)

	data, err := json.Marshal(silence)
	if err != nil {
		return "", fmt.Errorf("error marshaling the silence: %s", err)
	}

	req, err := http.NewRequest(http.MethodPost, u.String(), bytes.NewReader(data))
	if err != nil {
		return "", fmt.Errorf("error creating request: %v", err)
	}

	resp, body, err := c.alertmanagerClient.Do(ctx, req)
	if err != nil {
		return "", err
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("creating the silence failed with status %d and error %v", resp.StatusCode, string(body))
	}

	type response struct {
		Status string `json:"status"`
		Data   struct {
			SilenceID string `json:"silenceID"`
		} `json:"data"`
	}

	decoded := &response{}
	if err := json.Unmarshal(body, decoded); err != nil {
		return "", err
	}

	if decoded.Status != "success" {
		return "", fmt.Errorf("unexpected response status '%s'", decoded.Status)
	}

	return decoded.Data.SilenceID, nil
}

func (c *Client) GetSilencesV1(ctx context.Context) ([]types.Silence, error) {
	u := c.alertmanagerClient.URL("alertmanager/api/v1/silences", nil)

	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request: %v", err)
	}

	resp, body, err := c.alertmanagerClient.Do(ctx, req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrNotFound
	}

	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("getting silences failed with status %d and error %v", resp.StatusCode, string(body))
	}

	type response struct {
		Status string          `json:"status"`
		Data   []types.Silence `json:"data"`
	}

	decoded := &response{}
	if err := json.Unmarshal(body, decoded); err != nil {
		return nil, err
	}

	if decoded.Status != "success" {
		return nil, fmt.Errorf("unexpected response status '%s'", decoded.Status)
	}

	return decoded.Data, nil
}

func (c *Client) GetSilencesV2(ctx context.Context) ([]types.Silence, error) {
	u := c.alertmanagerClient.URL("alertmanager/api/v2/silences", nil)

	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request: %v", err)
	}

	resp, body, err := c.alertmanagerClient.Do(ctx, req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrNotFound
	}

	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("getting silences failed with status %d and error %v", resp.StatusCode, string(body))
	}

	decoded := []types.Silence{}
	if err := json.Unmarshal(body, &decoded); err != nil {
		return nil, err
	}

	return decoded, nil
}

func (c *Client) GetSilenceV1(ctx context.Context, id string) (types.Silence, error) {
	u := c.alertmanagerClient.URL(fmt.Sprintf("alertmanager/api/v1/silence/%s", url.PathEscape(id)), nil)

	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		return types.Silence{}, fmt.Errorf("error creating request: %v", err)
	}

	resp, body, err := c.alertmanagerClient.Do(ctx, req)
	if err != nil {
		return types.Silence{}, err
	}

	if resp.StatusCode == http.StatusNotFound {
		return types.Silence{}, ErrNotFound
	}

	if resp.StatusCode/100 != 2 {
		return types.Silence{}, fmt.Errorf("getting silence failed with status %d and error %v", resp.StatusCode, string(body))
	}

	type response struct {
		Status string        `json:"status"`
		Data   types.Silence `json:"data"`
	}

	decoded := &response{}
	if err := json.Unmarshal(body, decoded); err != nil {
		return types.Silence{}, err
	}

	if decoded.Status != "success" {
		return types.Silence{}, fmt.Errorf("unexpected response status '%s'", decoded.Status)
	}

	return decoded.Data, nil
}

func (c *Client) GetSilenceV2(ctx context.Context, id string) (types.Silence, error) {
	u := c.alertmanagerClient.URL(fmt.Sprintf("alertmanager/api/v2/silence/%s", url.PathEscape(id)), nil)

	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		return types.Silence{}, fmt.Errorf("error creating request: %v", err)
	}

	resp, body, err := c.alertmanagerClient.Do(ctx, req)
	if err != nil {
		return types.Silence{}, err
	}

	if resp.StatusCode == http.StatusNotFound {
		return types.Silence{}, ErrNotFound
	}

	if resp.StatusCode/100 != 2 {
		return types.Silence{}, fmt.Errorf("getting silence failed with status %d and error %v", resp.StatusCode, string(body))
	}

	decoded := types.Silence{}
	if err := json.Unmarshal(body, &decoded); err != nil {
		return types.Silence{}, err
	}

	return decoded, nil
}

func (c *Client) DeleteSilence(ctx context.Context, id string) error {
	u := c.alertmanagerClient.URL(fmt.Sprintf("alertmanager/api/v1/silence/%s", url.PathEscape(id)), nil)

	req, err := http.NewRequest(http.MethodDelete, u.String(), nil)
	if err != nil {
		return fmt.Errorf("error creating request: %v", err)
	}

	resp, body, err := c.alertmanagerClient.Do(ctx, req)
	if err != nil {
		return err
	}

	if resp.StatusCode == http.StatusNotFound {
		return ErrNotFound
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("deleting silence failed with status %d and error %v", resp.StatusCode, string(body))
	}

	return nil
}

func (c *Client) GetReceivers(ctx context.Context) ([]string, error) {
	u := c.alertmanagerClient.URL("alertmanager/api/v1/receivers", nil)

	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request: %v", err)
	}

	resp, body, err := c.alertmanagerClient.Do(ctx, req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrNotFound
	}

	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("getting receivers failed with status %d and error %v", resp.StatusCode, string(body))
	}

	type response struct {
		Status string   `json:"status"`
		Data   []string `json:"data"`
	}

	decoded := &response{}
	if err := json.Unmarshal(body, decoded); err != nil {
		return nil, err
	}

	if decoded.Status != "success" {
		return nil, fmt.Errorf("unexpected response status '%s'", decoded.Status)
	}

	return decoded.Data, nil
}

// DoGet performs a HTTP GET request towards the supplied URL. The request
// contains the X-Scope-OrgID header and the timeout configured by the client
// object.
func (c *Client) DoGet(url string) (*http.Response, error) {
	return c.doRequest("GET", url, nil)
}

// DoGetBody performs a HTTP GET request towards the supplied URL and returns
// the full response body. The request contains the X-Scope-OrgID header and
// the timeout configured by the client object.
func (c *Client) DoGetBody(url string) (*http.Response, []byte, error) {
	resp, err := c.DoGet(url)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, err
	}

	return resp, body, nil
}

// DoPost performs a HTTP POST request towards the supplied URL and using the
// given request body. The request contains the X-Scope-OrgID header and the
// timeout configured by the client object.
func (c *Client) DoPost(url string, body io.Reader) (*http.Response, error) {
	return c.doRequest("POST", url, body)
}

func (c *Client) doRequest(method, url string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Scope-OrgID", c.orgID)

	client := *c.httpClient
	client.Timeout = c.timeout

	return client.Do(req)
}

// FormatTime converts a time to a string acceptable by the Prometheus API.
func FormatTime(t time.Time) string {
	return strconv.FormatFloat(float64(t.Unix())+float64(t.Nanosecond())/1e9, 'f', -1, 64)
}

func TimeseriesToOTLPRequest(timeseries []prompb.TimeSeries) pmetricotlp.Request {
	d := pmetric.NewMetrics()

	for _, ts := range timeseries {
		name := ""
		attributes := pcommon.NewMap()

		for _, l := range ts.Labels {
			if l.Name == "__name__" {
				name = l.Value
				continue
			}

			attributes.InsertString(l.Name, l.Value)
		}

		metric := d.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
		metric.SetName(name)
		metric.SetDataType(pmetric.MetricDataTypeGauge)

		for _, sample := range ts.Samples {
			datapoint := metric.Gauge().DataPoints().AppendEmpty()
			datapoint.SetTimestamp(pcommon.NewTimestampFromTime(time.Unix(0, int64(sample.Timestamp)*1000000)))
			datapoint.SetDoubleVal(sample.Value)
			attributes.CopyTo(datapoint.Attributes())
		}
	}

	return pmetricotlp.NewRequestFromMetrics(d)
}
