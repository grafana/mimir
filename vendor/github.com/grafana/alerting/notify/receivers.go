package notify

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/prometheus/alertmanager/config"
	"github.com/prometheus/alertmanager/notify"
	"github.com/prometheus/alertmanager/types"
	"github.com/prometheus/common/model"
	"golang.org/x/sync/errgroup"

	"github.com/grafana/alerting/receivers"
	"github.com/grafana/alerting/receivers/alertmanager"
	"github.com/grafana/alerting/receivers/dinding"
	"github.com/grafana/alerting/receivers/discord"
	"github.com/grafana/alerting/receivers/email"
	"github.com/grafana/alerting/receivers/googlechat"
	"github.com/grafana/alerting/receivers/kafka"
	"github.com/grafana/alerting/receivers/line"
	"github.com/grafana/alerting/receivers/opsgenie"
	"github.com/grafana/alerting/receivers/pagerduty"
	"github.com/grafana/alerting/receivers/pushover"
	"github.com/grafana/alerting/receivers/sensugo"
	"github.com/grafana/alerting/receivers/slack"
	"github.com/grafana/alerting/receivers/teams"
	"github.com/grafana/alerting/receivers/telegram"
	"github.com/grafana/alerting/receivers/threema"
	"github.com/grafana/alerting/receivers/victorops"
	"github.com/grafana/alerting/receivers/webex"
	"github.com/grafana/alerting/receivers/webhook"
	"github.com/grafana/alerting/receivers/wecom"
	"github.com/grafana/alerting/templates"
)

const (
	maxTestReceiversWorkers = 10
)

var (
	ErrNoReceivers = errors.New("no receivers")
)

type TestReceiversResult struct {
	Alert     types.Alert
	Receivers []TestReceiverResult
	NotifedAt time.Time
}

type TestReceiverResult struct {
	Name    string
	Configs []TestIntegrationConfigResult
}

type TestIntegrationConfigResult struct {
	Name   string
	UID    string
	Status string
	Error  error
}

type GrafanaIntegrationConfig struct {
	UID                   string            `json:"uid"`
	Name                  string            `json:"name"`
	Type                  string            `json:"type"`
	DisableResolveMessage bool              `json:"disableResolveMessage"`
	Settings              json.RawMessage   `json:"settings"`
	SecureSettings        map[string]string `json:"secureSettings"`
}

type ConfigReceiver = config.Receiver

type APIReceiver struct {
	ConfigReceiver      `yaml:",inline"`
	GrafanaIntegrations `yaml:",inline"`
}

type GrafanaIntegrations struct {
	Integrations []*GrafanaIntegrationConfig `yaml:"grafana_managed_receiver_configs,omitempty" json:"grafana_managed_receiver_configs,omitempty"`
}

type TestReceiversConfigBodyParams struct {
	Alert     *TestReceiversConfigAlertParams `yaml:"alert,omitempty" json:"alert,omitempty"`
	Receivers []*APIReceiver                  `yaml:"receivers,omitempty" json:"receivers,omitempty"`
}

type TestReceiversConfigAlertParams struct {
	Annotations model.LabelSet `yaml:"annotations,omitempty" json:"annotations,omitempty"`
	Labels      model.LabelSet `yaml:"labels,omitempty" json:"labels,omitempty"`
}

type IntegrationTimeoutError struct {
	Integration *GrafanaIntegrationConfig
	Err         error
}

func (e IntegrationTimeoutError) Error() string {
	return fmt.Sprintf("the receiver timed out: %s", e.Err)
}

func (am *GrafanaAlertmanager) TestReceivers(ctx context.Context, c TestReceiversConfigBodyParams) (*TestReceiversResult, error) {
	// now represents the start time of the test
	now := time.Now()
	testAlert := newTestAlert(c, now, now)

	// we must set a group key that is unique per test as some receivers use this key to deduplicate alerts
	ctx = notify.WithGroupKey(ctx, testAlert.Labels.String()+now.String())

	tmpl, err := am.getTemplate()
	if err != nil {
		return nil, fmt.Errorf("failed to get template: %w", err)
	}

	// job contains all metadata required to test a receiver
	type job struct {
		Config       *GrafanaIntegrationConfig
		ReceiverName string
		Notifier     notify.Notifier
	}

	// result contains the receiver that was tested and an error that is non-nil if the test failed
	type result struct {
		Config       *GrafanaIntegrationConfig
		ReceiverName string
		Error        error
	}

	newTestReceiversResult := func(alert types.Alert, results []result, notifiedAt time.Time) *TestReceiversResult {
		m := make(map[string]TestReceiverResult)
		for _, receiver := range c.Receivers {
			// set up the result for this receiver
			m[receiver.Name] = TestReceiverResult{
				Name: receiver.Name,
				// A Grafana receiver can have multiple nested receivers
				Configs: make([]TestIntegrationConfigResult, 0, len(receiver.Integrations)),
			}
		}
		for _, next := range results {
			tmp := m[next.ReceiverName]
			status := "ok"
			if next.Error != nil {
				status = "failed"
			}
			tmp.Configs = append(tmp.Configs, TestIntegrationConfigResult{
				Name:   next.Config.Name,
				UID:    next.Config.UID,
				Status: status,
				Error:  ProcessIntegrationError(next.Config, next.Error),
			})
			m[next.ReceiverName] = tmp
		}
		v := new(TestReceiversResult)
		v.Alert = alert
		v.Receivers = make([]TestReceiverResult, 0, len(c.Receivers))
		v.NotifedAt = notifiedAt
		for _, next := range m {
			v.Receivers = append(v.Receivers, next)
		}

		// Make sure the return order is deterministic.
		sort.Slice(v.Receivers, func(i, j int) bool {
			return v.Receivers[i].Name < v.Receivers[j].Name
		})

		return v
	}

	// invalid keeps track of all invalid receiver configurations
	invalid := make([]result, 0, len(c.Receivers))
	// jobs keeps track of all receivers that need to be sent test notifications
	jobs := make([]job, 0, len(c.Receivers))

	for _, receiver := range c.Receivers {
		for _, next := range receiver.Integrations {
			n, err := am.buildSingleIntegration(next, tmpl)
			if err != nil {
				invalid = append(invalid, result{
					Config:       next,
					ReceiverName: next.Name,
					Error:        err,
				})
			} else {
				jobs = append(jobs, job{
					Config:       next,
					ReceiverName: receiver.Name,
					Notifier:     n,
				})
			}
		}
	}

	if len(invalid)+len(jobs) == 0 {
		return nil, ErrNoReceivers
	}

	if len(jobs) == 0 {
		return newTestReceiversResult(testAlert, invalid, now), nil
	}

	numWorkers := maxTestReceiversWorkers
	if numWorkers > len(jobs) {
		numWorkers = len(jobs)
	}

	resultCh := make(chan result, len(jobs))
	workCh := make(chan job, len(jobs))
	for _, job := range jobs {
		workCh <- job
	}
	close(workCh)

	g, ctx := errgroup.WithContext(ctx)
	for i := 0; i < numWorkers; i++ {
		g.Go(func() error {
			for next := range workCh {
				v := result{
					Config:       next.Config,
					ReceiverName: next.ReceiverName,
				}
				if _, err := next.Notifier.Notify(ctx, &testAlert); err != nil {
					v.Error = err
				}
				resultCh <- v
			}
			return nil
		})
	}
	err = g.Wait() // nolint
	close(resultCh)

	if err != nil {
		return nil, err
	}

	results := make([]result, 0, len(jobs))
	for next := range resultCh {
		results = append(results, next)
	}

	return newTestReceiversResult(testAlert, append(invalid, results...), now), nil
}

func (am *GrafanaAlertmanager) buildSingleIntegration(r *GrafanaIntegrationConfig, tmpl *templates.Template) (*Integration, error) {
	apiReceiver := &APIReceiver{
		GrafanaIntegrations: GrafanaIntegrations{
			Integrations: []*GrafanaIntegrationConfig{r},
		},
	}
	integrations, err := am.buildReceiverIntegrationsFunc(apiReceiver, tmpl)
	if err != nil {
		return nil, err
	}
	if len(integrations) == 0 {
		// This should not happen, but it is better to return some error rather than having a panic.
		return nil, errors.New("failed to build integration")
	}
	return integrations[0], nil
}

func newTestAlert(c TestReceiversConfigBodyParams, startsAt, updatedAt time.Time) types.Alert {
	var (
		defaultAnnotations = model.LabelSet{
			"summary":          "Notification test",
			"__value_string__": "[ metric='foo' labels={instance=bar} value=10 ]",
		}
		defaultLabels = model.LabelSet{
			"alertname": "TestAlert",
			"instance":  "Grafana",
		}
	)

	alert := types.Alert{
		Alert: model.Alert{
			Labels:      defaultLabels,
			Annotations: defaultAnnotations,
			StartsAt:    startsAt,
		},
		UpdatedAt: updatedAt,
	}

	if c.Alert != nil {
		if c.Alert.Annotations != nil {
			for k, v := range c.Alert.Annotations {
				alert.Annotations[k] = v
			}
		}
		if c.Alert.Labels != nil {
			for k, v := range c.Alert.Labels {
				alert.Labels[k] = v
			}
		}
	}

	return alert
}

func ProcessIntegrationError(config *GrafanaIntegrationConfig, err error) error {
	if err == nil {
		return nil
	}

	var urlError *url.Error
	if errors.As(err, &urlError) {
		if urlError.Timeout() {
			return IntegrationTimeoutError{
				Integration: config,
				Err:         err,
			}
		}
	}

	if errors.Is(err, context.DeadlineExceeded) {
		return IntegrationTimeoutError{
			Integration: config,
			Err:         err,
		}
	}

	return err
}

// GrafanaReceiverConfig represents a parsed and validated APIReceiver
type GrafanaReceiverConfig struct {
	Name                string
	AlertmanagerConfigs []*NotifierConfig[alertmanager.Config]
	DingdingConfigs     []*NotifierConfig[dinding.Config]
	DiscordConfigs      []*NotifierConfig[discord.Config]
	EmailConfigs        []*NotifierConfig[email.Config]
	GooglechatConfigs   []*NotifierConfig[googlechat.Config]
	KafkaConfigs        []*NotifierConfig[kafka.Config]
	LineConfigs         []*NotifierConfig[line.Config]
	OpsgenieConfigs     []*NotifierConfig[opsgenie.Config]
	PagerdutyConfigs    []*NotifierConfig[pagerduty.Config]
	PushoverConfigs     []*NotifierConfig[pushover.Config]
	SensugoConfigs      []*NotifierConfig[sensugo.Config]
	SlackConfigs        []*NotifierConfig[slack.Config]
	TeamsConfigs        []*NotifierConfig[teams.Config]
	TelegramConfigs     []*NotifierConfig[telegram.Config]
	ThreemaConfigs      []*NotifierConfig[threema.Config]
	VictoropsConfigs    []*NotifierConfig[victorops.Config]
	WebhookConfigs      []*NotifierConfig[webhook.Config]
	WecomConfigs        []*NotifierConfig[wecom.Config]
	WebexConfigs        []*NotifierConfig[webex.Config]
}

// NotifierConfig represents parsed GrafanaIntegrationConfig.
type NotifierConfig[T interface{}] struct {
	receivers.Metadata
	Settings T
}

// GetDecryptedValueFn is a function that returns the decrypted value of
// the given key. If the key is not present, then it returns the fallback value.
type GetDecryptedValueFn func(ctx context.Context, sjd map[string][]byte, key string, fallback string) string

// BuildReceiverConfiguration parses, decrypts and validates the APIReceiver.
func BuildReceiverConfiguration(ctx context.Context, api *APIReceiver, decrypt GetDecryptedValueFn) (GrafanaReceiverConfig, error) {
	result := GrafanaReceiverConfig{
		Name: api.Name,
	}
	for _, receiver := range api.Integrations {
		err := parseNotifier(ctx, &result, receiver, decrypt)
		if err != nil {
			return GrafanaReceiverConfig{}, IntegrationValidationError{
				Integration: receiver,
				Err:         err,
			}
		}
	}
	return result, nil
}

// parseNotifier parses receivers and populates the corresponding field in GrafanaReceiverConfig. Returns an error if the configuration cannot be parsed.
func parseNotifier(ctx context.Context, result *GrafanaReceiverConfig, receiver *GrafanaIntegrationConfig, decrypt GetDecryptedValueFn) error {
	secureSettings, err := decodeSecretsFromBase64(receiver.SecureSettings)
	if err != nil {
		return err
	}

	decryptFn := func(key string, fallback string) string {
		return decrypt(ctx, secureSettings, key, fallback)
	}

	switch strings.ToLower(receiver.Type) {
	case "prometheus-alertmanager":
		cfg, err := alertmanager.NewConfig(receiver.Settings, decryptFn)
		if err != nil {
			return err
		}
		result.AlertmanagerConfigs = append(result.AlertmanagerConfigs, newNotifierConfig(receiver, cfg))
	case "dingding":
		cfg, err := dinding.NewConfig(receiver.Settings)
		if err != nil {
			return err
		}
		result.DingdingConfigs = append(result.DingdingConfigs, newNotifierConfig(receiver, cfg))
	case "discord":
		fmt.Println("Discord settings:", string(receiver.Settings))
		cfg, err := discord.NewConfig(receiver.Settings, decryptFn)
		if err != nil {
			return err
		}
		spew.Dump("Discord config:", cfg)
		result.DiscordConfigs = append(result.DiscordConfigs, newNotifierConfig(receiver, cfg))
	case "email":
		cfg, err := email.NewConfig(receiver.Settings)
		if err != nil {
			return err
		}
		result.EmailConfigs = append(result.EmailConfigs, newNotifierConfig(receiver, cfg))
	case "googlechat":
		cfg, err := googlechat.NewConfig(receiver.Settings)
		if err != nil {
			return err
		}
		result.GooglechatConfigs = append(result.GooglechatConfigs, newNotifierConfig(receiver, cfg))
	case "kafka":
		cfg, err := kafka.NewConfig(receiver.Settings, decryptFn)
		if err != nil {
			return err
		}
		result.KafkaConfigs = append(result.KafkaConfigs, newNotifierConfig(receiver, cfg))
	case "line":
		cfg, err := line.NewConfig(receiver.Settings, decryptFn)
		if err != nil {
			return err
		}
		result.LineConfigs = append(result.LineConfigs, newNotifierConfig(receiver, cfg))
	case "opsgenie":
		cfg, err := opsgenie.NewConfig(receiver.Settings, decryptFn)
		if err != nil {
			return err
		}
		result.OpsgenieConfigs = append(result.OpsgenieConfigs, newNotifierConfig(receiver, cfg))
	case "pagerduty":
		cfg, err := pagerduty.NewConfig(receiver.Settings, decryptFn)
		if err != nil {
			return err
		}
		result.PagerdutyConfigs = append(result.PagerdutyConfigs, newNotifierConfig(receiver, cfg))
	case "pushover":
		cfg, err := pushover.NewConfig(receiver.Settings, decryptFn)
		if err != nil {
			return err
		}
		result.PushoverConfigs = append(result.PushoverConfigs, newNotifierConfig(receiver, cfg))
	case "sensugo":
		cfg, err := sensugo.NewConfig(receiver.Settings, decryptFn)
		if err != nil {
			return err
		}
		result.SensugoConfigs = append(result.SensugoConfigs, newNotifierConfig(receiver, cfg))
	case "slack":
		cfg, err := slack.NewConfig(receiver.Settings, decryptFn)
		if err != nil {
			return err
		}
		result.SlackConfigs = append(result.SlackConfigs, newNotifierConfig(receiver, cfg))
	case "teams":
		cfg, err := teams.NewConfig(receiver.Settings)
		if err != nil {
			return err
		}
		result.TeamsConfigs = append(result.TeamsConfigs, newNotifierConfig(receiver, cfg))
	case "telegram":
		cfg, err := telegram.NewConfig(receiver.Settings, decryptFn)
		if err != nil {
			return err
		}
		result.TelegramConfigs = append(result.TelegramConfigs, newNotifierConfig(receiver, cfg))
	case "threema":
		cfg, err := threema.NewConfig(receiver.Settings, decryptFn)
		if err != nil {
			return err
		}
		result.ThreemaConfigs = append(result.ThreemaConfigs, newNotifierConfig(receiver, cfg))
	case "victorops":
		cfg, err := victorops.NewConfig(receiver.Settings)
		if err != nil {
			return err
		}
		result.VictoropsConfigs = append(result.VictoropsConfigs, newNotifierConfig(receiver, cfg))
	case "webhook":
		cfg, err := webhook.NewConfig(receiver.Settings, decryptFn)
		if err != nil {
			return err
		}
		result.WebhookConfigs = append(result.WebhookConfigs, newNotifierConfig(receiver, cfg))
	case "wecom":
		cfg, err := wecom.NewConfig(receiver.Settings, decryptFn)
		if err != nil {
			return err
		}
		result.WecomConfigs = append(result.WecomConfigs, newNotifierConfig(receiver, cfg))
	case "webex":
		cfg, err := webex.NewConfig(receiver.Settings, decryptFn)
		if err != nil {
			return err
		}
		result.WebexConfigs = append(result.WebexConfigs, newNotifierConfig(receiver, cfg))
	default:
		return fmt.Errorf("notifier %s is not supported", receiver.Type)
	}
	return nil
}

func decodeSecretsFromBase64(secrets map[string]string) (map[string][]byte, error) {
	secureSettings := make(map[string][]byte, len(secrets))
	if secrets == nil {
		return secureSettings, nil
	}
	for k, v := range secrets {
		d, err := base64.StdEncoding.DecodeString(v)
		if err != nil {
			return nil, fmt.Errorf("failed to decode secure settings key %s: %w", k, err)
		}
		secureSettings[k] = d
	}
	return secureSettings, nil
}

func newNotifierConfig[T interface{}](receiver *GrafanaIntegrationConfig, settings T) *NotifierConfig[T] {
	return &NotifierConfig[T]{
		Metadata: receivers.Metadata{
			UID:                   receiver.UID,
			Name:                  receiver.Name,
			Type:                  receiver.Type,
			DisableResolveMessage: receiver.DisableResolveMessage,
		},
		Settings: settings,
	}
}

type IntegrationValidationError struct {
	Err         error
	Integration *GrafanaIntegrationConfig
}

func (e IntegrationValidationError) Error() string {
	name := ""
	if e.Integration.Name != "" {
		name = fmt.Sprintf("%q ", e.Integration.Name)
	}
	s := fmt.Sprintf("failed to validate integration %s(UID %s) of type %q: %s", name, e.Integration.UID, e.Integration.Type, e.Err.Error())
	return s
}

func (e IntegrationValidationError) Unwrap() error { return e.Err }
