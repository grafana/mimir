// copy of https://github.com/grafana/mimir/blob/a5f6bc75e858f2f7ede4b68bd692ed9b4f99193d/pkg/alertmanager/api.go

package definition

import (
	"errors"
	"reflect"

	"github.com/prometheus/alertmanager/config"
	commoncfg "github.com/prometheus/common/config"

	httpcfg "github.com/grafana/alerting/http/v0mimir1"
	discord_v0mimir1 "github.com/grafana/alerting/receivers/discord/v0mimir1"
	email_v0mimir1 "github.com/grafana/alerting/receivers/email/v0mimir1"
	opsgenie_v0mimir1 "github.com/grafana/alerting/receivers/opsgenie/v0mimir1"
	pagerduty_v0mimir1 "github.com/grafana/alerting/receivers/pagerduty/v0mimir1"
	pushover_v0mimir1 "github.com/grafana/alerting/receivers/pushover/v0mimir1"
	slack_v0mimir1 "github.com/grafana/alerting/receivers/slack/v0mimir1"
	teams_v0mimir1 "github.com/grafana/alerting/receivers/teams/v0mimir1"
	teams_v0mimir2 "github.com/grafana/alerting/receivers/teams/v0mimir2"
	telegram_v0mimir1 "github.com/grafana/alerting/receivers/telegram/v0mimir1"
	victorops_v0mimir1 "github.com/grafana/alerting/receivers/victorops/v0mimir1"
	webhook_v0mimir1 "github.com/grafana/alerting/receivers/webhook/v0mimir1"
)

var (
	errPasswordFileNotAllowed            = errors.New("setting smtp_auth_password_file, password_file, bearer_token_file, auth_password_file or credentials_file is not allowed")
	errOAuth2SecretFileNotAllowed        = errors.New("setting OAuth2 client_secret_file is not allowed")
	errProxyURLNotAllowed                = errors.New("setting proxy_url is not allowed")
	errProxyFromEnvironmentURLNotAllowed = errors.New("setting proxy_from_environment is not allowed")
	errTLSConfigNotAllowed               = errors.New("setting TLS ca_file, cert_file, key_file, ca, cert or key is not allowed")
	errSlackAPIURLFileNotAllowed         = errors.New("setting Slack api_url_file or global slack_api_url_file is not allowed")
	errVictorOpsAPIKeyFileNotAllowed     = errors.New("setting VictorOps api_key_file or global victorops_api_key_file is not allowed")
	errOpsGenieAPIKeyFileFileNotAllowed  = errors.New("setting OpsGenie api_key_file or global opsgenie_api_key_file is not allowed")
	errPagerDutyServiceKeyFileNotAllowed = errors.New("setting PagerDuty service_key_file is not allowed")
	errPagerDutyRoutingKeyFileNotAllowed = errors.New("setting PagerDuty routing_key_file is not allowed")
	errPushoverUserKeyFileNotAllowed     = errors.New("setting Pushover user_key_file is not allowed")
	errPushoverTokenFileNotAllowed       = errors.New("setting Pushover token_file is not allowed")
	errTelegramBotTokenFileNotAllowed    = errors.New("setting Telegram bot_token_file is not allowed")
	errWebhookURLFileNotAllowed          = errors.New("setting Webhook url_file is not allowed")
)

// ValidateAlertmanagerConfig recursively scans the input config looking for data types for which
// we have a specific validation and, whenever encountered, it runs their validation. Returns the
// first error or nil if validation succeeds.
func ValidateAlertmanagerConfig(cfg any) error {
	v := reflect.ValueOf(cfg)
	t := v.Type()

	// Skip invalid, the zero value or a nil pointer (checked by zero value).
	if !v.IsValid() || v.IsZero() {
		return nil
	}

	// If the input config is a pointer then we need to get its value.
	// At this point the pointer value can't be nil.
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
		t = v.Type()
	}

	// Check if the input config is a data type for which we have a specific validation.
	// At this point the value can't be a pointer anymore.
	switch t {
	case reflect.TypeOf(config.GlobalConfig{}):
		if err := validateGlobalConfig(v.Interface().(config.GlobalConfig)); err != nil {
			return err
		}

	// v0mimir1 receiver configs
	case reflect.TypeOf(discord_v0mimir1.Config{}):
		if err := validateDiscordConfig(v.Interface().(discord_v0mimir1.Config)); err != nil {
			return err
		}

	case reflect.TypeOf(email_v0mimir1.Config{}):
		if err := validateEmailConfig(v.Interface().(email_v0mimir1.Config)); err != nil {
			return err
		}

	case reflect.TypeOf(slack_v0mimir1.Config{}):
		if err := validateSlackConfig(v.Interface().(slack_v0mimir1.Config)); err != nil {
			return err
		}

	case reflect.TypeOf(opsgenie_v0mimir1.Config{}):
		if err := validateOpsGenieConfig(v.Interface().(opsgenie_v0mimir1.Config)); err != nil {
			return err
		}

	case reflect.TypeOf(victorops_v0mimir1.Config{}):
		if err := validateVictorOpsConfig(v.Interface().(victorops_v0mimir1.Config)); err != nil {
			return err
		}

	case reflect.TypeOf(pagerduty_v0mimir1.Config{}):
		if err := validatePagerDutyConfig(v.Interface().(pagerduty_v0mimir1.Config)); err != nil {
			return err
		}

	case reflect.TypeOf(pushover_v0mimir1.Config{}):
		if err := validatePushoverConfig(v.Interface().(pushover_v0mimir1.Config)); err != nil {
			return err
		}

	case reflect.TypeOf(teams_v0mimir1.Config{}):
		if err := validateMSTeamsConfig(v.Interface().(teams_v0mimir1.Config)); err != nil {
			return err
		}

	case reflect.TypeOf(teams_v0mimir2.Config{}):
		if err := validateMSTeamsV2Config(v.Interface().(teams_v0mimir2.Config)); err != nil {
			return err
		}

	case reflect.TypeOf(telegram_v0mimir1.Config{}):
		if err := validateTelegramConfig(v.Interface().(telegram_v0mimir1.Config)); err != nil {
			return err
		}

	case reflect.TypeOf(webhook_v0mimir1.Config{}):
		if err := validateWebhookConfig(v.Interface().(webhook_v0mimir1.Config)); err != nil {
			return err
		}

	// v0mimir1 HTTP client config (used by all v0mimir1 receiver configs)
	case reflect.TypeOf(httpcfg.HTTPClientConfig{}):
		if err := validateReceiverHTTPConfig(v.Interface().(httpcfg.HTTPClientConfig)); err != nil {
			return err
		}

	case reflect.TypeOf(httpcfg.TLSConfig{}):
		if err := validateReceiverTLSConfig(v.Interface().(httpcfg.TLSConfig)); err != nil {
			return err
		}

	// prometheus common HTTP client config (used by GlobalConfig)
	case reflect.TypeOf(commoncfg.HTTPClientConfig{}):
		if err := validateCommonHTTPConfig(v.Interface().(commoncfg.HTTPClientConfig)); err != nil {
			return err
		}

	case reflect.TypeOf(commoncfg.TLSConfig{}):
		if err := validateCommonTLSConfig(v.Interface().(commoncfg.TLSConfig)); err != nil {
			return err
		}
	}

	// If the input config is a struct, recursively iterate on all fields.
	if t.Kind() == reflect.Struct {
		for i := 0; i < t.NumField(); i++ {
			field := t.Field(i)
			fieldValue := v.FieldByIndex(field.Index)

			// Skip any field value which can't be converted to interface (eg. primitive types).
			if fieldValue.CanInterface() {
				if err := ValidateAlertmanagerConfig(fieldValue.Interface()); err != nil {
					return err
				}
			}
		}
	}

	if t.Kind() == reflect.Slice || t.Kind() == reflect.Array {
		for i := 0; i < v.Len(); i++ {
			fieldValue := v.Index(i)

			// Skip any field value which can't be converted to interface (eg. primitive types).
			if fieldValue.CanInterface() {
				if err := ValidateAlertmanagerConfig(fieldValue.Interface()); err != nil {
					return err
				}
			}
		}
	}

	if t.Kind() == reflect.Map {
		for _, key := range v.MapKeys() {
			fieldValue := v.MapIndex(key)

			// Skip any field value which can't be converted to interface (eg. primitive types).
			if fieldValue.CanInterface() {
				if err := ValidateAlertmanagerConfig(fieldValue.Interface()); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// validateReceiverHTTPConfig validates the v0mimir1 HTTP config and returns an error if it contains
// settings not allowed by Mimir.
func validateReceiverHTTPConfig(cfg httpcfg.HTTPClientConfig) error {
	if cfg.BasicAuth != nil && cfg.BasicAuth.PasswordFile != "" {
		return errPasswordFileNotAllowed
	}
	if cfg.Authorization != nil && cfg.Authorization.CredentialsFile != "" {
		return errPasswordFileNotAllowed
	}
	if cfg.BearerTokenFile != "" {
		return errPasswordFileNotAllowed
	}
	if cfg.OAuth2 != nil {
		if cfg.OAuth2.ClientSecretFile != "" {
			return errOAuth2SecretFileNotAllowed
		}
		// Mimir's "firewall" doesn't protect OAuth2 client, so we disallow Proxy settings here.
		if cfg.OAuth2.ProxyURL.URL != nil && cfg.OAuth2.ProxyURL.String() != "" {
			return errProxyURLNotAllowed
		}
		if cfg.OAuth2.ProxyFromEnvironment {
			return errProxyFromEnvironmentURLNotAllowed
		}
	}
	// We allow setting proxy config (cfg.ProxyConfig), because Mimir's "firewall" protects those calls.
	return validateReceiverTLSConfig(cfg.TLSConfig)
}

// validateReceiverTLSConfig validates the v0mimir1 TLS config and returns an error if it contains
// settings not allowed by Mimir.
func validateReceiverTLSConfig(cfg httpcfg.TLSConfig) error {
	if cfg.CAFile != "" || cfg.CertFile != "" || cfg.KeyFile != "" || cfg.CA != "" || cfg.Cert != "" || cfg.Key != "" {
		return errTLSConfigNotAllowed
	}
	return nil
}

// validateCommonHTTPConfig validates the prometheus common HTTP config and returns an error if it
// contains settings not allowed by Mimir. Used for GlobalConfig.
func validateCommonHTTPConfig(cfg commoncfg.HTTPClientConfig) error {
	if cfg.BasicAuth != nil && cfg.BasicAuth.PasswordFile != "" {
		return errPasswordFileNotAllowed
	}
	if cfg.Authorization != nil && cfg.Authorization.CredentialsFile != "" {
		return errPasswordFileNotAllowed
	}
	if cfg.BearerTokenFile != "" {
		return errPasswordFileNotAllowed
	}
	if cfg.OAuth2 != nil {
		if cfg.OAuth2.ClientSecretFile != "" {
			return errOAuth2SecretFileNotAllowed
		}
		// Mimir's "firewall" doesn't protect OAuth2 client, so we disallow Proxy settings here.
		if cfg.OAuth2.ProxyURL.URL != nil && cfg.OAuth2.ProxyURL.String() != "" {
			return errProxyURLNotAllowed
		}
		if cfg.OAuth2.ProxyFromEnvironment {
			return errProxyFromEnvironmentURLNotAllowed
		}
	}
	// We allow setting proxy config (cfg.ProxyConfig), because Mimir's "firewall" protects those calls.
	return validateCommonTLSConfig(cfg.TLSConfig)
}

// validateCommonTLSConfig validates the prometheus common TLS config and returns an error if it
// contains settings not allowed by Mimir. Used for GlobalConfig.
func validateCommonTLSConfig(cfg commoncfg.TLSConfig) error {
	if cfg.CAFile != "" || cfg.CertFile != "" || cfg.KeyFile != "" || cfg.CA != "" || cfg.Cert != "" || cfg.Key != "" {
		return errTLSConfigNotAllowed
	}
	return nil
}

// validateGlobalConfig validates the Global config and returns an error if it contains
// settings not allowed by Mimir.
func validateGlobalConfig(cfg config.GlobalConfig) error {
	if cfg.SlackAPIURLFile != "" {
		return errSlackAPIURLFileNotAllowed
	}
	if cfg.OpsGenieAPIKeyFile != "" {
		return errOpsGenieAPIKeyFileFileNotAllowed
	}
	if cfg.SMTPAuthPasswordFile != "" {
		return errPasswordFileNotAllowed
	}
	if cfg.VictorOpsAPIKeyFile != "" {
		return errVictorOpsAPIKeyFileNotAllowed
	}
	return nil
}

// validateDiscordConfig validates the Discord config and returns an error if it
// contains settings not allowed by Mimir.
func validateDiscordConfig(cfg discord_v0mimir1.Config) error {
	if cfg.WebhookURLFile != "" {
		return errWebhookURLFileNotAllowed
	}
	return nil
}

// validateEmailConfig validates the Email config and returns an error if it contains settings not allowed by Mimir.
func validateEmailConfig(cfg email_v0mimir1.Config) error {
	if cfg.AuthPasswordFile != "" {
		return errPasswordFileNotAllowed
	}

	return nil
}

// validateSlackConfig validates the Slack config and returns an error if it contains
// settings not allowed by Mimir.
func validateSlackConfig(cfg slack_v0mimir1.Config) error {
	if cfg.APIURLFile != "" {
		return errSlackAPIURLFileNotAllowed
	}
	return nil
}

// validateVictorOpsConfig validates the VictorOps config and returns an error if it contains
// settings not allowed by Mimir.
func validateVictorOpsConfig(cfg victorops_v0mimir1.Config) error {
	if cfg.APIKeyFile != "" {
		return errVictorOpsAPIKeyFileNotAllowed
	}
	return nil
}

// validateOpsGenieConfig validates the OpsGenie config and returns an error if it contains
// settings not allowed by Mimir.
func validateOpsGenieConfig(cfg opsgenie_v0mimir1.Config) error {
	if cfg.APIKeyFile != "" {
		return errOpsGenieAPIKeyFileFileNotAllowed
	}
	return nil
}

// validatePagerDutyConfig validates the PagerDuty config and returns an error if it contains
// settings not allowed by Mimir.
func validatePagerDutyConfig(cfg pagerduty_v0mimir1.Config) error {
	if cfg.ServiceKeyFile != "" {
		return errPagerDutyServiceKeyFileNotAllowed
	}
	if cfg.RoutingKeyFile != "" {
		return errPagerDutyRoutingKeyFileNotAllowed
	}

	return nil
}

// validatePushoverConfig validates the Pushover config and returns an error if it contains
// settings not allowed by Mimir.
func validatePushoverConfig(cfg pushover_v0mimir1.Config) error {
	if cfg.UserKeyFile != "" {
		return errPushoverUserKeyFileNotAllowed
	}
	if cfg.TokenFile != "" {
		return errPushoverTokenFileNotAllowed
	}

	return nil
}

// validateMSTeamsConfig validates the Microsoft Teams config and returns an error if it
// contains settings not allowed by Mimir.
func validateMSTeamsConfig(cfg teams_v0mimir1.Config) error {
	if cfg.WebhookURLFile != "" {
		return errWebhookURLFileNotAllowed
	}
	return nil
}

// validateMSTeamsV2Config validates the Microsoft Teams V2 config and returns an error if it
// contains settings not allowed by Mimir.
func validateMSTeamsV2Config(cfg teams_v0mimir2.Config) error {
	if cfg.WebhookURLFile != "" {
		return errWebhookURLFileNotAllowed
	}
	return nil
}

// validateTelegramConfig validates the Telegram config and returns an error if it contains
// settings not allowed by Mimir.
func validateTelegramConfig(cfg telegram_v0mimir1.Config) error {
	if cfg.BotTokenFile != "" {
		return errTelegramBotTokenFileNotAllowed
	}
	return nil
}

// validateWebhookConfig validates the Webhook config and returns an error if it contains
// settings not allowed by Mimir.
func validateWebhookConfig(cfg webhook_v0mimir1.Config) error {
	if cfg.URLFile != "" {
		return errWebhookURLFileNotAllowed
	}
	return nil
}
