// SPDX-License-Identifier: AGPL-3.0-only

package api

import (
	"errors"
	"flag"
)

type Config struct {
	Notifications NotificationsConfig `yaml:"notifications"`
}

type NotificationsConfig struct {
	DisableEmail   bool `yaml:"disable_email"`
	DisableWebHook bool `yaml:"disable_webhook"`
}

var (
	ErrEmailNotificationsAreDisabled   = errors.New("email notifications are disabled")
	ErrWebhookNotificationsAreDisabled = errors.New("webhook notifications are disabled")
)

const (
	FormatInvalid = "invalid"
	FormatJSON    = "json"
	FormatYAML    = "yaml"
)

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.BoolVar(&cfg.Notifications.DisableEmail, "configs.notifications.disable-email", false, "Disable Email notifications for Alertmanager.")
	f.BoolVar(&cfg.Notifications.DisableWebHook, "configs.notifications.disable-webhook", false, "Disable WebHook notifications for Alertmanager.")
}
