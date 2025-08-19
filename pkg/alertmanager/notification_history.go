package alertmanager

import (
	"flag"
	"fmt"
	"net/url"

	"github.com/go-kit/log"
	"github.com/grafana/alerting/lokiclient"
	"github.com/grafana/alerting/notificationhistorian"
	"github.com/grafana/alerting/notify/nfstatus"
	"github.com/grafana/dskit/instrument"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type NotificationHistoryConfig struct {
	Enabled               bool   `yaml:"enabled"`
	LokiRemoteURL         string `yaml:"loki_remote_url"`
	LokiTenantID          string `yaml:"loki_tenant_id"`
	LokiBasicAuthUsername string `yaml:"loki_basic_auth_username"`
	LokiBasicAuthPassword string `yaml:"loki_basic_auth_password"`

	//ExternalLabels        map[string]string `yaml:"external_labels"` // TODO
}

func (cfg *NotificationHistoryConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.BoolVar(&cfg.Enabled, prefix+".enabled", false, "Enable notification history.")
	f.StringVar(&cfg.LokiRemoteURL, prefix+".loki-remote-url", "", "Loki remote URL for notification history.")
	f.StringVar(&cfg.LokiTenantID, prefix+".loki-tenant-id", "", "Loki tenant ID for notification history.")
	f.StringVar(&cfg.LokiBasicAuthUsername, prefix+".loki-basic-auth-username", "", "Loki basic auth username for notification history.")
	f.StringVar(&cfg.LokiBasicAuthPassword, prefix+".loki-basic-auth-password", "", "Loki basic auth password for notification history.")
}

func (cfg *NotificationHistoryConfig) Validate() error {
	if !cfg.Enabled {
		return nil
	}

	if cfg.LokiRemoteURL == "" {
		return fmt.Errorf("loki remote URL must be provided")
	}

	_, err := url.Parse(cfg.LokiRemoteURL)
	if err != nil {
		return fmt.Errorf("failed to parse loki remote URL: %w", err)
	}

	return nil
}

func createNotificationHistorian(cfg NotificationHistoryConfig, reg *prometheus.Registry, logger log.Logger) nfstatus.NotificationHistorian {
	if !cfg.Enabled {
		return nil
	}

	bytesWritten := promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "alertmanager_notification_history_writes_bytes_total",
		Help: "The total number of bytes sent within a batch to the notification history store.",
	})

	writeDuration := instrument.NewHistogramCollector(promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
		Name:    "alertmanager_notification_history_request_duration_seconds",
		Help:    "Histogram of request durations to the notification history store.",
		Buckets: instrument.DefBuckets,
	}, instrument.HistogramCollectorBuckets))

	writesTotal := promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "alertmanager_notification_history_writes_total",
		Help: "The total number of notification history batches that were attempted to be written.",
	})

	writesFailed := promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "alertmanager_notification_history_writes_failed_total",
		Help: "The total number of failed writes of notification history batches.",
	})

	lokiRemoteURL, _ := url.Parse(cfg.LokiRemoteURL)
	return notificationhistorian.NewNotificationHistorian(
		log.With(logger, "component", "notification_historian"),
		lokiclient.LokiConfig{
			WritePathURL:      lokiRemoteURL,
			BasicAuthUser:     cfg.LokiBasicAuthUsername,
			BasicAuthPassword: cfg.LokiBasicAuthPassword,
			TenantID:          cfg.LokiTenantID,
			// ExternalLabels:    cfg.ExternalLabels, // TODO
			Encoder: lokiclient.SnappyProtoEncoder{},
		},
		lokiclient.NewRequester(),
		bytesWritten,
		writeDuration,
		writesTotal,
		writesFailed,
		tracer,
	)
}
