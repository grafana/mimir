package commands

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/cortexproject/cortex/pkg/util/validation"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
	"gopkg.in/alecthomas/kingpin.v2"
	"gopkg.in/yaml.v3"
)

type OverridesExporterCommand struct {
	listenAddress     string
	metricsEndpoint   string
	overridesFilePath string
	presetsFilePath   string
	refreshInterval   time.Duration

	registry      *prometheus.Registry
	overrideGauge *prometheus.GaugeVec
}

func NewOverridesExporterCommand() *OverridesExporterCommand {
	registry := prometheus.NewRegistry()
	return &OverridesExporterCommand{
		registry: registry,
		overrideGauge: promauto.With(registry).NewGaugeVec(prometheus.GaugeOpts{
			Name: "cortex_overrides",
			Help: "Various different limits.",
		}, []string{"limit_type", "type", "user"}),
	}
}

func (o *OverridesExporterCommand) Register(app *kingpin.Application) {
	overridesExporterCommand := app.Command("overrides-exporter", "The overrides exporter allow to expose metrics about the runtime configuration of Cortex.").Action(o.run)
	overridesExporterCommand.Flag("overrides-file", "File path where overrides config is stored.").Required().StringVar(&o.overridesFilePath)
	// Presets are the small user, medium user, etc config that we have defined.
	overridesExporterCommand.Flag("presets-file", "File path where presets config stored.").Default("").StringVar(&o.presetsFilePath)
	overridesExporterCommand.Flag("listen-address", "Address on which to expose metrics.").Default(":9683").StringVar(&o.listenAddress)
	overridesExporterCommand.Flag("metrics-endpoint", "Path under which to expose metrics.").Default("/metrics").StringVar(&o.metricsEndpoint)
	overridesExporterCommand.Flag("refresh-interval", "Interval how often the overrides and potentially presets files get refreshed.").Default("1m").DurationVar(&o.refreshInterval)
}

func (o *OverridesExporterCommand) updateOverridesMetrics() error {
	if o.overridesFilePath == "" {
		return errors.New("overrides filepath is empty")
	}

	logrus.Debug("updating overrides")

	overrides := &struct {
		TenantLimits map[string]*validation.Limits `yaml:"overrides"`
	}{}
	bytes, err := ioutil.ReadFile(o.overridesFilePath)
	if err != nil {
		return fmt.Errorf("failed to update overrides, err: %w", err)
	}
	if err := yaml.Unmarshal(bytes, overrides); err != nil {
		return fmt.Errorf("failed to update overrides, err: %w", err)
	}
	o.updateMetrics("tenant", overrides.TenantLimits)

	return nil
}

func (o *OverridesExporterCommand) updatePresetsMetrics() error {
	if o.presetsFilePath == "" {
		return nil
	}

	logrus.Debug("updating presets")

	presets := &struct {
		Presets map[string]*validation.Limits `yaml:"presets"`
	}{}
	bytes, err := ioutil.ReadFile(o.presetsFilePath)
	if err != nil {
		return fmt.Errorf("failed to update presets, error reading file: %w", err)
	}
	if err := yaml.Unmarshal(bytes, presets); err != nil {
		return fmt.Errorf("failed to update presets, error parsing YAML: %w", err)
	}
	o.updateMetrics("preset", presets.Presets)
	return nil
}

func (o *OverridesExporterCommand) updateMetrics(typ string, limitsMap map[string]*validation.Limits) {
	for user, limits := range limitsMap {
		o.overrideGauge.WithLabelValues(
			"max_series_per_query", typ, user,
		).Set(float64(limits.MaxSeriesPerQuery))
		o.overrideGauge.WithLabelValues(
			"max_samples_per_query", typ, user,
		).Set(float64(limits.MaxSamplesPerQuery))
		o.overrideGauge.WithLabelValues(
			"max_local_series_per_user", typ, user,
		).Set(float64(limits.MaxLocalSeriesPerUser))
		o.overrideGauge.WithLabelValues(
			"max_local_series_per_metric", typ, user,
		).Set(float64(limits.MaxLocalSeriesPerMetric))
		o.overrideGauge.WithLabelValues(
			"max_global_series_per_user", typ, user,
		).Set(float64(limits.MaxGlobalSeriesPerUser))
		o.overrideGauge.WithLabelValues(
			"max_global_series_per_metric", typ, user,
		).Set(float64(limits.MaxGlobalSeriesPerMetric))
		o.overrideGauge.WithLabelValues(
			"ingestion_rate", typ, user,
		).Set(limits.IngestionRate)
		o.overrideGauge.WithLabelValues(
			"ingestion_burst_size", typ, user,
		).Set(float64(limits.IngestionBurstSize))
	}
}

func (o *OverridesExporterCommand) run(k *kingpin.ParseContext) error {
	if o.overridesFilePath == "" {
		return errors.New("Empty overrides file path")
	}

	// Update the metrics once before starting.
	if err := o.updateOverridesMetrics(); err != nil {
		return err
	}
	if err := o.updatePresetsMetrics(); err != nil {
		return err
	}

	stopCh := make(chan struct{})
	var wg sync.WaitGroup
	defer func() {
		close(stopCh)
		wg.Wait()
	}()

	// Update the metrics every 1 minute.
	wg.Add(1)
	go func() {
		defer wg.Done()

		for {
			select {
			case <-stopCh:
				return
			case <-time.After(o.refreshInterval):
				if err := o.updateOverridesMetrics(); err != nil {
					logrus.Warnf("error updating override metrics: %s", err)
				}
				if err := o.updatePresetsMetrics(); err != nil {
					logrus.Warnf("error updating presets metrics: %s", err)
				}
			}
		}
	}()

	mux := http.NewServeMux()
	mux.Handle(o.metricsEndpoint, promhttp.HandlerFor(o.registry, promhttp.HandlerOpts{
		MaxRequestsInFlight: 10,
		Registry:            o.registry,
	}))

	mux.HandleFunc("/ready", func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "ready", http.StatusOK)
	})

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt)

	server := &http.Server{
		Addr:    o.listenAddress,
		Handler: mux,
	}

	// Block until a signal is received.
	wg.Add(1)
	go func() {
		defer wg.Done()

		select {
		case <-stopCh:
			return
		case s := <-signalCh:
			logrus.Infof("got signal: %s", s)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := server.Shutdown(ctx); err != nil {
				logrus.Warnf("error shutting down http server: %s", err)
			}
		}
	}()

	mode := "runtime config overrides"
	if o.presetsFilePath != "" {
		mode += " and presets"
	}
	logrus.Infof("exposing %s metrics on %s", mode, o.listenAddress)
	if err := server.ListenAndServe(); err != http.ErrServerClosed {
		return err
	}

	return nil
}
