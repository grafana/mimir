package commands

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"os"
	"sort"
	"time"

	"github.com/grafana/cortex-tools/pkg/analyse"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/config"
	"github.com/prometheus/common/model"
	log "github.com/sirupsen/logrus"
	"gopkg.in/alecthomas/kingpin.v2"
)

type PrometheusAnalyseCommand struct {
	address     string
	username    string
	password    string
	readTimeout time.Duration

	grafanaMetricsFile string
	outputFile         string
}

func (cmd *PrometheusAnalyseCommand) run(k *kingpin.ParseContext) error {
	metrics := map[string]int{}
	grafanaMetrics := analyse.MetricsInGrafana{}

	byt, err := ioutil.ReadFile(cmd.grafanaMetricsFile)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(byt, &grafanaMetrics); err != nil {
		return err
	}

	rt := api.DefaultRoundTripper
	if cmd.username != "" {
		rt = config.NewBasicAuthRoundTripper(cmd.username, config.Secret(cmd.password), "", api.DefaultRoundTripper)
	}
	promClient, err := api.NewClient(api.Config{
		Address: cmd.address,

		RoundTripper: rt,
	})
	if err != nil {
		return err
	}

	v1api := v1.NewAPI(promClient)
	for _, metric := range grafanaMetrics.MetricsUsed {
		ctx, cancel := context.WithTimeout(context.Background(), cmd.readTimeout)
		defer cancel()

		query := "count(" + metric + ")"
		result, _, err := v1api.Query(ctx, query, time.Now())
		if err != nil {
			return errors.Wrap(err, "error querying "+query)
		}

		vec := result.(model.Vector)
		if len(vec) == 0 {
			metrics[metric] += 0
			log.Debugln(metric, 0)

			continue
		}

		metrics[metric] += int(vec[0].Value)
		log.Debugln(metric, vec[0].Value)
	}

	output := analyse.MetricsInPrometheus{}
	for metric, count := range metrics {
		output.TotalActiveSeries += count
		output.MetricCounts = append(output.MetricCounts, analyse.MetricCount{Metric: metric, Count: count})
	}
	sort.Slice(output.MetricCounts, func(i, j int) bool {
		return output.MetricCounts[i].Count > output.MetricCounts[j].Count
	})

	out, err := json.MarshalIndent(output, "", "  ")
	if err != nil {
		return err
	}

	if err := ioutil.WriteFile(cmd.outputFile, out, os.FileMode(int(0666))); err != nil {
		return err
	}

	return nil
}
