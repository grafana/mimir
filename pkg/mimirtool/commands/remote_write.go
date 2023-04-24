// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package commands

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/golang/snappy"
	dto "github.com/prometheus/client_model/go"
	config_util "github.com/prometheus/common/config"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/remote"
	log "github.com/sirupsen/logrus"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/grafana/mimir/pkg/mimirtool/client"
)

type RemoteWriteCommand struct {
	address         string
	remoteWritePath string

	tenantID string
	apiKey   string
	jobLabel string

	TLSCAPath             string
	TLSKeyPath            string
	TLSCertPath           string
	TLSInsecureSkipVerify bool

	writeTimeout time.Duration

	MetricFilesList []string
}

func (c *RemoteWriteCommand) Register(app *kingpin.Application, envVars EnvVarNames) {
	remoteWriteCmd := app.Command("remote-write", "Push series in Grafana Mimir using the remote write API.")
	pushCmd := remoteWriteCmd.Command("push", "Push remote write series.").Action(c.push)
	pushCmd.Arg("metric-files", "The metric files to push.").Required().ExistingFilesVar(&c.MetricFilesList)

	for _, cmd := range []*kingpin.CmdClause{pushCmd} {
		cmd.Flag("address", "Address of the Grafana Mimir cluster; alternatively, set "+envVars.Address+".").
			Envar(envVars.Address).
			Required().
			StringVar(&c.address)
		cmd.Flag("remote-write-path", "Path of the remote write endpoint.").
			Default("/api/v1/push").
			StringVar(&c.remoteWritePath)
		cmd.Flag("remote-write-job-label", "Job label to attach to remote write series.").
			Default("mimirtool").
			StringVar(&c.jobLabel)
		cmd.Flag("id", "Grafana Mimir tenant ID; alternatively, set "+envVars.TenantID+".").
			Envar(envVars.TenantID).
			Default("").
			StringVar(&c.tenantID)
		cmd.Flag("key", "API key to use when contacting Grafana Mimir; alternatively, set "+envVars.APIKey+".").
			Envar(envVars.APIKey).
			Default("").
			StringVar(&c.apiKey)
		cmd.Flag("write-timeout", "timeout for write requests").
			Default("30s").
			DurationVar(&c.writeTimeout)
		cmd.Flag("tls-ca-path", "TLS CA certificate to verify Grafana Mimir API as part of mTLS; alternatively, set "+envVars.TLSCAPath+".").
			Default("").
			Envar(envVars.TLSCAPath).
			StringVar(&c.TLSCAPath)
		cmd.Flag("tls-cert-path", "TLS client certificate to authenticate with the Grafana Mimir API as part of mTLS; alternatively, set "+envVars.TLSCertPath+".").
			Default("").
			Envar(envVars.TLSCertPath).
			StringVar(&c.TLSCertPath)
		cmd.Flag("tls-key-path", "TLS client certificate private key to authenticate with the Grafana Mimir API as part of mTLS; alternatively, set "+envVars.TLSKeyPath+".").
			Default("").
			Envar(envVars.TLSKeyPath).
			StringVar(&c.TLSKeyPath)
		cmd.Flag("tls-insecure-skip-verify", "Skip TLS certificate verification; alternatively, set "+envVars.TLSInsecureSkipVerify+".").
			Default("false").
			Envar(envVars.TLSInsecureSkipVerify).
			BoolVar(&c.TLSInsecureSkipVerify)
	}
}

func (c *RemoteWriteCommand) writeClient() (remote.WriteClient, error) {
	// validate inputs
	addressURL, err := url.Parse(c.address)
	if err != nil {
		return nil, err
	}

	addressURL.Path = filepath.Join(
		addressURL.Path,
		c.remoteWritePath,
	)

	// build client
	writeClient, err := remote.NewWriteClient("remote-write", &remote.ClientConfig{
		URL:     &config_util.URL{URL: addressURL},
		Timeout: model.Duration(c.writeTimeout),
		HTTPClientConfig: config_util.HTTPClientConfig{
			BasicAuth: &config_util.BasicAuth{
				Username: c.tenantID,
				Password: config_util.Secret(c.apiKey),
			},
			TLSConfig: config_util.TLSConfig{
				CAFile:             c.TLSCAPath,
				CertFile:           c.TLSCertPath,
				KeyFile:            c.TLSKeyPath,
				InsecureSkipVerify: c.TLSInsecureSkipVerify,
			},
		},
		Headers: map[string]string{
			"User-Agent": client.UserAgent,
		},
	})
	if err != nil {
		return nil, err
	}

	// if tenant ID is set, add a tenant ID header to every request
	if c.tenantID != "" {
		client, ok := writeClient.(*remote.Client)
		if !ok {
			return nil, fmt.Errorf("unexpected type %T", writeClient)
		}
		client.Client.Transport = &setTenantIDTransport{
			RoundTripper: client.Client.Transport,
			tenantID:     c.tenantID,
		}
	}

	log.Debugf("Created remote write client using endpoint '%s'", redactedURL(addressURL))

	return writeClient, nil
}

func (c *RemoteWriteCommand) push(k *kingpin.ParseContext) error {
	client, err := c.writeClient()
	if err != nil {
		return err
	}

	for _, f := range c.MetricFilesList {
		data, err := os.ReadFile(f)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			continue
		}

		log.Debugf("Parsing metric file %s", f)
		metricsData, err := ParseTextAndFormat(bytes.NewReader(data), c.jobLabel)
		if err != nil {
			return err
		}

		raw, err := metricsData.Marshal()
		if err != nil {
			return err
		}

		// Encode the content into snappy encoding.
		compressed := snappy.Encode(nil, raw)
		err = client.Store(context.Background(), compressed)
		if err != nil {
			return err
		}
	}
	return nil
}

var MetricMetadataTypeValue = map[string]int32{
	"UNKNOWN":        0,
	"COUNTER":        1,
	"GAUGE":          2,
	"HISTOGRAM":      3,
	"GAUGEHISTOGRAM": 4,
	"SUMMARY":        5,
	"INFO":           6,
	"STATESET":       7,
}

// FormatData convert metric family to a writerequest
func FormatData(mf map[string]*dto.MetricFamily, jobLabel string) *prompb.WriteRequest {
	wr := &prompb.WriteRequest{}

	for metricName, data := range mf {
		// Set metadata writerequest
		mtype := MetricMetadataTypeValue[data.Type.String()]
		metadata := prompb.MetricMetadata{
			MetricFamilyName: data.GetName(),
			Type:             prompb.MetricMetadata_MetricType(mtype),
			Help:             data.GetHelp(),
		}
		wr.Metadata = append(wr.Metadata, metadata)

		for _, metric := range data.Metric {
			timeserie := prompb.TimeSeries{
				Labels: []prompb.Label{
					{
						Name:  "__name__",
						Value: metricName,
					},
					{
						Name:  "job",
						Value: jobLabel,
					},
				},
			}

			for _, label := range metric.Label {
				labelname := label.GetName()
				if labelname == "job" {
					labelname = fmt.Sprintf("%s_exported", labelname)
				}
				timeserie.Labels = append(timeserie.Labels, prompb.Label{
					Name:  labelname,
					Value: label.GetValue(),
				})
			}

			timeserie.Samples = []prompb.Sample{
				{
					Timestamp: time.Now().UnixNano() / int64(time.Millisecond),
					Value:     GetValue(metric),
				},
			}

			wr.Timeseries = append(wr.Timeseries, timeserie)
		}
	}
	return wr
}

// ParseTextReader consumes an io.Reader and returns the MetricFamily
func ParseTextReader(input io.Reader) (map[string]*dto.MetricFamily, error) {
	var parser expfmt.TextParser
	mf, err := parser.TextToMetricFamilies(input)
	if err != nil {
		return nil, err
	}
	return mf, nil
}

// GetValue return the value of a timeserie without the need to give value type
func GetValue(m *dto.Metric) float64 {
	switch {
	case m.Gauge != nil:
		return m.GetGauge().GetValue()
	case m.Counter != nil:
		return m.GetCounter().GetValue()
	case m.Untyped != nil:
		return m.GetUntyped().GetValue()
	default:
		return 0.
	}
}

// ParseTextAndFormat return the data in the expected prometheus metrics write request format
func ParseTextAndFormat(input io.Reader, jobLabel string) (*prompb.WriteRequest, error) {
	mf, err := ParseTextReader(input)
	if err != nil {
		return nil, err
	}
	return FormatData(mf, jobLabel), nil
}
