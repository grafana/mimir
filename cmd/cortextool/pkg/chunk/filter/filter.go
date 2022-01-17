package filter

import (
	"math"
	"strings"

	"github.com/grafana/mimir/pkg/chunk"
	"github.com/prometheus/common/model"
	"github.com/sirupsen/logrus"
	"gopkg.in/alecthomas/kingpin.v2"
)

type Config struct {
	Name   string
	User   string
	From   int64
	To     int64
	Labels string
}

func (c *Config) Register(cmd *kingpin.CmdClause) {
	cmd.Flag("filter.name", "option to filter metrics by metric name").StringVar(&c.Name)
	cmd.Flag("filter.user", "option to filter metrics by user").StringVar(&c.User)
	cmd.Flag("filter.from", "option to filter only metrics after specific time point").Int64Var(&c.From)
	cmd.Flag("filter.to", "option to filter only metrics after specific time point").Int64Var(&c.To)
	cmd.Flag("filter.labels", "option to filter metrics with the corresponding labels, provide a comma separated list e.g. <label1>,<label2>").StringVar(&c.Labels)
}

// MetricFilter provides a set of matchers to determine whether a chunk should be returned
type MetricFilter struct {
	User   string
	Name   string
	From   model.Time
	To     model.Time
	Labels []string
}

// NewMetricFilter returns a metric filter
func NewMetricFilter(cfg Config) MetricFilter {
	// By default the maximum time point is chosen if no point is specified
	if cfg.To == 0 {
		cfg.To = math.MaxInt64
	}

	labellist := strings.Split(cfg.Labels, ",")

	return MetricFilter{
		User:   cfg.User,
		Name:   cfg.Name,
		From:   model.Time(cfg.From),
		To:     model.Time(cfg.To),
		Labels: labellist,
	}
}

// Filter returns true if the chunk passes the filter
func (f *MetricFilter) Filter(c chunk.Chunk) bool {
	if f.From > c.Through || c.From > f.To {
		logrus.Debugf("chunk %v does not pass filter, incorrect chunk ranges From: %v, To: %v", c.ExternalKey(), c.From, c.Through)
		return false
	}

	if f.Name != "" && f.Name != c.Metric.Get("__name__") {
		logrus.Debugf("chunk %v does not pass filter, incorrect name: %v", c.ExternalKey(), c.Metric.Get("__name__"))
		return false
	}

	return true
}
