package filter

import (
	"github.com/cortexproject/cortex/pkg/chunk"
	"github.com/prometheus/common/model"
	"github.com/sirupsen/logrus"
	"gopkg.in/alecthomas/kingpin.v2"
)

type Config struct {
	Name string
	User string
	From int64
	To   int64
}

func (c *Config) Register(cmd *kingpin.CmdClause) {
	cmd.Flag("filter.name", "option to filter metrics by metric name").StringVar(&c.Name)
	cmd.Flag("filter.user", "option to filter metrics by user").StringVar(&c.User)
	cmd.Flag("filter.from", "option to filter metrics by from a specific time point").Int64Var(&c.From)
	cmd.Flag("filter.to", "option to filter metrics by from a specific time point").Int64Var(&c.To)
}

// MetricFilter provides a set of matchers to determine whether a chunk should be returned
type MetricFilter struct {
	User string
	Name string
	From model.Time
	To   model.Time
}

func NewMetricFilter(cfg Config) MetricFilter {
	return MetricFilter{
		User: cfg.User,
		Name: cfg.Name,
		From: model.Time(cfg.From),
		To:   model.Time(cfg.To),
	}
}

// Filter returns true if the chunk passes the filter
func (f *MetricFilter) Filter(c chunk.Chunk) bool {
	if !(c.From.After(f.From) && c.From.Before(f.To)) && !(c.Through.After(f.From) && c.Through.Before(f.To)) {
		logrus.Debugf("chunk %v does not pass filter, incorrect chunk ranges From: %v, To: %v", c.ExternalKey(), c.From, c.Through)
		return false
	}

	if f.Name != "" && f.Name != c.Metric.Get("__name__") {
		logrus.Debugf("chunk %v does not pass filter, incorrect name: %v", c.ExternalKey(), c.Metric.Get("__name__"))
		return false
	}

	return true
}
