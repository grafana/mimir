package alertmanager

import (
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/alertmanager/matchers/parse"
	"github.com/prometheus/alertmanager/pkg/labels"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"reflect"
)

type ParseMetrics struct {
	Total             prometheus.Counter
	DisagreeTotal     prometheus.Counter
	InvalidTotal      prometheus.Counter
	IncompatibleTotal prometheus.Counter
}

func NewParseMetrics(r prometheus.Registerer) *ParseMetrics {
	return &ParseMetrics{
		Total: promauto.With(r).NewCounter(prometheus.CounterOpts{
			Name: "alertmanager_matchers_parse_total",
			Help: "Total number of matchers parsed.",
		}),
		DisagreeTotal: promauto.With(r).NewCounter(prometheus.CounterOpts{
			Name: "alertmanager_matchers_disagree_total",
			Help: "Total number of matchers which produce different parsings (disagreement).",
		}),
		IncompatibleTotal: promauto.With(r).NewCounter(prometheus.CounterOpts{
			Name: "alertmanager_matchers_incompatible_total",
			Help: "Total number of matchers that are incompatible with the UTF-8 parser.",
		}),
		InvalidTotal: promauto.With(r).NewCounter(prometheus.CounterOpts{
			Name: "alertmanager_matchers_invalid_total",
			Help: "Total number of matchers that could not be parsed.",
		}),
	}
}

// ParseMatcher is a replacement for compat/parse.go in Mimir that instruments
// the total number of invalid matchers, the total number of incompatible matchers,
// and the total number of matchers which produce different parsings when parsed
// in both the classic and UTF-8 parsers (disagreement). It returns matchers parsed
// with the classic parser ONLY.
func ParseMatcher(l log.Logger, metrics *ParseMetrics) func(string) (*labels.Matcher, error) {
	return func(s string) (*labels.Matcher, error) {
		m, err := labels.ParseMatcher(s)
		if err != nil {
			// The input is not valid in the classic parser.
			metrics.InvalidTotal.Inc()
			return m, err
		}
		// Parse the input with the UTF-8 parser to see if it's forward compatible.
		n, err := parse.Matcher(s)
		if err != nil {
			metrics.IncompatibleTotal.Inc()
			level.Info(l).Log("msg", "Matcher is not forwards compatible", "input", s)
		} else if !reflect.DeepEqual(m, n) {
			metrics.DisagreeTotal.Inc()
			level.Info(l).Log("msg", "Matcher produces different parsings", "input", s)
		}
		metrics.Total.Inc()
		return m, nil
	}
}

// ParseMatchers is a replacement for compat/parse.go in Mimir that instruments
// the total number of invalid matchers, the total number of incompatible matchers,
// and the total number of matchers which produce different parsings when parsed
// in both the classic and UTF-8 parsers (disagreement). It returns matchers parsed
// with the classic parser ONLY.
func ParseMatchers(l log.Logger, metrics *ParseMetrics) func(string) (labels.Matchers, error) {
	return func(s string) (labels.Matchers, error) {
		m, err := labels.ParseMatchers(s)
		if err != nil {
			// The input is not valid in the classic parser.
			metrics.InvalidTotal.Inc()
			return m, err
		}
		// Parse the input with the UTF-8 parser to see if it's forward compatible.
		n, err := parse.Matchers(s)
		if err != nil {
			metrics.IncompatibleTotal.Inc()
			level.Info(l).Log("msg", "Matchers are not forwards compatible", "input", s)
		} else if !reflect.DeepEqual(labels.Matchers(m), n) {
			metrics.DisagreeTotal.Inc()
			level.Info(l).Log("msg", "Matches produces different parsings", "input", s)
		}
		metrics.Total.Inc()
		return m, nil
	}
}
