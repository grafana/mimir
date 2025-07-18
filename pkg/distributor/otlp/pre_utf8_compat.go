package otlp

import (
	"strings"

	"github.com/grafana/regexp"
	"github.com/prometheus/prometheus/semconv"
)

type PreUTF8Compatibility struct {
	WithMetricSuffixes bool
	Namespace          string
}

func (c PreUTF8Compatibility) Schema(metricName string, labelNames []string) (semconv.Schema, bool) {
	hasUnderscore := false
	hasPeriod := false
	for _, l := range append([]string{metricName}, labelNames...) {
		for _, r := range l {
			if r == '_' {
				hasUnderscore = true
			} else if r == '.' {
				hasPeriod = true
				break
			}
		}
	}
	if looksPreUTF8 := hasUnderscore && !hasPeriod; !looksPreUTF8 {
		return semconv.Schema{}, false
	}

	preName := metricName
	postName := metricName
	if underscoreNumber.MatchString(metricName) {
		// _<number> -> <number>
		postName = postName[1:]
	}
	if c.Namespace != "" && strings.HasPrefix(postName, c.Namespace+"_") {
		postName = postName[len(c.Namespace)+1:]
	}
	if c.WithMetricSuffixes {
		postName = unitSuffixes.ReplaceAllString(postName, "")
	}
	// _ replaces any "weird" character. Here we guess it was a '.', but it might have been anything.
	postName = strings.ReplaceAll(postName, "_", ".")

	preAttrs := make([]semconv.Attribute, 0, len(labelNames))
	postAttrs := make([]semconv.Attribute, 0, len(labelNames))
	for _, label := range labelNames {
		preAttrs = append(preAttrs, semconv.Attribute{Tag: label})

		if keyLabel.MatchString(label) {
			// key_<number> -> <number>
			label = label[len("key_"):]
		}
		// _ replaces any "weird" character. Here we guess it was a '.', but it might have been anything.
		label = strings.ReplaceAll(label, "_", ".")
		postAttrs = append(postAttrs, semconv.Attribute{Tag: label})
	}

	return semconv.Schema{
		IDs: semconv.IDs{
			Version: 1,
			MetricsIDs: map[string][]semconv.VersionedID{
				preName:        {{ID: semconv.MetricID(preName), IntroVersion: "1.0.0"}},
				preName + ".2": {{ID: semconv.MetricID(postName), IntroVersion: "1.1.0"}},
			},
		},
		Changelog: semconv.Changelog{
			Version: 1,
			MetricsChangelog: map[semconv.SemanticMetricID][]semconv.MetricChange{
				semconv.SemanticMetricID(preName): {{
					Forward: semconv.MetricGroupDescription{
						MetricName: postName,
						Attributes: postAttrs,
					},
					Backward: semconv.MetricGroupDescription{
						MetricName: preName,
						Attributes: preAttrs,
					},
				}},
			},
		},
	}, true
}

var (
	underscoreNumber = regexp.MustCompile("^_[0-9]+")
	unitSuffixes     = regexp.MustCompile(`(_(days|hours|minutes|seconds|milliseconds|microseconds|nanoseconds|bytes|kibibytes|mebibytes|gibibytes|tibibytes|kilobytes|megabytes|gigabytes|terabytes|meters|volts|amperes|joules|watts|grams|celsius|hertz|percent|per_(second|minute|hour|day|week|month|year)))?(_(ratio|total))?$`)
	keyLabel         = regexp.MustCompile("^key_[0-9]")
)
