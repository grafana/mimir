// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/common/blob/main/version/info.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors.

package version

import (
	"bytes"
	"fmt"
	"runtime"
	"strings"
	"text/template"

	"github.com/prometheus/client_golang/prometheus"
)

// Build information. Populated at build-time.
// Note: Removed BuildUser and BuildDate for reproducible builds
var (
	Version   = "unknown"
	Revision  = "unknown"
	Branch    = "unknown"
	GoVersion = runtime.Version()
)

// NewCollector returns a collector that exports metrics about current version
// information.
func NewCollector(program string) prometheus.Collector {
	//lint:ignore faillint In this case we want to just want to create the metric.
	return prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Namespace: program,
			Name:      "build_info",
			Help: fmt.Sprintf(
				"A metric with a constant '1' value labeled by version, revision, branch, and goversion from which %s was built.",
				program,
			),
			ConstLabels: prometheus.Labels{
				"version":   Version,
				"revision":  Revision,
				"branch":    Branch,
				"goversion": GoVersion,
			},
		},
		func() float64 { return 1 },
	)
}

// versionInfoTmpl contains the template used by Info.
var versionInfoTmpl = `
{{.program}}, version {{.version}} (branch: {{.branch}}, revision: {{.revision}})
  go version:       {{.goVersion}}
  platform:         {{.platform}}
`

// Print returns version information.
func Print(program string) string {
	m := map[string]string{
		"program":   program,
		"version":   Version,
		"revision":  Revision,
		"branch":    Branch,
		"goVersion": GoVersion,
		"platform":  runtime.GOOS + "/" + runtime.GOARCH,
	}
	t := template.Must(template.New("version").Parse(versionInfoTmpl))

	var buf bytes.Buffer
	if err := t.ExecuteTemplate(&buf, "version", m); err != nil {
		panic(err)
	}
	return strings.TrimSpace(buf.String())
}

// Info returns version, branch and revision information.
func Info() string {
	return fmt.Sprintf("(version=%s, branch=%s, revision=%s)", Version, Branch, Revision)
}

// UserAgent returns build information in format suitable to be used in HTTP User-Agent header.
func UserAgent() string {
	return fmt.Sprintf("mimir/%s", Version)
}
