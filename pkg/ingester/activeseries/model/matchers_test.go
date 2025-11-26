// SPDX-License-Identifier: AGPL-3.0-only

package activeseriesmodel

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"

	amlabels "github.com/prometheus/alertmanager/pkg/labels"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMatcher_MatchesSeries(t *testing.T) {
	asm := NewMatchers(mustNewCustomTrackersConfigFromMap(t, map[string]string{
		"bar_starts_with_1":                `{bar=~"1.*"}`,
		"does_not_have_foo_label":          `{foo=""}`,
		"has_foo_and_bar_starts_with_1":    `{foo!="", bar=~"1.*"}`,
		"has_foo_label":                    `{foo!=""}`,
		"has_foo_and_is_not_true_or_false": `{foo!="", foo!~"(true|false)"}`,
		"foo_is_true":                      `{foo="true"}`,
		"foo_is_false":                     `{foo="false"}`,
		"foo_is_true_bar_starts_with_1":    `{foo="true", bar=~"1.*"}`,
		"foo_is_true_bar_is_100":           `{foo="true", bar="100"}`,
		"baz_is_boolean":                   `{baz=~"true|false"}`,
		"baz_is_boolean_zzz_is_ok":         `{baz=~"true|false", zzz="ok"}`,
	}))

	for _, tc := range []struct {
		series   labels.Labels
		expected []string
	}{
		{
			series: labels.FromStrings("foo", "true", "baz", "unrelated"),
			expected: []string{
				"foo_is_true",
				"has_foo_label",
			},
		},
		{
			series: labels.FromStrings("foo", "true", "bar", "100", "baz", "unrelated"),
			expected: []string{
				"bar_starts_with_1",
				"foo_is_true",
				"foo_is_true_bar_is_100",
				"foo_is_true_bar_starts_with_1",
				"has_foo_and_bar_starts_with_1",
				"has_foo_label",
			},
		},
		{
			series: labels.FromStrings("foo", "true", "bar", "200", "baz", "unrelated"),
			expected: []string{
				"foo_is_true",
				"has_foo_label",
			},
		},
		{
			series: labels.FromStrings("bar", "200", "baz", "unrelated"),
			expected: []string{
				"does_not_have_foo_label",
			},
		},
		{
			series: labels.FromStrings("bar", "100", "baz", "unrelated"),
			expected: []string{
				"bar_starts_with_1",
				"does_not_have_foo_label",
			},
		},
		{
			series: labels.FromStrings("baz", "unrelated"),
			expected: []string{
				"does_not_have_foo_label",
			},
		},
		// Test cases for foo=false
		{
			series: labels.FromStrings("foo", "false"),
			expected: []string{
				"foo_is_false",
				"has_foo_label",
			},
		},
		{
			series: labels.FromStrings("foo", "false", "bar", "100"),
			expected: []string{
				"bar_starts_with_1",
				"foo_is_false",
				"has_foo_and_bar_starts_with_1",
				"has_foo_label",
			},
		},
		{
			series: labels.FromStrings("foo", "false", "bar", "200"),
			expected: []string{
				"foo_is_false",
				"has_foo_label",
			},
		},
		{
			series: labels.FromStrings("foo", "false", "bar", "150"),
			expected: []string{
				"bar_starts_with_1",
				"foo_is_false",
				"has_foo_and_bar_starts_with_1",
				"has_foo_label",
			},
		},
		// Test cases for baz boolean values
		{
			series: labels.FromStrings("baz", "true"),
			expected: []string{
				"baz_is_boolean",
				"does_not_have_foo_label",
			},
		},
		{
			series: labels.FromStrings("baz", "false"),
			expected: []string{
				"baz_is_boolean",
				"does_not_have_foo_label",
			},
		},
		{
			series: labels.FromStrings("foo", "true", "baz", "true"),
			expected: []string{
				"baz_is_boolean",
				"foo_is_true",
				"has_foo_label",
			},
		},
		{
			series: labels.FromStrings("foo", "false", "baz", "false"),
			expected: []string{
				"baz_is_boolean",
				"foo_is_false",
				"has_foo_label",
			},
		},
		// Test cases for zzz label combinations
		{
			series: labels.FromStrings("baz", "true", "zzz", "ok"),
			expected: []string{
				"baz_is_boolean",
				"baz_is_boolean_zzz_is_ok",
				"does_not_have_foo_label",
			},
		},
		{
			series: labels.FromStrings("baz", "false", "zzz", "ok"),
			expected: []string{
				"baz_is_boolean",
				"baz_is_boolean_zzz_is_ok",
				"does_not_have_foo_label",
			},
		},
		{
			series: labels.FromStrings("foo", "true", "baz", "true", "zzz", "ok"),
			expected: []string{
				"baz_is_boolean",
				"baz_is_boolean_zzz_is_ok",
				"foo_is_true",
				"has_foo_label",
			},
		},
		{
			series: labels.FromStrings("foo", "false", "baz", "false", "zzz", "ok"),
			expected: []string{
				"baz_is_boolean",
				"baz_is_boolean_zzz_is_ok",
				"foo_is_false",
				"has_foo_label",
			},
		},
		{
			series: labels.FromStrings("baz", "true", "zzz", "not_ok"),
			expected: []string{
				"baz_is_boolean",
				"does_not_have_foo_label",
			},
		},
		// Complex combinations
		{
			series: labels.FromStrings("foo", "true", "bar", "123", "baz", "true", "zzz", "ok"),
			expected: []string{
				"bar_starts_with_1",
				"baz_is_boolean",
				"baz_is_boolean_zzz_is_ok",
				"foo_is_true",
				"foo_is_true_bar_starts_with_1",
				"has_foo_and_bar_starts_with_1",
				"has_foo_label",
			},
		},
		{
			series: labels.FromStrings("foo", "false", "bar", "155", "baz", "false", "zzz", "ok"),
			expected: []string{
				"bar_starts_with_1",
				"baz_is_boolean",
				"baz_is_boolean_zzz_is_ok",
				"foo_is_false",
				"has_foo_and_bar_starts_with_1",
				"has_foo_label",
			},
		},
		{
			series: labels.FromStrings("foo", "other_value", "bar", "100"),
			expected: []string{
				"bar_starts_with_1",
				"has_foo_and_bar_starts_with_1",
				"has_foo_label",
				"has_foo_and_is_not_true_or_false",
			},
		},
		{
			series: labels.FromStrings("foo", "other_value"),
			expected: []string{
				"has_foo_label",
				"has_foo_and_is_not_true_or_false",
			},
		},
		// Edge cases
		{
			series: labels.FromStrings("foo", "true", "bar", "100", "baz", "invalid"),
			expected: []string{
				"bar_starts_with_1",
				"foo_is_true",
				"foo_is_true_bar_is_100",
				"foo_is_true_bar_starts_with_1",
				"has_foo_and_bar_starts_with_1",
				"has_foo_label",
			},
		},
		{
			series: labels.FromStrings("foo", "false", "bar", "not_a_number", "baz", "true"),
			expected: []string{
				"baz_is_boolean",
				"foo_is_false",
				"has_foo_label",
			},
		},
		{
			series: labels.FromStrings("bar", "1", "baz", "true", "zzz", "ok"),
			expected: []string{
				"bar_starts_with_1",
				"baz_is_boolean",
				"baz_is_boolean_zzz_is_ok",
				"does_not_have_foo_label",
			},
		},
	} {
		t.Run(tc.series.String(), func(t *testing.T) {
			got := asm.Matches(tc.series)
			gotValues := make([]string, 0, got.Len())
			for i := 0; i < got.Len(); i++ {
				gotValues = append(gotValues, asm.MatcherNames()[int(got.Get(i))])
			}
			sort.Strings(gotValues)
			sort.Strings(tc.expected)
			assert.Equal(t, tc.expected, gotValues)
		})
	}
}

func BenchmarkMatchesSeries(b *testing.B) {
	trackerCounts := []int{10, 100, 1000}
	asms := make([]*Matchers, len(trackerCounts))

	for i, matcherCount := range trackerCounts {
		configMap := map[string]string{}
		for j := 0; j < matcherCount; j++ {
			configMap[strconv.Itoa(j)] = fmt.Sprintf(`{this_will_match_%d="true"}`, j)
		}
		config, err := NewCustomTrackersConfig(configMap)
		require.NoError(b, err)
		asms[i] = NewMatchers(config)
	}

	makeLabels := func(total, matching int) labels.Labels {
		if total < matching {
			b.Fatal("wrong test setup, total < matching")
		}
		builder := labels.NewScratchBuilder(total)
		for i := 0; i < matching; i++ {
			builder.Add(fmt.Sprintf("this_will_match_%d", i), "true")
		}
		for i := matching; i < total; i++ {
			builder.Add(fmt.Sprintf("something_else_%d", i), "true")
		}
		builder.Sort()
		return builder.Labels()
	}

	for i, trackerCount := range trackerCounts {
		for _, bc := range []struct {
			total, matching int
		}{
			{1, 0},
			{1, 1},
			{10, 1},
			{10, 2},
			{10, 5},
			{25, 1},
			{25, 2},
			{25, 5},
			{100, 1},
			{100, 2},
			{100, 5},
		} {
			series := makeLabels(bc.total, bc.matching)
			b.Run(fmt.Sprintf("TrackerCount: %d, Labels: %d, Matching: %d", trackerCount, bc.total, bc.matching), func(b *testing.B) {
				for x := 0; x < b.N; x++ {
					got := asms[i].Matches(series)
					require.Equal(b, bc.matching, got.Len())
				}
			})
		}
	}
}

func BenchmarkMatchesSeriesRealTrackers(b *testing.B) {
	var m map[string]string
	f, err := os.Open("testdata/trackers.json")
	require.NoError(b, err)
	defer f.Close()
	require.NoError(b, json.NewDecoder(f).Decode(&m))

	config, err := NewCustomTrackersConfig(m)
	require.NoError(b, err)
	matchers := NewMatchers(config)

	series := makeBenchSeriesForTestDataTrackers(b)

	matchingCounts := map[int]int{}
	for _, s := range series {
		m := matchers.Matches(s)
		matchingCounts[m.Len()]++
	}
	for i := 0; i <= 4; i++ {
		require.NotZero(b, matchingCounts[i], "no series matched %d matchers", i)
	}
	b.Logf("MatchingCounts: %v", matchingCounts)

	b.Run("match series", func(b *testing.B) {
		total := 0
		for i := 0; i < b.N; i++ {
			for _, s := range series {
				m := matchers.Matches(s)
				total += m.Len()
			}
		}
		require.NotZero(b, total, "this can't be zero, we're just checking to make sure that it's not optimized away")
	})

}

func makeBenchSeriesForTestDataTrackers(b *testing.B) []labels.Labels {
	r := rand.New(rand.NewPCG(0, 0)) // deterministic random source for reproducibility

	// generate 1000 random label names.
	var someRandomLabelNames []string
	for i := 0; i < 1000; i++ {
		var sb strings.Builder
		l := int(10 + r.Int64N(10)) // random length between 10 and 20
		for j := 0; j < l; j++ {
			sb.WriteByte('a' + byte(r.Int64N(26)))    // just some random letters
			if j > 0 && j > l-1 && r.Int64N(10) < 2 { // 20% chance to add an underscore, never at the end or at the start.
				sb.WriteByte('_')
			}
		}
		someRandomLabelNames = append(someRandomLabelNames, sb.String())
	}

	lb := labels.NewScratchBuilder(100)
	makeSeries := func(lvs ...string) labels.Labels {
		hasMetricName := false
		lb.Reset()
		for i := 0; i < len(lvs); i += 2 {
			if i+1 >= len(lvs) {
				panic("odd number of label values")
			}
			lb.Add(lvs[i], lvs[i+1])
			if lvs[i] == model.MetricNameLabel {
				hasMetricName = true
			}
		}
		totalLabels := int(2 + r.Int64N(10)) // 10 fixed labels + 0-60 random labels
		for j := 0; j < totalLabels; j++ {
			lb.Add(someRandomLabelNames[r.Int64N(int64(len(someRandomLabelNames)))], fmt.Sprintf("value_%d", r.Int64N(1000)))
		}
		if !hasMetricName {
			lb.Add(model.MetricNameLabel, someRandomLabelNames[r.Int64N(int64(len(someRandomLabelNames)))])
		}
		lb.Sort()
		return lb.Labels()
	}

	// 2000 series that match 4 matchers.
	series := make([]labels.Labels, 0, 10000)
	for i := 0; i < 2000; i++ {
		series = append(series, makeSeries(
			model.MetricNameLabel, "cloudplatform_something",
			"planets_env", "internal",
			"job", "integrations/activemq",
			"__proxy_source__", "cooldb",
		))
	}

	// series matching this kind of matchers:
	// "wildlife/beyla-cpp": "{telemetry_sdk_name=\"beyla\",telemetry_sdk_language=\"cpp\"}"
	for _, telemetrySDKName := range []string{"beyla", "opentelemetry", "prometheus"} {
		for _, telemetrySDKLanguage := range []string{"cpp", "go", "java", "python", "javascript", "nodejs", "ruby"} {
			// generate 1000 series with random labels
			for i := 0; i < 1000; i++ {
				series = append(series, makeSeries("telemetry_sdk_name", telemetrySDKName, "telemetry_sdk_language", telemetrySDKLanguage))
			}
		}
	}

	// series for:
	//  "planets/alerts": "{__name__=\"planets:alerts\"}",
	//  "planets/metrics": "{__name__=~\"planets.+\"}",
	//  "planets/resource/gauge": "{__name__=\"planets:resource:gauge\"}",
	//  "planets/resource/total": "{__name__=\"planets:resource:total\"}",
	for _, metricName := range []string{
		"planets:alerts",
		"planets:metrics",
		"planets:resource:gauge",
		"planets:resource:total",
	} {
		for i := 0; i < 20; i++ {
			series = append(series, makeSeries(model.MetricNameLabel, metricName, "planets_env", fmt.Sprintf("env_%d", i)))
		}
	}

	// series for:
	//  "planets/input-series": "{__name__!~\"planets:.+\", planets_env!=\"\"}",
	for i := 0; i < 20; i++ {
		series = append(series, makeSeries("planets_env", fmt.Sprintf("env_%d", i)))
	}

	for _, job := range []string{
		// "integrations/activemq": "{job=\"integrations/activemq\"}",
		// etc.
		"integrations/activemq",
		"integrations/aerospike",
		"integrations/agent-check",
		"integrations/apache-activemq",
		"integrations/apache-airflow",
		"integrations/apache-cassandra",
		"integrations/apache-couchdb",
		"integrations/apache-hadoop",
		"integrations/apache-hbase",
		"integrations/apache-mesos",
		"integrations/apache-solr",
		"integrations/apache_http",
		"integrations/apollo-server",
		"integrations/asterisk-prom",

		// "integrations/cloudprovider/fox": "{job=~\"\\d+-CLOUD-fox-.+\"}",
		// etc.
		"123-CLOUD-fox-foo",
		"456-CLOUD-wolf-bar",
		"789-CLOUD-bear-baz",
		"101-CLOUD-lion-x",
		"202-CLOUD-tiger-y",
		"303-CLOUD-elephant-z",
		"404-CLOUD-giraffe-abc",
		"505-CLOUD-zebra-def",
		"606-CLOUD-rhino-ghi",
		"707-CLOUD-hippo-jkl",
		"808-CLOUD-monkey-mno",
		"909-CLOUD-panda-pqr",
		"111-CLOUD-koala-stu",
		"222-CLOUD-kangaroo-vwx",
		"333-CLOUD-penguin-yz",
		"444-CLOUD-dolphin-foo",
		"555-CLOUD-whale-bar",
		"666-CLOUD-shark-baz",
		"777-CLOUD-octopus-x",
		"888-CLOUD-turtle-y",
		"999-CLOUD-crab-z",
		"123-CLOUD-lobster-foo",
		"234-CLOUD-salmon-bar",
		"345-CLOUD-tuna-baz",
		"456-CLOUD-cod-x",
		"567-CLOUD-eagle-y",
		"678-CLOUD-hawk-z",
		"789-CLOUD-falcon-abc",
		"890-CLOUD-owl-def",
		"901-CLOUD-robin-ghi",
		"101-CLOUD-sparrow-jkl",
		"202-CLOUD-parrot-mno",
		"303-CLOUD-canary-pqr",
		"404-CLOUD-raven-stu",
		"505-CLOUD-swan-vwx",
		"606-CLOUD-peacock-yz",
		"707-CLOUD-flamingo-foo",
		"808-CLOUD-pelican-bar",
		"909-CLOUD-stork-baz",
		"111-CLOUD-crane-x",
		"222-CLOUD-heron-y",
		"333-CLOUD-albatross-z",
		"444-CLOUD-seagull-abc",
		"555-CLOUD-pigeon-def",
		"666-CLOUD-dove-ghi",
		"777-CLOUD-finch-jkl",
		"888-CLOUD-wren-mno",
		"999-CLOUD-jay-pqr",
		"123-CLOUD-cardinal-stu",
		"234-CLOUD-bluebird-vwx",
		"345-CLOUD-mockingbird-yz",
		"456-CLOUD-warbler-foo",
		"567-CLOUD-thrush-bar",
		"678-CLOUD-oriole-baz",
		"789-CLOUD-blackbird-x",
		"890-CLOUD-woodpecker-y",
		"901-CLOUD-hummingbird-z",
		"101-CLOUD-ostrich-abc",
		"202-CLOUD-emu-def",
		"303-CLOUD-alpha-ghi",
		"404-CLOUD-beta-jkl",
		"505-CLOUD-gamma-mno",
		"606-CLOUD-delta-pqr",
		"707-CLOUD-epsilon-stu",
		"808-CLOUD-zeta-vwx",
		"909-CLOUD-kiwi-yz",
		"111-CLOUD-toucan-foo",
		"222-CLOUD-macaw-bar",
		"333-CLOUD-cockatoo-baz",
		"444-CLOUD-parakeet-x",
		"555-CLOUD-budgie-y",
		"666-CLOUD-lovebird-z",
		"777-CLOUD-conure-abc",
		"888-CLOUD-magpie-def",
		"999-CLOUD-nightingale-ghi",
		"123-CLOUD-skylark-jkl",
		"234-CLOUD-chickadee-mno",
		"345-CLOUD-nuthatch-pqr",
	} {
		series = append(series, makeSeries("job", job))
	}

	// Series for:
	// "integrations/cloudplatform": "{__name__=~\"telemetryservice_.+|cloudplatform_.+\", job=~\"integration.*\"}",
	for _, job := range []string{
		"integrations/cloudplatform",
		"integrations/cloudplatform-cloudsql",
		"integrations/cloudplatform-cloudsql-postgres",
		// the one below does not match.
		"something-else", // this one does not match, because it has a different metric name.
	} {
		for _, metricName := range []string{
			"telemetryservice_cpu",
			"telemetryservice_memory",
			"cloudplatform_network",
			"cloudplatform_storage",
			"cloudplatform_compute",
			// the one below does not match.
			"cloudprovider_something",
		} {
			for i := 0; i < 20; i++ {
				series = append(series, makeSeries(model.MetricNameLabel, metricName, "job", job, "cloudplatform_project", fmt.Sprintf("project_%d", i)))
			}
		}
	}

	// Series for:
	// "appo11y/beyla-network": "{__name__=\"beyla_network_flow_bytes_total\"}",
	for i := 0; i < 50; i++ {
		series = append(series, makeSeries(model.MetricNameLabel, "beyla_network_flow_bytes_total", "src_address", fmt.Sprintf("192.168.1.%d", i), "dst_address", fmt.Sprintf("10.0.0.%d", i)))
	}

	// Series for:
	// "appo11y/beyla-prom": "{__name__=\"beyla_build_info\"}",
	for i := 0; i < 30; i++ {
		series = append(series, makeSeries(model.MetricNameLabel, "beyla_build_info", "version", fmt.Sprintf("v1.%d.0", i), "commit", fmt.Sprintf("abc%d", i)))
	}

	// Series for telemetry distro name matchers:
	// "appo11y/grafana-otel-distro-dotnet": "{telemetry_distro_name=\"grafana-opentelemetry-dotnet\"}",
	// "appo11y/grafana-otel-distro-java": "{telemetry_distro_name=\"grafana-opentelemetry-java\"}",
	// "appo11y/grafana-otel-distro-starter": "{telemetry_distro_name=\"grafana-opentelemetry-starter\"}",
	for _, distroName := range []string{
		"grafana-opentelemetry-dotnet",
		"grafana-opentelemetry-java",
		"grafana-opentelemetry-starter",
	} {
		for i := 0; i < 100; i++ {
			series = append(series, makeSeries("telemetry_distro_name", distroName, "service_name", fmt.Sprintf("service_%d", i)))
		}
	}

	// Series for various OpenTelemetry distro names:
	// "appo11y/otel-dotnetauto": "{telemetry_distro_name=\"opentelemetry-dotnet-instrumentation\"}",
	// "appo11y/otel-javaagent": "{telemetry_distro_name=\"opentelemetry-java-instrumentation\"}",
	// "appo11y/otel-starter": "{telemetry_distro_name=\"opentelemetry-spring-boot-starter\"}",
	for _, distroName := range []string{
		"opentelemetry-dotnet-instrumentation",
		"opentelemetry-java-instrumentation",
		"opentelemetry-spring-boot-starter",
	} {
		for i := 0; i < 100; i++ {
			series = append(series, makeSeries("telemetry_distro_name", distroName, "service_name", fmt.Sprintf("otel_service_%d", i)))
		}
	}

	// Series for:
	// "appo11y/micrometer": "{telemetry_sdk_name=\"io.micrometer\"}",
	for i := 0; i < 200; i++ {
		series = append(series, makeSeries("telemetry_sdk_name", "io.micrometer", "application", fmt.Sprintf("spring_app_%d", i)))
	}

	// Series for node exporter metrics (infrasku/prototype):
	// "infrasku/prototype/linux/node_arp_entries": "{__name__=\"node_arp_entries\"}",
	for i := 0; i < 10; i++ {
		series = append(series, makeSeries(model.MetricNameLabel, "node_arp_entries", "device", fmt.Sprintf("eth%d", i)))
	}

	// "infrasku/prototype/linux/node_cpu_seconds_total": "{__name__=\"node_cpu_seconds_total\", cpu=\"0\",mode=\"idle\"}",
	for cpu := 0; cpu < 4; cpu++ {
		for _, mode := range []string{"idle", "user", "system", "nice", "iowait"} {
			series = append(series, makeSeries(model.MetricNameLabel, "node_cpu_seconds_total", "cpu", fmt.Sprintf("%d", cpu), "mode", mode))
		}
	}

	// "infrasku/prototype/linux/node_filesystem_avail_bytes/device": "{__name__=\"node_filesystem_avail_bytes\", device=~\"shm|/dev/(root|vd.*|nvme.*|sd.*|xvd.*)\"}",
	for _, device := range []string{"shm", "/dev/root", "/dev/vda1", "/dev/nvme0n1p1", "/dev/sda1", "/dev/xvda1"} {
		series = append(series, makeSeries(model.MetricNameLabel, "node_filesystem_avail_bytes", "device", device, "mountpoint", "/"))
	}

	// "infrasku/prototype/linux/node_filesystem_avail_bytes/fstype": "{__name__=\"node_filesystem_avail_bytes\", fstype=~\"ext.*|btrfs|xfs|zfs|f2fs|vfat|tmpfs\"}",
	for _, fstype := range []string{"ext4", "ext3", "btrfs", "xfs", "zfs", "f2fs", "vfat", "tmpfs"} {
		series = append(series, makeSeries(model.MetricNameLabel, "node_filesystem_avail_bytes", "fstype", fstype, "mountpoint", "/"))
	}

	// "infrasku/prototype/linux/node_filesystem_avail_bytes/mountpoint": "{__name__=\"node_filesystem_avail_bytes\", mountpoint=~\"/boot.*|/run/.*\"}",
	for _, mountpoint := range []string{"/boot", "/boot/efi", "/run/user/1000", "/run/systemd"} {
		series = append(series, makeSeries(model.MetricNameLabel, "node_filesystem_avail_bytes", "mountpoint", mountpoint, "fstype", "ext4"))
	}

	// "infrasku/prototype/linux/node_network_receive_bytes_total": "{__name__=\"node_network_receive_bytes_total\", device!~\"anpi.*|ap.*|awdl.*\"}",
	for _, device := range []string{"eth0", "eth1", "lo", "wlan0", "enp0s3"} {
		series = append(series, makeSeries(model.MetricNameLabel, "node_network_receive_bytes_total", "device", device))
	}

	// "infrasku/prototype/linux/node_os_info": "{__name__=\"node_os_info\", name!=\"macOS\"}",
	for _, osName := range []string{"Ubuntu", "CentOS", "Debian", "Red Hat Enterprise Linux"} {
		series = append(series, makeSeries(model.MetricNameLabel, "node_os_info", "name", osName, "version", "20.04"))
	}

	// "infrasku/prototype/linux/node_uname_info": "{__name__=\"node_uname_info\", sysname=~\"linux|unix\", release=~\".*-generic\"}",
	for _, sysname := range []string{"linux", "unix"} {
		for i := 0; i < 3; i++ {
			series = append(series, makeSeries(model.MetricNameLabel, "node_uname_info", "sysname", sysname, "release", fmt.Sprintf("5.4.%d-generic", i)))
		}
	}

	// macOS variants:
	// "infrasku/prototype/macos/node_filesystem_avail_bytes/device": "{__name__=\"node_filesystem_avail_bytes\", device=~\"/dev/disk.*\"}",
	for i := 0; i < 3; i++ {
		series = append(series, makeSeries(model.MetricNameLabel, "node_filesystem_avail_bytes", "device", fmt.Sprintf("/dev/disk%ds1", i), "fstype", "apfs"))
	}

	// "infrasku/prototype/macos/node_filesystem_avail_bytes/fstype": "{__name__=\"node_filesystem_avail_bytes\", fstype=\"apfs\"}",
	series = append(series, makeSeries(model.MetricNameLabel, "node_filesystem_avail_bytes", "fstype", "apfs", "mountpoint", "/"))

	// "infrasku/prototype/macos/node_filesystem_avail_bytes/mountpoint": "{__name__=\"node_filesystem_avail_bytes\", mountpoint=~\"/System/.*\"}",
	for _, mountpoint := range []string{"/System/Library", "/System/Applications", "/System/Volumes"} {
		series = append(series, makeSeries(model.MetricNameLabel, "node_filesystem_avail_bytes", "mountpoint", mountpoint, "fstype", "apfs"))
	}

	// "infrasku/prototype/macos/node_network_noproto_total": "{__name__=\"node_network_noproto_total\"}",
	series = append(series, makeSeries(model.MetricNameLabel, "node_network_noproto_total"))

	// "infrasku/prototype/macos/node_network_receive_bytes_total": "{__name__=\"node_network_receive_bytes_total\", device=~\"anpi.*|ap.*|awdl.*\"}",
	for _, device := range []string{"anpi0", "ap1", "awdl0"} {
		series = append(series, makeSeries(model.MetricNameLabel, "node_network_receive_bytes_total", "device", device))
	}

	// "infrasku/prototype/macos/node_os_info": "{__name__=\"node_os_info\", name=\"macOS\"}",
	series = append(series, makeSeries(model.MetricNameLabel, "node_os_info", "name", "macOS", "version", "14.0"))

	// "infrasku/prototype/macos/node_uname_info": "{__name__=\"node_uname_info\", sysname=\"Darwin\"}",
	series = append(series, makeSeries(model.MetricNameLabel, "node_uname_info", "sysname", "Darwin", "release", "23.0.0"))

	// Windows specific metrics:
	// "infrasku/prototype/windows/windows_cpu_time_toal": "{__name__=\"windows_cpu_time_total\", core=\"0,0\", mode=\"idle\"}",
	for core := 0; core < 4; core++ {
		for _, mode := range []string{"idle", "user", "system", "interrupt"} {
			series = append(series, makeSeries(model.MetricNameLabel, "windows_cpu_time_total", "core", fmt.Sprintf("%d,0", core), "mode", mode))
		}
	}

	// "infrasku/prototype/windows/windows_cs_logical_processors": "{__name__=\"windows_cs_logical_processors\"}",
	series = append(series, makeSeries(model.MetricNameLabel, "windows_cs_logical_processors"))

	// "infrasku/prototype/windows/windows_diskdrive_info": "{__name__=\"windows_diskdrive_info\", device_id=\"PHYSICALDRIVE0\"}",
	for i := 0; i < 3; i++ {
		series = append(series, makeSeries(model.MetricNameLabel, "windows_diskdrive_info", "device_id", fmt.Sprintf("PHYSICALDRIVE%d", i)))
	}

	// "infrasku/prototype/windows/windows_logical_disk_info": "{__name__=\"windows_logical_disk_info\", disk=\"0\"}",
	for i := 0; i < 3; i++ {
		series = append(series, makeSeries(model.MetricNameLabel, "windows_logical_disk_info", "disk", fmt.Sprintf("%d", i)))
	}

	// "infrasku/prototype/windows/windows_logical_disk_size_bytes": "{__name__=\"windows_logical_disk_size_bytes\", volume=\"C:\"}",
	for _, volume := range []string{"C:", "D:", "E:"} {
		series = append(series, makeSeries(model.MetricNameLabel, "windows_logical_disk_size_bytes", "volume", volume))
	}

	// "infrasku/prototype/windows/windows_os_info": "{__name__=\"windows_os_info\", product!=\"\"}",
	for _, product := range []string{"Windows Server 2019", "Windows 10", "Windows 11"} {
		series = append(series, makeSeries(model.MetricNameLabel, "windows_os_info", "product", product, "version", "10.0"))
	}

	// "infrasku/prototype/windows/windows_os_paging_limit_bytes": "{__name__=\"windows_os_paging_limit_bytes\"}",
	series = append(series, makeSeries(model.MetricNameLabel, "windows_os_paging_limit_bytes"))

	// "infrasku/prototype/windows/windows_service_status": "{__name__=\"windows_service_status\", name=\"winmgmt\"}",
	for _, serviceName := range []string{"winmgmt", "eventlog", "schedule", "wuauserv"} {
		series = append(series, makeSeries(model.MetricNameLabel, "windows_service_status", "name", serviceName))
	}

	// "infrasku/prototype/windows/windows_system_system_up_time": "{__name__=\"windows_system_system_up_time\"}",
	series = append(series, makeSeries(model.MetricNameLabel, "windows_system_system_up_time"))

	// "infrasku/prototype/windows/windows_time_computed_time_offset_seconds": "{__name__=\"windows_time_computed_time_offset_seconds\"}",
	series = append(series, makeSeries(model.MetricNameLabel, "windows_time_computed_time_offset_seconds"))

	// Additional cloud provider patterns and specific matchers:
	// "integrations/azure": "{__name__=~\"azure_.+\", job=~\"integration.*\"}",
	for _, job := range []string{"integrations/azure", "integrations/azure-vm", "integrations/azure-sql"} {
		for _, metricName := range []string{"azure_vm_cpu_percent", "azure_storage_blob_count", "azure_sql_database_cpu_percent"} {
			for i := 0; i < 10; i++ {
				series = append(series, makeSeries(model.MetricNameLabel, metricName, "job", job, "resource_group", fmt.Sprintf("rg_%d", i)))
			}
		}
	}

	// "integrations/monitoringservice": "{__name__=~\"cloudprovider_.+|metricscollector_.+\", job=~\"integration.*\"}",
	for _, job := range []string{"integrations/monitoringservice", "integrations/monitoringservice-ec2"} {
		for _, metricName := range []string{"cloudprovider_ec2_cpu_utilization", "metricscollector_monitoringservice_requests_total", "cloudprovider_rds_database_connections"} {
			for i := 0; i < 15; i++ {
				series = append(series, makeSeries(model.MetricNameLabel, metricName, "job", job, "instance_id", fmt.Sprintf("i-%d", i)))
			}
		}
	}

	// Pattern matches integrations/cloud/cloudprovider with \d+-Glue-.+
	for i := 0; i < 5; i++ {
		jobName := fmt.Sprintf("%d-Glue-job-%d", 1000+i, i)
		for _, metricName := range []string{"cloudprovider_glue_data_processing_units", "cloudprovider_glue_job_runs_total"} {
			series = append(series, makeSeries(model.MetricNameLabel, metricName, "job", jobName, "glue_job_name", fmt.Sprintf("etl-job-%d", i)))
		}
	}

	// Additional specific matchers:
	// "metrics-vendor": "{_dot_internal_dot_mv__type!=\"\"}",
	for i := 0; i < 20; i++ {
		series = append(series, makeSeries("_dot_internal_dot_mv__type", "custom_metric", "mv_service", fmt.Sprintf("service_%d", i)))
	}

	// "influx": "{__proxy_source__=\"influx\"}",
	for i := 0; i < 15; i++ {
		series = append(series, makeSeries("__proxy_source__", "influx", "measurement", fmt.Sprintf("cpu_%d", i)))
	}

	// "asserts/servicegraph/istio": "{__name__=~\"istio_.*\"}",
	for _, metricName := range []string{"istio_requests_total", "istio_request_duration_milliseconds", "istio_tcp_connections_opened_total"} {
		for i := 0; i < 25; i++ {
			series = append(series, makeSeries(model.MetricNameLabel, metricName, "source_service", fmt.Sprintf("service_%d", i), "destination_service", fmt.Sprintf("dst_%d", i)))
		}
	}

	// "integrations/k6": "{__name__=~\"k6_.+\"}",
	for _, metricName := range []string{"k6_http_req_duration", "k6_http_reqs", "k6_vus", "k6_iteration_duration"} {
		for i := 0; i < 10; i++ {
			series = append(series, makeSeries(model.MetricNameLabel, metricName, "scenario", fmt.Sprintf("load_test_%d", i)))
		}
	}

	// Kubernetes monitoring features:
	// "k8smonhelm/feature_annotation_autodiscovery": "{__name__=\"grafana_kubernetes_monitoring_feature_info\", feature=\"annotationAutodiscovery\"}",
	for _, feature := range []string{
		"annotationAutodiscovery", "applicationObservability", "autoInstrumentation", "clusterEvents",
		"clusterMetrics", "integrations", "nodeLogs", "podLogs", "profiling", "prometheusOperatorObjects",
	} {
		series = append(series, makeSeries(model.MetricNameLabel, "grafana_kubernetes_monitoring_feature_info", "feature", feature, "enabled", "true"))
	}

	// "k8smonhelm/version_2_0": "{__name__=\"grafana_kubernetes_monitoring_build_info\", version=~\"2\\\\.0\\\\..*\"}",
	// "k8smonhelm/version_2_1": "{__name__=\"grafana_kubernetes_monitoring_build_info\", version=~\"2\\\\.1\\\\..*\"}",
	for _, version := range []string{"2.0.1", "2.0.5", "2.1.0", "2.1.3"} {
		series = append(series, makeSeries(model.MetricNameLabel, "grafana_kubernetes_monitoring_build_info", "version", version, "git_commit", "abc123"))
	}

	// "k8so11y/grafana_kubernetes_monitoring_build_info": "{__name__=\"grafana_kubernetes_monitoring_build_info\"}",
	series = append(series, makeSeries(model.MetricNameLabel, "grafana_kubernetes_monitoring_build_info", "version", "latest"))

	// "k8so11y/kepler_node_info": "{__name__=\"kepler_node_info\"}",
	for i := 0; i < 5; i++ {
		series = append(series, makeSeries(model.MetricNameLabel, "kepler_node_info", "node", fmt.Sprintf("node-%d", i), "cpu_arch", "amd64"))
	}

	// Node count by cloud provider:
	// "k8so11y/node_count_cloudprovider": "{__name__=\"kube_node_info\", provider_id=~\"^cloudprovider://.*\"}",
	for i := 0; i < 10; i++ {
		series = append(series, makeSeries(model.MetricNameLabel, "kube_node_info", "provider_id", fmt.Sprintf("cloudprovider://us-west-2a/i-12345%d", i), "node", fmt.Sprintf("node-%d", i)))
	}

	// "k8so11y/node_count_azure": "{__name__=\"kube_node_info\", provider_id=~\"^azure://.*\"}",
	for i := 0; i < 5; i++ {
		series = append(series, makeSeries(model.MetricNameLabel, "kube_node_info", "provider_id", fmt.Sprintf("azure://subscriptions/123/resourceGroups/rg/providers/Microsoft.Compute/virtualMachines/vm%d", i), "node", fmt.Sprintf("azure-node-%d", i)))
	}

	// "trees/node_count_computeengine": "{__name__=\"kube_node_info\", provider_id=~\"^computeengine://.*\"}",
	for i := 0; i < 7; i++ {
		series = append(series, makeSeries(model.MetricNameLabel, "kube_node_info", "provider_id", fmt.Sprintf("computeengine://project-123/zones/us-central1-a/instances/instance-%d", i), "node", fmt.Sprintf("computeengine-node-%d", i)))
	}

	// "trees/node_count_other": "{__name__=\"kube_node_info\", provider_id!~\"^(cloudprovider|azure|digitalocean|computeengine|ibm)://.*|^ocid.*\"}",
	for i := 0; i < 3; i++ {
		series = append(series, makeSeries(model.MetricNameLabel, "kube_node_info", "provider_id", fmt.Sprintf("custom://cluster-123/node-%d", i), "node", fmt.Sprintf("custom-node-%d", i)))
	}

	// Additional observability metrics:
	// "o11y/servicegraphmetrics": "{__name__=~\"traces_service_graph_.*\"}",
	for _, metricName := range []string{"traces_service_graph_request_total", "traces_service_graph_request_failed_total", "traces_service_graph_request_server_seconds"} {
		for i := 0; i < 20; i++ {
			series = append(series, makeSeries(model.MetricNameLabel, metricName, "client", fmt.Sprintf("client-%d", i), "server", fmt.Sprintf("server-%d", i)))
		}
	}

	// "o11y/spanmetrics": "{__name__=~\"traces_spanmetrics_.*\"}",
	for _, metricName := range []string{"traces_spanmetrics_latency", "traces_spanmetrics_calls_total", "traces_spanmetrics_size_total"} {
		for i := 0; i < 15; i++ {
			series = append(series, makeSeries(model.MetricNameLabel, metricName, "service_name", fmt.Sprintf("service-%d", i), "operation", fmt.Sprintf("op-%d", i)))
		}
	}

	// "o11y/target_info": "{__name__=\"target_info\"}",
	for i := 0; i < 10; i++ {
		series = append(series, makeSeries(model.MetricNameLabel, "target_info", "service_name", fmt.Sprintf("service-%d", i), "service_version", "1.0.0"))
	}
	return series
}

func TestCustomTrackersConfigs_MalformedMatcher(t *testing.T) {
	for _, matcher := range []string{
		`{foo}`,
		`{foo=~"}`,
	} {
		t.Run(matcher, func(t *testing.T) {
			config := map[string]string{
				"malformed": matcher,
			}

			_, err := NewCustomTrackersConfig(config)
			assert.Error(t, err)
		})
	}
}

func TestAmlabelMatchersToProm_HappyCase(t *testing.T) {
	amMatcher, err := amlabels.NewMatcher(amlabels.MatchRegexp, "foo", "bar.*")
	require.NoError(t, err)

	expected := labels.MustNewMatcher(labels.MatchRegexp, "foo", "bar.*")
	assert.Equal(t, expected.String(), amlabelMatcherToProm(amMatcher).String())
}

func TestAmlabelMatchersToProm_MatchTypeValues(t *testing.T) {
	lastType := amlabels.MatchNotRegexp
	// just checking that our assumption on that MatchType enums are the same is correct
	for mt := amlabels.MatchEqual; mt <= lastType; mt++ {
		assert.Equal(t, mt.String(), labels.MatchType(mt).String())
	}
	// and that nobody just added more match types in amlabels,
	assert.Panics(t, func() {
		_ = (lastType + 1).String()
	}, "amlabels.MatchNotRegexp is expected to be the last enum value, update the test and check mapping")
}
