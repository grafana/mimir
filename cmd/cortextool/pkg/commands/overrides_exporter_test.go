package commands

import (
	"bytes"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
)

const (
	metricsOverrides = `
# HELP cortex_overrides Resource limit overrides applied to tenants
# TYPE cortex_overrides gauge
cortex_overrides{limit_name="ingestion_burst_size",user="123"} 7e+06
cortex_overrides{limit_name="ingestion_burst_size",user="159"} 3.5e+06
cortex_overrides{limit_name="ingestion_burst_size",user="456"} 7e+06
cortex_overrides{limit_name="ingestion_burst_size",user="789"} 3.5e+06
cortex_overrides{limit_name="ingestion_rate",user="123"} 700000
cortex_overrides{limit_name="ingestion_rate",user="159"} 350000
cortex_overrides{limit_name="ingestion_rate",user="456"} 700000
cortex_overrides{limit_name="ingestion_rate",user="789"} 350000
cortex_overrides{limit_name="max_global_series_per_metric",user="123"} 600000
cortex_overrides{limit_name="max_global_series_per_metric",user="159"} 300000
cortex_overrides{limit_name="max_global_series_per_metric",user="456"} 600000
cortex_overrides{limit_name="max_global_series_per_metric",user="789"} 300000
cortex_overrides{limit_name="max_global_series_per_user",user="123"} 6e+06
cortex_overrides{limit_name="max_global_series_per_user",user="159"} 3e+06
cortex_overrides{limit_name="max_global_series_per_user",user="456"} 6e+06
cortex_overrides{limit_name="max_global_series_per_user",user="789"} 3e+06
cortex_overrides{limit_name="max_local_series_per_metric",user="123"} 0
cortex_overrides{limit_name="max_local_series_per_metric",user="159"} 0
cortex_overrides{limit_name="max_local_series_per_metric",user="456"} 0
cortex_overrides{limit_name="max_local_series_per_metric",user="789"} 0
cortex_overrides{limit_name="max_local_series_per_user",user="123"} 0
cortex_overrides{limit_name="max_local_series_per_user",user="159"} 0
cortex_overrides{limit_name="max_local_series_per_user",user="456"} 0
cortex_overrides{limit_name="max_local_series_per_user",user="789"} 0
cortex_overrides{limit_name="max_samples_per_query",user="123"} 1e+06
cortex_overrides{limit_name="max_samples_per_query",user="159"} 1e+06
cortex_overrides{limit_name="max_samples_per_query",user="456"} 1e+06
cortex_overrides{limit_name="max_samples_per_query",user="789"} 1e+06
cortex_overrides{limit_name="max_series_per_query",user="123"} 100000
cortex_overrides{limit_name="max_series_per_query",user="159"} 100000
cortex_overrides{limit_name="max_series_per_query",user="456"} 100000
cortex_overrides{limit_name="max_series_per_query",user="789"} 100000
`
	metricsOverridesAndPresets = `
# HELP cortex_overrides Resource limit overrides applied to tenants
# TYPE cortex_overrides gauge
cortex_overrides{limit_name="ingestion_burst_size",user="123"} 7e+06
cortex_overrides{limit_name="ingestion_burst_size",user="159"} 3.5e+06
cortex_overrides{limit_name="ingestion_burst_size",user="456"} 7e+06
cortex_overrides{limit_name="ingestion_burst_size",user="789"} 3.5e+06
cortex_overrides{limit_name="ingestion_rate",user="123"} 700000
cortex_overrides{limit_name="ingestion_rate",user="159"} 350000
cortex_overrides{limit_name="ingestion_rate",user="456"} 700000
cortex_overrides{limit_name="ingestion_rate",user="789"} 350000
cortex_overrides{limit_name="max_global_series_per_metric",user="123"} 600000
cortex_overrides{limit_name="max_global_series_per_metric",user="159"} 300000
cortex_overrides{limit_name="max_global_series_per_metric",user="456"} 600000
cortex_overrides{limit_name="max_global_series_per_metric",user="789"} 300000
cortex_overrides{limit_name="max_global_series_per_user",user="123"} 6e+06
cortex_overrides{limit_name="max_global_series_per_user",user="159"} 3e+06
cortex_overrides{limit_name="max_global_series_per_user",user="456"} 6e+06
cortex_overrides{limit_name="max_global_series_per_user",user="789"} 3e+06
cortex_overrides{limit_name="max_local_series_per_metric",user="123"} 0
cortex_overrides{limit_name="max_local_series_per_metric",user="159"} 0
cortex_overrides{limit_name="max_local_series_per_metric",user="456"} 0
cortex_overrides{limit_name="max_local_series_per_metric",user="789"} 0
cortex_overrides{limit_name="max_local_series_per_user",user="123"} 0
cortex_overrides{limit_name="max_local_series_per_user",user="159"} 0
cortex_overrides{limit_name="max_local_series_per_user",user="456"} 0
cortex_overrides{limit_name="max_local_series_per_user",user="789"} 0
cortex_overrides{limit_name="max_samples_per_query",user="123"} 1e+06
cortex_overrides{limit_name="max_samples_per_query",user="159"} 1e+06
cortex_overrides{limit_name="max_samples_per_query",user="456"} 1e+06
cortex_overrides{limit_name="max_samples_per_query",user="789"} 1e+06
cortex_overrides{limit_name="max_series_per_query",user="123"} 100000
cortex_overrides{limit_name="max_series_per_query",user="159"} 100000
cortex_overrides{limit_name="max_series_per_query",user="456"} 100000
cortex_overrides{limit_name="max_series_per_query",user="789"} 100000
# HELP cortex_overrides_presets Preset limits.
# TYPE cortex_overrides_presets gauge
cortex_overrides_presets{limit_name="ingestion_burst_size",preset="big_user"} 7e+06
cortex_overrides_presets{limit_name="ingestion_burst_size",preset="medium_user"} 3.5e+06
cortex_overrides_presets{limit_name="ingestion_burst_size",preset="mega_user"} 2.25e+07
cortex_overrides_presets{limit_name="ingestion_burst_size",preset="small_user"} 1e+06
cortex_overrides_presets{limit_name="ingestion_burst_size",preset="super_user"} 1.5e+07
cortex_overrides_presets{limit_name="ingestion_rate",preset="big_user"} 700000
cortex_overrides_presets{limit_name="ingestion_rate",preset="medium_user"} 350000
cortex_overrides_presets{limit_name="ingestion_rate",preset="mega_user"} 2.25e+06
cortex_overrides_presets{limit_name="ingestion_rate",preset="small_user"} 100000
cortex_overrides_presets{limit_name="ingestion_rate",preset="super_user"} 1.5e+06
cortex_overrides_presets{limit_name="max_global_series_per_metric",preset="big_user"} 600000
cortex_overrides_presets{limit_name="max_global_series_per_metric",preset="medium_user"} 300000
cortex_overrides_presets{limit_name="max_global_series_per_metric",preset="mega_user"} 1.6e+06
cortex_overrides_presets{limit_name="max_global_series_per_metric",preset="small_user"} 100000
cortex_overrides_presets{limit_name="max_global_series_per_metric",preset="super_user"} 1.2e+06
cortex_overrides_presets{limit_name="max_global_series_per_user",preset="big_user"} 6e+06
cortex_overrides_presets{limit_name="max_global_series_per_user",preset="medium_user"} 3e+06
cortex_overrides_presets{limit_name="max_global_series_per_user",preset="mega_user"} 1.6e+07
cortex_overrides_presets{limit_name="max_global_series_per_user",preset="small_user"} 1e+06
cortex_overrides_presets{limit_name="max_global_series_per_user",preset="super_user"} 1.2e+07
cortex_overrides_presets{limit_name="max_local_series_per_metric",preset="big_user"} 0
cortex_overrides_presets{limit_name="max_local_series_per_metric",preset="medium_user"} 0
cortex_overrides_presets{limit_name="max_local_series_per_metric",preset="mega_user"} 0
cortex_overrides_presets{limit_name="max_local_series_per_metric",preset="small_user"} 0
cortex_overrides_presets{limit_name="max_local_series_per_metric",preset="super_user"} 0
cortex_overrides_presets{limit_name="max_local_series_per_user",preset="big_user"} 0
cortex_overrides_presets{limit_name="max_local_series_per_user",preset="medium_user"} 0
cortex_overrides_presets{limit_name="max_local_series_per_user",preset="mega_user"} 0
cortex_overrides_presets{limit_name="max_local_series_per_user",preset="small_user"} 0
cortex_overrides_presets{limit_name="max_local_series_per_user",preset="super_user"} 0
cortex_overrides_presets{limit_name="max_samples_per_query",preset="big_user"} 1e+06
cortex_overrides_presets{limit_name="max_samples_per_query",preset="medium_user"} 1e+06
cortex_overrides_presets{limit_name="max_samples_per_query",preset="mega_user"} 1e+06
cortex_overrides_presets{limit_name="max_samples_per_query",preset="small_user"} 100000
cortex_overrides_presets{limit_name="max_samples_per_query",preset="super_user"} 1e+06
cortex_overrides_presets{limit_name="max_series_per_query",preset="big_user"} 100000
cortex_overrides_presets{limit_name="max_series_per_query",preset="medium_user"} 100000
cortex_overrides_presets{limit_name="max_series_per_query",preset="mega_user"} 100000
cortex_overrides_presets{limit_name="max_series_per_query",preset="small_user"} 10000
cortex_overrides_presets{limit_name="max_series_per_query",preset="super_user"} 100000
`
)

func TestOverridesExporterCommand(t *testing.T) {
	o := NewOverridesExporterCommand()

	o.overridesFilePath = "testdata/overrides.yaml"

	assert.NoError(t, o.updateOverridesMetrics())
	assert.NoError(t, o.updatePresetsMetrics())

	count, err := testutil.GatherAndCount(o.registry, "cortex_overrides", "cortex_overrides_presets")
	assert.NoError(t, err)
	assert.Equal(t, 32, count)
	assert.NoError(t, testutil.GatherAndCompare(o.registry, bytes.NewReader([]byte(metricsOverrides)), "cortex_overrides", "cortex_overrides_presets"))

	o.presetsFilePath = "testdata/presets.yaml"
	assert.NoError(t, o.updateOverridesMetrics())
	assert.NoError(t, o.updatePresetsMetrics())

	count, err = testutil.GatherAndCount(o.registry, "cortex_overrides", "cortex_overrides_presets")
	assert.NoError(t, err)
	assert.Equal(t, 72, count)
	assert.NoError(t, testutil.GatherAndCompare(o.registry, bytes.NewReader([]byte(metricsOverridesAndPresets)), "cortex_overrides", "cortex_overrides_presets"))
}
