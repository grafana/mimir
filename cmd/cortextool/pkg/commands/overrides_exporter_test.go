package commands

import (
	"bytes"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
)

const (
	metricsOverrides = `
# HELP cortex_overrides Various different limits.
# TYPE cortex_overrides gauge
cortex_overrides{limit_type="ingestion_burst_size",type="tenant",user="123"} 7e+06
cortex_overrides{limit_type="ingestion_burst_size",type="tenant",user="159"} 3.5e+06
cortex_overrides{limit_type="ingestion_burst_size",type="tenant",user="456"} 7e+06
cortex_overrides{limit_type="ingestion_burst_size",type="tenant",user="789"} 3.5e+06
cortex_overrides{limit_type="ingestion_rate",type="tenant",user="123"} 700000
cortex_overrides{limit_type="ingestion_rate",type="tenant",user="159"} 350000
cortex_overrides{limit_type="ingestion_rate",type="tenant",user="456"} 700000
cortex_overrides{limit_type="ingestion_rate",type="tenant",user="789"} 350000
cortex_overrides{limit_type="max_global_series_per_metric",type="tenant",user="123"} 600000
cortex_overrides{limit_type="max_global_series_per_metric",type="tenant",user="159"} 300000
cortex_overrides{limit_type="max_global_series_per_metric",type="tenant",user="456"} 600000
cortex_overrides{limit_type="max_global_series_per_metric",type="tenant",user="789"} 300000
cortex_overrides{limit_type="max_global_series_per_user",type="tenant",user="123"} 6e+06
cortex_overrides{limit_type="max_global_series_per_user",type="tenant",user="159"} 3e+06
cortex_overrides{limit_type="max_global_series_per_user",type="tenant",user="456"} 6e+06
cortex_overrides{limit_type="max_global_series_per_user",type="tenant",user="789"} 3e+06
cortex_overrides{limit_type="max_local_series_per_metric",type="tenant",user="123"} 0
cortex_overrides{limit_type="max_local_series_per_metric",type="tenant",user="159"} 0
cortex_overrides{limit_type="max_local_series_per_metric",type="tenant",user="456"} 0
cortex_overrides{limit_type="max_local_series_per_metric",type="tenant",user="789"} 0
cortex_overrides{limit_type="max_local_series_per_user",type="tenant",user="123"} 0
cortex_overrides{limit_type="max_local_series_per_user",type="tenant",user="159"} 0
cortex_overrides{limit_type="max_local_series_per_user",type="tenant",user="456"} 0
cortex_overrides{limit_type="max_local_series_per_user",type="tenant",user="789"} 0
cortex_overrides{limit_type="max_samples_per_query",type="tenant",user="123"} 1e+06
cortex_overrides{limit_type="max_samples_per_query",type="tenant",user="159"} 1e+06
cortex_overrides{limit_type="max_samples_per_query",type="tenant",user="456"} 1e+06
cortex_overrides{limit_type="max_samples_per_query",type="tenant",user="789"} 1e+06
cortex_overrides{limit_type="max_series_per_query",type="tenant",user="123"} 100000
cortex_overrides{limit_type="max_series_per_query",type="tenant",user="159"} 100000
cortex_overrides{limit_type="max_series_per_query",type="tenant",user="456"} 100000
cortex_overrides{limit_type="max_series_per_query",type="tenant",user="789"} 100000
`
	metricsOverridesAndPresets = `
# HELP cortex_overrides Various different limits.
# TYPE cortex_overrides gauge
cortex_overrides{limit_type="ingestion_burst_size",type="preset",user="big_user"} 7e+06
cortex_overrides{limit_type="ingestion_burst_size",type="preset",user="medium_user"} 3.5e+06
cortex_overrides{limit_type="ingestion_burst_size",type="preset",user="mega_user"} 2.25e+07
cortex_overrides{limit_type="ingestion_burst_size",type="preset",user="small_user"} 1e+06
cortex_overrides{limit_type="ingestion_burst_size",type="preset",user="super_user"} 1.5e+07
cortex_overrides{limit_type="ingestion_burst_size",type="tenant",user="123"} 7e+06
cortex_overrides{limit_type="ingestion_burst_size",type="tenant",user="159"} 3.5e+06
cortex_overrides{limit_type="ingestion_burst_size",type="tenant",user="456"} 7e+06
cortex_overrides{limit_type="ingestion_burst_size",type="tenant",user="789"} 3.5e+06
cortex_overrides{limit_type="ingestion_rate",type="preset",user="big_user"} 700000
cortex_overrides{limit_type="ingestion_rate",type="preset",user="medium_user"} 350000
cortex_overrides{limit_type="ingestion_rate",type="preset",user="mega_user"} 2.25e+06
cortex_overrides{limit_type="ingestion_rate",type="preset",user="small_user"} 100000
cortex_overrides{limit_type="ingestion_rate",type="preset",user="super_user"} 1.5e+06
cortex_overrides{limit_type="ingestion_rate",type="tenant",user="123"} 700000
cortex_overrides{limit_type="ingestion_rate",type="tenant",user="159"} 350000
cortex_overrides{limit_type="ingestion_rate",type="tenant",user="456"} 700000
cortex_overrides{limit_type="ingestion_rate",type="tenant",user="789"} 350000
cortex_overrides{limit_type="max_global_series_per_metric",type="preset",user="big_user"} 600000
cortex_overrides{limit_type="max_global_series_per_metric",type="preset",user="medium_user"} 300000
cortex_overrides{limit_type="max_global_series_per_metric",type="preset",user="mega_user"} 1.6e+06
cortex_overrides{limit_type="max_global_series_per_metric",type="preset",user="small_user"} 100000
cortex_overrides{limit_type="max_global_series_per_metric",type="preset",user="super_user"} 1.2e+06
cortex_overrides{limit_type="max_global_series_per_metric",type="tenant",user="123"} 600000
cortex_overrides{limit_type="max_global_series_per_metric",type="tenant",user="159"} 300000
cortex_overrides{limit_type="max_global_series_per_metric",type="tenant",user="456"} 600000
cortex_overrides{limit_type="max_global_series_per_metric",type="tenant",user="789"} 300000
cortex_overrides{limit_type="max_global_series_per_user",type="preset",user="big_user"} 6e+06
cortex_overrides{limit_type="max_global_series_per_user",type="preset",user="medium_user"} 3e+06
cortex_overrides{limit_type="max_global_series_per_user",type="preset",user="mega_user"} 1.6e+07
cortex_overrides{limit_type="max_global_series_per_user",type="preset",user="small_user"} 1e+06
cortex_overrides{limit_type="max_global_series_per_user",type="preset",user="super_user"} 1.2e+07
cortex_overrides{limit_type="max_global_series_per_user",type="tenant",user="123"} 6e+06
cortex_overrides{limit_type="max_global_series_per_user",type="tenant",user="159"} 3e+06
cortex_overrides{limit_type="max_global_series_per_user",type="tenant",user="456"} 6e+06
cortex_overrides{limit_type="max_global_series_per_user",type="tenant",user="789"} 3e+06
cortex_overrides{limit_type="max_local_series_per_metric",type="preset",user="big_user"} 0
cortex_overrides{limit_type="max_local_series_per_metric",type="preset",user="medium_user"} 0
cortex_overrides{limit_type="max_local_series_per_metric",type="preset",user="mega_user"} 0
cortex_overrides{limit_type="max_local_series_per_metric",type="preset",user="small_user"} 0
cortex_overrides{limit_type="max_local_series_per_metric",type="preset",user="super_user"} 0
cortex_overrides{limit_type="max_local_series_per_metric",type="tenant",user="123"} 0
cortex_overrides{limit_type="max_local_series_per_metric",type="tenant",user="159"} 0
cortex_overrides{limit_type="max_local_series_per_metric",type="tenant",user="456"} 0
cortex_overrides{limit_type="max_local_series_per_metric",type="tenant",user="789"} 0
cortex_overrides{limit_type="max_local_series_per_user",type="preset",user="big_user"} 0
cortex_overrides{limit_type="max_local_series_per_user",type="preset",user="medium_user"} 0
cortex_overrides{limit_type="max_local_series_per_user",type="preset",user="mega_user"} 0
cortex_overrides{limit_type="max_local_series_per_user",type="preset",user="small_user"} 0
cortex_overrides{limit_type="max_local_series_per_user",type="preset",user="super_user"} 0
cortex_overrides{limit_type="max_local_series_per_user",type="tenant",user="123"} 0
cortex_overrides{limit_type="max_local_series_per_user",type="tenant",user="159"} 0
cortex_overrides{limit_type="max_local_series_per_user",type="tenant",user="456"} 0
cortex_overrides{limit_type="max_local_series_per_user",type="tenant",user="789"} 0
cortex_overrides{limit_type="max_samples_per_query",type="preset",user="big_user"} 1e+06
cortex_overrides{limit_type="max_samples_per_query",type="preset",user="medium_user"} 1e+06
cortex_overrides{limit_type="max_samples_per_query",type="preset",user="mega_user"} 1e+06
cortex_overrides{limit_type="max_samples_per_query",type="preset",user="small_user"} 100000
cortex_overrides{limit_type="max_samples_per_query",type="preset",user="super_user"} 1e+06
cortex_overrides{limit_type="max_samples_per_query",type="tenant",user="123"} 1e+06
cortex_overrides{limit_type="max_samples_per_query",type="tenant",user="159"} 1e+06
cortex_overrides{limit_type="max_samples_per_query",type="tenant",user="456"} 1e+06
cortex_overrides{limit_type="max_samples_per_query",type="tenant",user="789"} 1e+06
cortex_overrides{limit_type="max_series_per_query",type="preset",user="big_user"} 100000
cortex_overrides{limit_type="max_series_per_query",type="preset",user="medium_user"} 100000
cortex_overrides{limit_type="max_series_per_query",type="preset",user="mega_user"} 100000
cortex_overrides{limit_type="max_series_per_query",type="preset",user="small_user"} 10000
cortex_overrides{limit_type="max_series_per_query",type="preset",user="super_user"} 100000
cortex_overrides{limit_type="max_series_per_query",type="tenant",user="123"} 100000
cortex_overrides{limit_type="max_series_per_query",type="tenant",user="159"} 100000
cortex_overrides{limit_type="max_series_per_query",type="tenant",user="456"} 100000
cortex_overrides{limit_type="max_series_per_query",type="tenant",user="789"} 100000
`
)

func TestOverridesExporterCommand(t *testing.T) {
	o := NewOverridesExporterCommand()

	o.overridesFilePath = "testdata/overrides.yaml"

	assert.NoError(t, o.updateOverridesMetrics())
	assert.NoError(t, o.updatePresetsMetrics())

	assert.Equal(t, 32, testutil.CollectAndCount(o.overrideGauge, "cortex_overrides"))
	assert.NoError(t, testutil.CollectAndCompare(o.overrideGauge, bytes.NewReader([]byte(metricsOverrides)), "cortex_overrides"))

	o.presetsFilePath = "testdata/presets.yaml"
	assert.NoError(t, o.updateOverridesMetrics())
	assert.NoError(t, o.updatePresetsMetrics())

	assert.Equal(t, 72, testutil.CollectAndCount(o.overrideGauge, "cortex_overrides"))
	assert.NoError(t, testutil.CollectAndCompare(o.overrideGauge, bytes.NewReader([]byte(metricsOverridesAndPresets)), "cortex_overrides"))
}
