// SPDX-License-Identifier: AGPL-3.0-only
//go:build requires_docker

package integration

import (
	"net/http"
	"testing"
	"time"

	"github.com/grafana/e2e"
	e2edb "github.com/grafana/e2e/db"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/integration/e2emimir"
)

func TestOOOIngestion(t *testing.T) {
	t.Helper()

	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Start dependencies.
	minio := e2edb.NewMinio(9000, blocksBucketName)
	require.NoError(t, s.StartAndWaitReady(minio))

	// Start Mimir components.
	require.NoError(t, copyFileToSharedDir(s, "docs/configurations/single-process-config-blocks.yaml", mimirConfigFile))

	// Start Mimir in single binary mode, reading the config from file and overwriting
	// the backend config to make it work with Minio.
	flags := mergeFlags(
		DefaultSingleBinaryFlags(),
		BlocksStorageFlags(),
		BlocksStorageS3Flags(),
		map[string]string{
			"-ingester.out-of-order-time-window":                "10m",
			"-ingester.native-histograms-ingestion-enabled":     "true",
			"-ingester.ooo-native-histograms-ingestion-enabled": "true",
		},
	)

	mimir := e2emimir.NewSingleBinary("mimir-1", flags, e2emimir.WithConfigFile(mimirConfigFile), e2emimir.WithPorts(9009, 9095))
	require.NoError(t, s.StartAndWaitReady(mimir))

	c, err := e2emimir.NewClient(mimir.HTTPEndpoint(), mimir.HTTPEndpoint(), "", "", "user-1")
	require.NoError(t, err)

	nowTS := time.Now()
	oooTS := nowTS.Add(-time.Minute)
	tooOldTS := nowTS.Add(-time.Hour)

	var expectedMatrix model.Matrix
	var expectedVector model.Vector

	// Push float series.
	floatSeriesName := "ooo_float_series"

	// Push in-order sample.
	series, expectedVector, _ := generateFloatSeries(floatSeriesName, nowTS)
	res, err := c.Push(series)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, res.StatusCode)

	// Push out-of-order sample.
	series, _, expectedMatrix = generateFloatSeries(floatSeriesName, oooTS)
	res, err = c.Push(series)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, res.StatusCode)

	// Push sample that's older than the out-of-order time window.
	series, _, _ = generateFloatSeries(floatSeriesName, tooOldTS)
	res, err = c.Push(series)
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, res.StatusCode)

	expectedMatrix[0].Values = append(expectedMatrix[0].Values, model.SamplePair{Timestamp: expectedVector[0].Timestamp, Value: expectedVector[0].Value})

	// Query float series.
	rangeResult, err := c.QueryRange(floatSeriesName, nowTS.Add(-time.Minute), nowTS, time.Minute)
	require.NoError(t, err)
	require.Equal(t, model.ValMatrix, rangeResult.Type())
	require.Equal(t, expectedMatrix, rangeResult.(model.Matrix))

	// Push int histogram series.
	intHistogramSeriesName := "ooo_int_histogram_series"

	// Push in-order sample.
	series, expectedVector, _ = generateHistogramSeries(intHistogramSeriesName, nowTS)
	res, err = c.Push(series)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, res.StatusCode)

	// Push out-of-order sample.
	series, _, expectedMatrix = generateHistogramSeries(intHistogramSeriesName, oooTS)
	res, err = c.Push(series)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, res.StatusCode)

	// Push sample that's older than the out-of-order time window.
	series, _, _ = generateHistogramSeries(intHistogramSeriesName, tooOldTS)
	res, err = c.Push(series)
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, res.StatusCode)

	expectedMatrix[0].Histograms = append(expectedMatrix[0].Histograms, model.SampleHistogramPair{Timestamp: expectedVector[0].Timestamp, Histogram: expectedVector[0].Histogram})

	// Query int histogram series.
	rangeResult, err = c.QueryRange(intHistogramSeriesName, nowTS.Add(-time.Minute), nowTS, time.Minute)
	require.NoError(t, err)
	require.Equal(t, model.ValMatrix, rangeResult.Type())
	require.Equal(t, expectedMatrix, rangeResult.(model.Matrix))

	// Push float histogram series.
	floatHistogramSeriesName := "ooo_float_histogram_series"

	// Push in-order sample.
	series, expectedVector, _ = GenerateFloatHistogramSeries(floatHistogramSeriesName, nowTS)
	res, err = c.Push(series)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, res.StatusCode)

	// Push out-of-order sample.
	series, _, expectedMatrix = GenerateFloatHistogramSeries(floatHistogramSeriesName, oooTS)
	res, err = c.Push(series)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, res.StatusCode)

	// Push sample that's older than the out-of-order time window.
	series, _, _ = generateHistogramSeries(floatHistogramSeriesName, tooOldTS)
	res, err = c.Push(series)
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, res.StatusCode)

	expectedMatrix[0].Histograms = append(expectedMatrix[0].Histograms, model.SampleHistogramPair{Timestamp: expectedVector[0].Timestamp, Histogram: expectedVector[0].Histogram})

	// Query float histogram series.
	rangeResult, err = c.QueryRange(floatHistogramSeriesName, nowTS.Add(-time.Minute), nowTS, time.Minute)
	require.NoError(t, err)
	require.Equal(t, model.ValMatrix, rangeResult.Type())
	require.Equal(t, expectedMatrix, rangeResult.(model.Matrix))
}

func TestOOOHistogramIngestionDisabled(t *testing.T) {
	t.Helper()

	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Start dependencies.
	minio := e2edb.NewMinio(9000, blocksBucketName)
	require.NoError(t, s.StartAndWaitReady(minio))

	// Start Mimir components.
	require.NoError(t, copyFileToSharedDir(s, "docs/configurations/single-process-config-blocks.yaml", mimirConfigFile))

	// Start Mimir in single binary mode, reading the config from file and overwriting
	// the backend config to make it work with Minio.
	flags := mergeFlags(
		DefaultSingleBinaryFlags(),
		BlocksStorageFlags(),
		BlocksStorageS3Flags(),
		map[string]string{
			"-ingester.out-of-order-time-window":            "10m",
			"-ingester.native-histograms-ingestion-enabled": "true",
		},
	)

	mimir := e2emimir.NewSingleBinary("mimir-1", flags, e2emimir.WithConfigFile(mimirConfigFile), e2emimir.WithPorts(9009, 9095))
	require.NoError(t, s.StartAndWaitReady(mimir))

	c, err := e2emimir.NewClient(mimir.HTTPEndpoint(), mimir.HTTPEndpoint(), "", "", "user-1")
	require.NoError(t, err)

	nowTS := time.Now()
	oooTS := nowTS.Add(-time.Minute)

	// Push float series.
	floatSeriesName := "ooo_float_series"

	// Push in-order sample.
	series, _, _ := generateFloatSeries(floatSeriesName, nowTS)
	res, err := c.Push(series)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, res.StatusCode)

	// Push out-of-order sample.
	series, _, _ = generateFloatSeries(floatSeriesName, oooTS)
	res, err = c.Push(series)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, res.StatusCode)

	// Push int histogram series.
	intHistogramSeriesName := "ooo_int_histogram_series"

	// Push in-order sample.
	series, _, _ = generateHistogramSeries(intHistogramSeriesName, nowTS)
	res, err = c.Push(series)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, res.StatusCode)

	// Push out-of-order sample.
	series, _, _ = generateHistogramSeries(intHistogramSeriesName, oooTS)
	res, err = c.Push(series)
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, res.StatusCode)

	// Push float histogram series.
	floatHistogramSeriesName := "ooo_float_histogram_series"

	// Push in-order sample.
	series, _, _ = GenerateFloatHistogramSeries(floatHistogramSeriesName, nowTS)
	res, err = c.Push(series)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, res.StatusCode)

	// Push out-of-order sample.
	series, _, _ = GenerateFloatHistogramSeries(floatHistogramSeriesName, oooTS)
	res, err = c.Push(series)
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, res.StatusCode)
}
