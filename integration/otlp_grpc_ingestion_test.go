// SPDX-License-Identifier: AGPL-3.0-only

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/grafana/e2e"
	e2edb "github.com/grafana/e2e/db"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/grafana/mimir/integration/e2emimir"
)

// TestOTLPGRPCIngestion pushes OTLP metrics over the main gRPC server and queries them back.
func TestOTLPGRPCIngestion(t *testing.T) {
	s, err := e2e.NewScenario(networkName)
	require.NoError(t, err)
	defer s.Close()

	// Start dependencies.
	minio := e2edb.NewMinio(9000, blocksBucketName, rulesBucketName)
	require.NoError(t, s.StartAndWaitReady(minio))

	// Start Mimir components.
	require.NoError(t, copyFileToSharedDir(s, "docs/configurations/single-process-config-blocks.yaml", mimirConfigFile))

	flags := mergeFlags(
		DefaultSingleBinaryFlags(),
		BlocksStorageFlags(),
		BlocksStorageS3Flags(),
		RulerStorageS3Flags(),
		map[string]string{
			"-distributor.otlp-grpc-endpoint-enabled": "true",
		},
	)

	mimir := e2emimir.NewSingleBinary("mimir-1", flags, e2emimir.WithConfigFile(mimirConfigFile), e2emimir.WithPorts(9009, 9095), e2emimir.WithOtherPorts(9095))
	require.NoError(t, s.StartAndWaitReady(mimir))

	c, err := e2emimir.NewClient(mimir.HTTPEndpoint(), mimir.HTTPEndpoint(), "", "", "user-1")
	require.NoError(t, err)

	conn, err := grpc.NewClient(mimir.Endpoint(9095), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	// Build a gauge with one data point.
	now := time.Now()
	req := pmetricotlp.NewExportRequest()
	metric := req.Metrics().ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	metric.SetName("series_grpc")
	dp := metric.SetEmptyGauge().DataPoints().AppendEmpty()
	dp.SetDoubleValue(42)
	dp.SetTimestamp(pcommon.NewTimestampFromTime(now))
	dp.Attributes().PutStr("foo", "bar")

	payload, err := req.MarshalProto()
	require.NoError(t, err)
	require.NotEmpty(t, payload)

	ctx := metadata.AppendToOutgoingContext(context.Background(), "X-Scope-OrgID", "user-1")
	resp := pmetricotlp.NewExportResponse()
	// The method name is the one OTLP gRPC clients call.
	require.NoError(t, conn.Invoke(ctx, "/opentelemetry.proto.collector.metrics.v1.MetricsService/Export", req, &resp))
	require.Equal(t, int64(0), resp.PartialSuccess().RejectedDataPoints())

	// Query the series back.
	result, _, _, err := c.Query("series_grpc", now)
	require.NoError(t, err)
	require.Equal(t, model.ValVector, result.Type())
	vector := result.(model.Vector)
	require.Len(t, vector, 1)
	require.Equal(t, model.SampleValue(42), vector[0].Value)
	require.Equal(t, model.LabelValue("bar"), vector[0].Metric["foo"])
}
