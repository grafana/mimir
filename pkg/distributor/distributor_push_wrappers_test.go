// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/grafana/dskit/user"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
)

func TestDistributor_PushWrapperSnapshotsBeforeNext(t *testing.T) {
	for _, ingestStorage := range []bool{false, true} {
		t.Run(fmt.Sprintf("ingest-storage-%t", ingestStorage), func(t *testing.T) {
			ds, _, _, _ := prepare(t, prepConfig{
				numDistributors: 1, numIngesters: 3, happyIngesters: 3, ingestStorageEnabled: ingestStorage, ingestStoragePartitions: 3, limits: prepareDefaultLimits(),
				configure: func(cfg *Config) {
					cfg.PushWrappers = []PushWrapper{func(next PushFunc) PushFunc {
						return func(ctx context.Context, request *Request) error {
							before, err := request.WriteRequest()
							if err != nil {
								return err
							}
							metricName := strings.Clone(before.Metadata[0].MetricFamilyName)
							samples := len(before.Timeseries[0].Samples)
							value := before.Timeseries[0].Samples[0].Value
							if err := next(ctx, request); err != nil {
								return err
							}
							if metricName != "wrapper_test" || samples != 1 || value != 1 {
								return fmt.Errorf("post-push snapshot changed")
							}
							return nil
						}
					}}
				},
			})
			body := mockWriteRequest([]mimirpb.LabelAdapter{{Name: "__name__", Value: "wrapper_test"}}, 1, time.Now().UnixMilli())
			body.Metadata = []*mimirpb.MetricMetadata{{MetricFamilyName: "wrapper_test", Type: mimirpb.COUNTER}}
			_, err := ds[0].Push(user.InjectOrgID(t.Context(), "user"), body)
			require.NoError(t, err)
		})
	}
}
