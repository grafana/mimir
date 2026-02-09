// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/grafana/dskit/grpcutil"
	"github.com/grafana/dskit/kv/consul"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/test"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/ingester"
	ingester_client "github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/storage/ingest"
	"github.com/grafana/mimir/pkg/util/limiter"
)

func TestDistributor_QueryStream_ShouldSupportIngestStorage(t *testing.T) {
	const tenantID = "user"

	ctx := user.InjectOrgID(context.Background(), tenantID)
	ctx = limiter.ContextWithNewUnlimitedMemoryConsumptionTracker(ctx)
	ctx = limiter.ContextWithNewSeriesLabelsDeduplicator(ctx)
	selectAllSeriesMatcher := mustEqualMatcher("bar", "baz")

	tests := map[string]struct {
		ingesterStateByZone map[string]ingesterZoneState
		// ingesterDataPerZone:
		//   map[zone-a][0] -> ingester-zone-a-0 write request
		//   map[zone-a][1] -> ingester-zone-a-1 write request
		ingesterDataByZone map[string][]*mimirpb.WriteRequest

		preferZone               []string
		minimizeIngesterRequests bool
		shuffleShardSize         int
		matchers                 []*labels.Matcher
		expectedResponse         model.Matrix
		expectedQueriedIngesters int
		expectedErr              error
	}{
		"should query 1 ingester per partition if all ingesters are healthy, ingesters are replicated across 3 zones and requests minimization is enabled": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 5, happyIngesters: 5},
				"zone-b": {numIngesters: 5, happyIngesters: 5},
				"zone-c": {numIngesters: 5, happyIngesters: 5},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					makeWriteRequest(0, 1, 0, false, false, "foo0"),
					makeWriteRequest(0, 1, 0, false, false, "foo1"),
					makeWriteRequest(0, 1, 0, false, false, "foo2"),
					makeWriteRequest(0, 1, 0, false, false, "foo3"),
					makeWriteRequest(0, 1, 0, false, false, "foo4"),
				},
				"zone-b": {
					makeWriteRequest(0, 1, 0, false, false, "foo0"),
					makeWriteRequest(0, 1, 0, false, false, "foo1"),
					makeWriteRequest(0, 1, 0, false, false, "foo2"),
					makeWriteRequest(0, 1, 0, false, false, "foo3"),
					makeWriteRequest(0, 1, 0, false, false, "foo4"),
				},
				"zone-c": {
					makeWriteRequest(0, 1, 0, false, false, "foo0"),
					makeWriteRequest(0, 1, 0, false, false, "foo1"),
					makeWriteRequest(0, 1, 0, false, false, "foo2"),
					makeWriteRequest(0, 1, 0, false, false, "foo3"),
					makeWriteRequest(0, 1, 0, false, false, "foo4"),
				},
			},
			preferZone:               []string{"zone-a"},
			minimizeIngesterRequests: true,
			matchers:                 []*labels.Matcher{selectAllSeriesMatcher},
			expectedResponse:         expectedResponse(0, 1, false, "foo0", "foo1", "foo2", "foo3", "foo4"),
			expectedQueriedIngesters: 5, // Requests minimization is enabled.
		},
		"should query all ingesters if all ingesters are healthy, ingesters are replicated across 3 zones and requests minimization is disabled": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 5, happyIngesters: 5},
				"zone-b": {numIngesters: 5, happyIngesters: 5},
				"zone-c": {numIngesters: 5, happyIngesters: 5},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					makeWriteRequest(0, 1, 0, false, false, "foo0"),
					makeWriteRequest(0, 1, 0, false, false, "foo1"),
					makeWriteRequest(0, 1, 0, false, false, "foo2"),
					makeWriteRequest(0, 1, 0, false, false, "foo3"),
					makeWriteRequest(0, 1, 0, false, false, "foo4"),
				},
				"zone-b": {
					makeWriteRequest(0, 1, 0, false, false, "foo0"),
					makeWriteRequest(0, 1, 0, false, false, "foo1"),
					makeWriteRequest(0, 1, 0, false, false, "foo2"),
					makeWriteRequest(0, 1, 0, false, false, "foo3"),
					makeWriteRequest(0, 1, 0, false, false, "foo4"),
				},
				"zone-c": {
					makeWriteRequest(0, 1, 0, false, false, "foo0"),
					makeWriteRequest(0, 1, 0, false, false, "foo1"),
					makeWriteRequest(0, 1, 0, false, false, "foo2"),
					makeWriteRequest(0, 1, 0, false, false, "foo3"),
					makeWriteRequest(0, 1, 0, false, false, "foo4"),
				},
			},
			preferZone:               []string{"zone-a"},
			minimizeIngesterRequests: false,
			matchers:                 []*labels.Matcher{selectAllSeriesMatcher},
			expectedResponse:         expectedResponse(0, 1, false, "foo0", "foo1", "foo2", "foo3", "foo4"),
			expectedQueriedIngesters: 15, // Requests minimization is disabled.
		},
		"should query 1 ingester per partition if all ingesters are healthy, ingesters are replicated across 2 zones and requests minimization is enabled": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 5, happyIngesters: 5},
				"zone-b": {numIngesters: 5, happyIngesters: 5},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					makeWriteRequest(0, 1, 0, false, false, "foo0"),
					makeWriteRequest(0, 1, 0, false, false, "foo1"),
					makeWriteRequest(0, 1, 0, false, false, "foo2"),
					makeWriteRequest(0, 1, 0, false, false, "foo3"),
					makeWriteRequest(0, 1, 0, false, false, "foo4"),
				},
				"zone-b": {
					makeWriteRequest(0, 1, 0, false, false, "foo0"),
					makeWriteRequest(0, 1, 0, false, false, "foo1"),
					makeWriteRequest(0, 1, 0, false, false, "foo2"),
					makeWriteRequest(0, 1, 0, false, false, "foo3"),
					makeWriteRequest(0, 1, 0, false, false, "foo4"),
				},
			},
			preferZone:               []string{"zone-a"},
			minimizeIngesterRequests: true,
			matchers:                 []*labels.Matcher{selectAllSeriesMatcher},
			expectedResponse:         expectedResponse(0, 1, false, "foo0", "foo1", "foo2", "foo3", "foo4"),
			expectedQueriedIngesters: 5,
		},
		"should succeed with an empty response if the query label matchers match no series": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 5, happyIngesters: 5},
				"zone-b": {numIngesters: 5, happyIngesters: 5},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					makeWriteRequest(0, 1, 0, false, false, "foo0"),
					makeWriteRequest(0, 1, 0, false, false, "foo1"),
					makeWriteRequest(0, 1, 0, false, false, "foo2"),
					makeWriteRequest(0, 1, 0, false, false, "foo3"),
					makeWriteRequest(0, 1, 0, false, false, "foo4"),
				},
				"zone-b": {
					makeWriteRequest(0, 1, 0, false, false, "foo0"),
					makeWriteRequest(0, 1, 0, false, false, "foo1"),
					makeWriteRequest(0, 1, 0, false, false, "foo2"),
					makeWriteRequest(0, 1, 0, false, false, "foo3"),
					makeWriteRequest(0, 1, 0, false, false, "foo4"),
				},
			},
			preferZone:               []string{"zone-a"},
			minimizeIngesterRequests: true,
			matchers:                 []*labels.Matcher{mustEqualMatcher("not", "found")},
			expectedResponse:         expectedResponse(0, 0, false),
			expectedQueriedIngesters: 5,
		},
		"should fallback to ingesters in the non-preferred zone for partitions owned by unhealthy ingesters in the preferred zone": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 5, happyIngesters: 3},
				"zone-b": {numIngesters: 5, happyIngesters: 5},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					makeWriteRequest(0, 1, 0, false, false, "foo0"),
					makeWriteRequest(0, 1, 0, false, false, "foo1"),
					makeWriteRequest(0, 1, 0, false, false, "foo2"),
				},
				"zone-b": {
					makeWriteRequest(0, 1, 0, false, false, "foo0"),
					makeWriteRequest(0, 1, 0, false, false, "foo1"),
					makeWriteRequest(0, 1, 0, false, false, "foo2"),
					makeWriteRequest(0, 1, 0, false, false, "foo3"),
					makeWriteRequest(0, 1, 0, false, false, "foo4"),
				},
			},
			preferZone:               []string{"zone-a"},
			minimizeIngesterRequests: true,
			matchers:                 []*labels.Matcher{selectAllSeriesMatcher},
			expectedResponse:         expectedResponse(0, 1, false, "foo0", "foo1", "foo2", "foo3", "foo4"),
			expectedQueriedIngesters: 5 /* zone-a */ + 2, /* the two failed fall back to zone-b */
		},
		"should not fallback to ingesters in the non-preferred zone if all ingesters in the preferred zone are healthy and some ingesters in the non-preferred zone are unhealthy": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 5, happyIngesters: 5},
				"zone-b": {numIngesters: 5, happyIngesters: 3},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					makeWriteRequest(0, 1, 0, false, false, "foo0"),
					makeWriteRequest(0, 1, 0, false, false, "foo1"),
					makeWriteRequest(0, 1, 0, false, false, "foo2"),
					makeWriteRequest(0, 1, 0, false, false, "foo3"),
					makeWriteRequest(0, 1, 0, false, false, "foo4"),
				},
				"zone-b": {
					makeWriteRequest(0, 1, 0, false, false, "foo0"),
					makeWriteRequest(0, 1, 0, false, false, "foo1"),
					makeWriteRequest(0, 1, 0, false, false, "foo2"),
				},
			},
			preferZone:               []string{"zone-a"},
			minimizeIngesterRequests: true,
			matchers:                 []*labels.Matcher{selectAllSeriesMatcher},
			expectedResponse:         expectedResponse(0, 1, false, "foo0", "foo1", "foo2", "foo3", "foo4"),
			expectedQueriedIngesters: 5, /* zone-a. zone-b isn't queried because of minimization */
		},
		"should fail if all ingesters owning a given partition are unhealthy": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 5, happyIngesters: 3},
				"zone-b": {numIngesters: 5, happyIngesters: 3},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					makeWriteRequest(0, 1, 0, false, false, "foo0"),
					makeWriteRequest(0, 1, 0, false, false, "foo1"),
					makeWriteRequest(0, 1, 0, false, false, "foo2"),
				},
				"zone-b": {
					makeWriteRequest(0, 1, 0, false, false, "foo0"),
					makeWriteRequest(0, 1, 0, false, false, "foo1"),
					makeWriteRequest(0, 1, 0, false, false, "foo2"),
				},
			},
			preferZone:               []string{"zone-a"},
			minimizeIngesterRequests: true,
			matchers:                 []*labels.Matcher{selectAllSeriesMatcher},
			expectedResponse:         expectedResponse(0, 0, false),
			expectedQueriedIngesters: 5 /* zone-a */ + 2, /* the two failed fall back to zone-b */
			expectedErr:              errFail,
		},
		"should succeed if there unhealthy ingesters in every zone but unhealthy ingesters own different partitions": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {states: []ingesterState{ingesterStateHappy, ingesterStateHappy, ingesterStateHappy, ingesterStateHappy, ingesterStateFailed}},
				"zone-b": {states: []ingesterState{ingesterStateHappy, ingesterStateHappy, ingesterStateHappy, ingesterStateFailed, ingesterStateHappy}},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					makeWriteRequest(0, 1, 0, false, false, "foo0"),
					makeWriteRequest(0, 1, 0, false, false, "foo1"),
					makeWriteRequest(0, 1, 0, false, false, "foo2"),
					makeWriteRequest(0, 1, 0, false, false, "foo3"),
					nil,
				},
				"zone-b": {
					makeWriteRequest(0, 1, 0, false, false, "foo0"),
					makeWriteRequest(0, 1, 0, false, false, "foo1"),
					makeWriteRequest(0, 1, 0, false, false, "foo2"),
					nil,
					makeWriteRequest(0, 1, 0, false, false, "foo4"),
				},
			},
			preferZone:               []string{"zone-a"},
			minimizeIngesterRequests: true,
			matchers:                 []*labels.Matcher{selectAllSeriesMatcher},
			expectedResponse:         expectedResponse(0, 1, false, "foo0", "foo1", "foo2", "foo3", "foo4"),
			expectedQueriedIngesters: 5 /* zone-a */ + 1, /* fall back one failed request to zone-b */
		},
		"should succeed if all ingesters owning a partition are unhealthy but unhealthy partition is NOT part of the tenant's shard when shuffle sharding is enabled": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {states: []ingesterState{ingesterStateFailed, ingesterStateHappy, ingesterStateHappy, ingesterStateFailed, ingesterStateHappy}},
				"zone-b": {states: []ingesterState{ingesterStateFailed, ingesterStateHappy, ingesterStateHappy, ingesterStateFailed, ingesterStateHappy}},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					nil,
					makeWriteRequest(0, 1, 0, false, false, "foo1"),
					makeWriteRequest(0, 1, 0, false, false, "foo2"),
					nil,
					makeWriteRequest(0, 1, 0, false, false, "foo4"),
				},
				"zone-b": {
					nil,
					makeWriteRequest(0, 1, 0, false, false, "foo1"),
					makeWriteRequest(0, 1, 0, false, false, "foo2"),
					nil,
					makeWriteRequest(0, 1, 0, false, false, "foo4"),
				},
			},
			shuffleShardSize:         2, // shuffle-sharding chooses partitions 1 and 2 for this tenant
			preferZone:               []string{"zone-a"},
			minimizeIngesterRequests: true,
			matchers:                 []*labels.Matcher{selectAllSeriesMatcher},
			expectedResponse:         expectedResponse(0, 1, false, "foo1", "foo2"),
			expectedQueriedIngesters: 2, /* zone-a only with shuffle-shard of 2 */
		},
		"should succeed if all ingesters owning a partition are LEAVING but LEAVING partition is NOT part of the tenant's shard when shuffle sharding is enabled": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 5, happyIngesters: 5, ringStates: []ring.InstanceState{ring.LEAVING, ring.ACTIVE, ring.ACTIVE, ring.LEAVING, ring.ACTIVE}},
				"zone-b": {numIngesters: 5, happyIngesters: 5, ringStates: []ring.InstanceState{ring.LEAVING, ring.ACTIVE, ring.ACTIVE, ring.LEAVING, ring.ACTIVE}},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					makeWriteRequest(0, 1, 0, false, false, "foo0"),
					makeWriteRequest(0, 1, 0, false, false, "foo1"),
					makeWriteRequest(0, 1, 0, false, false, "foo2"),
					makeWriteRequest(0, 1, 0, false, false, "foo3"),
					makeWriteRequest(0, 1, 0, false, false, "foo4"),
				},
				"zone-b": {
					makeWriteRequest(0, 1, 0, false, false, "foo0"),
					makeWriteRequest(0, 1, 0, false, false, "foo1"),
					makeWriteRequest(0, 1, 0, false, false, "foo2"),
					makeWriteRequest(0, 1, 0, false, false, "foo3"),
					makeWriteRequest(0, 1, 0, false, false, "foo4"),
				},
			},
			shuffleShardSize:         2, // shuffle-sharding chooses partitions 1 and 2 for this tenant
			preferZone:               []string{"zone-a"},
			minimizeIngesterRequests: true,
			matchers:                 []*labels.Matcher{selectAllSeriesMatcher},
			expectedResponse:         expectedResponse(0, 1, false, "foo1", "foo2"),
			expectedQueriedIngesters: 2, /* zone-a only with shuffle-shard of 2 */
		},
		"should fallback to ingesters in the non-preferred zone for partitions owned by LEAVING ingesters in the preferred zone": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 5, happyIngesters: 5, ringStates: []ring.InstanceState{ring.LEAVING, ring.ACTIVE, ring.ACTIVE, ring.ACTIVE, ring.ACTIVE}},
				"zone-b": {numIngesters: 5, happyIngesters: 5},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					makeWriteRequest(0, 1, 0, false, false, "foo0"),
					makeWriteRequest(0, 1, 0, false, false, "foo1"),
					makeWriteRequest(0, 1, 0, false, false, "foo2"),
					makeWriteRequest(0, 1, 0, false, false, "foo3"),
					makeWriteRequest(0, 1, 0, false, false, "foo4"),
				},
				"zone-b": {
					makeWriteRequest(0, 1, 0, false, false, "foo0"),
					makeWriteRequest(0, 1, 0, false, false, "foo1"),
					makeWriteRequest(0, 1, 0, false, false, "foo2"),
					makeWriteRequest(0, 1, 0, false, false, "foo3"),
					makeWriteRequest(0, 1, 0, false, false, "foo4"),
				},
			},
			preferZone:               []string{"zone-a"},
			minimizeIngesterRequests: true,
			matchers:                 []*labels.Matcher{selectAllSeriesMatcher},
			expectedResponse:         expectedResponse(0, 1, false, "foo0", "foo1", "foo2", "foo3", "foo4"),
			expectedQueriedIngesters: 4 /* zone-a ingesters (excluding LEAVING one) */ + 1, /* zone-b ingester as a fallback for the LEAVING one */
		},
		"should fallback to ingesters in the non-preferred zone for partitions owned by JOINING ingesters in the preferred zone": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 5, happyIngesters: 5, ringStates: []ring.InstanceState{ring.JOINING, ring.ACTIVE, ring.ACTIVE, ring.ACTIVE, ring.ACTIVE}},
				"zone-b": {numIngesters: 5, happyIngesters: 5},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					makeWriteRequest(0, 1, 0, false, false, "foo0"),
					makeWriteRequest(0, 1, 0, false, false, "foo1"),
					makeWriteRequest(0, 1, 0, false, false, "foo2"),
					makeWriteRequest(0, 1, 0, false, false, "foo3"),
					makeWriteRequest(0, 1, 0, false, false, "foo4"),
				},
				"zone-b": {
					makeWriteRequest(0, 1, 0, false, false, "foo0"),
					makeWriteRequest(0, 1, 0, false, false, "foo1"),
					makeWriteRequest(0, 1, 0, false, false, "foo2"),
					makeWriteRequest(0, 1, 0, false, false, "foo3"),
					makeWriteRequest(0, 1, 0, false, false, "foo4"),
				},
			},
			preferZone:               []string{"zone-a"},
			minimizeIngesterRequests: true,
			matchers:                 []*labels.Matcher{selectAllSeriesMatcher},
			expectedResponse:         expectedResponse(0, 1, false, "foo0", "foo1", "foo2", "foo3", "foo4"),
			expectedQueriedIngesters: 4 /* zone-a ingesters (excluding JOINING one) */ + 1, /* zone-b ingester as a fallback for the LEAVING one */
		},
		"should fail if all the ingesters owning a partition are in LEAVING state": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 5, happyIngesters: 5, ringStates: []ring.InstanceState{ring.LEAVING, ring.ACTIVE, ring.ACTIVE, ring.ACTIVE, ring.ACTIVE}},
				"zone-b": {numIngesters: 5, happyIngesters: 5, ringStates: []ring.InstanceState{ring.LEAVING, ring.ACTIVE, ring.ACTIVE, ring.ACTIVE, ring.ACTIVE}},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					makeWriteRequest(0, 1, 0, false, false, "foo0"),
					makeWriteRequest(0, 1, 0, false, false, "foo1"),
					makeWriteRequest(0, 1, 0, false, false, "foo2"),
					makeWriteRequest(0, 1, 0, false, false, "foo3"),
					makeWriteRequest(0, 1, 0, false, false, "foo4"),
				},
				"zone-b": {
					makeWriteRequest(0, 1, 0, false, false, "foo0"),
					makeWriteRequest(0, 1, 0, false, false, "foo1"),
					makeWriteRequest(0, 1, 0, false, false, "foo2"),
					makeWriteRequest(0, 1, 0, false, false, "foo3"),
					makeWriteRequest(0, 1, 0, false, false, "foo4"),
				},
			},
			preferZone:               []string{"zone-a"},
			minimizeIngesterRequests: true,
			matchers:                 []*labels.Matcher{selectAllSeriesMatcher},
			expectedErr:              ring.ErrTooManyUnhealthyInstances,
		},
		"should fail if all the ingesters owning a partition are in JOINING state": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 5, happyIngesters: 5, ringStates: []ring.InstanceState{ring.JOINING, ring.ACTIVE, ring.ACTIVE, ring.ACTIVE, ring.ACTIVE}},
				"zone-b": {numIngesters: 5, happyIngesters: 5, ringStates: []ring.InstanceState{ring.JOINING, ring.ACTIVE, ring.ACTIVE, ring.ACTIVE, ring.ACTIVE}},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					makeWriteRequest(0, 1, 0, false, false, "foo0"),
					makeWriteRequest(0, 1, 0, false, false, "foo1"),
					makeWriteRequest(0, 1, 0, false, false, "foo2"),
					makeWriteRequest(0, 1, 0, false, false, "foo3"),
					makeWriteRequest(0, 1, 0, false, false, "foo4"),
				},
				"zone-b": {
					makeWriteRequest(0, 1, 0, false, false, "foo0"),
					makeWriteRequest(0, 1, 0, false, false, "foo1"),
					makeWriteRequest(0, 1, 0, false, false, "foo2"),
					makeWriteRequest(0, 1, 0, false, false, "foo3"),
					makeWriteRequest(0, 1, 0, false, false, "foo4"),
				},
			},
			preferZone:               []string{"zone-a"},
			minimizeIngesterRequests: true,
			matchers:                 []*labels.Matcher{selectAllSeriesMatcher},
			expectedErr:              ring.ErrTooManyUnhealthyInstances,
		},
		"should succeed if there are ingesters in LEAVING state in both zones, but they own different partitions": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 5, happyIngesters: 5, ringStates: []ring.InstanceState{ring.LEAVING, ring.ACTIVE, ring.LEAVING, ring.ACTIVE, ring.LEAVING}},
				"zone-b": {numIngesters: 5, happyIngesters: 5, ringStates: []ring.InstanceState{ring.ACTIVE, ring.LEAVING, ring.ACTIVE, ring.LEAVING, ring.ACTIVE}},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					makeWriteRequest(0, 1, 0, false, false, "foo0"),
					makeWriteRequest(0, 1, 0, false, false, "foo1"),
					makeWriteRequest(0, 1, 0, false, false, "foo2"),
					makeWriteRequest(0, 1, 0, false, false, "foo3"),
					makeWriteRequest(0, 1, 0, false, false, "foo4"),
				},
				"zone-b": {
					makeWriteRequest(0, 1, 0, false, false, "foo0"),
					makeWriteRequest(0, 1, 0, false, false, "foo1"),
					makeWriteRequest(0, 1, 0, false, false, "foo2"),
					makeWriteRequest(0, 1, 0, false, false, "foo3"),
					makeWriteRequest(0, 1, 0, false, false, "foo4"),
				},
			},
			preferZone:               []string{"zone-a"},
			minimizeIngesterRequests: true,
			matchers:                 []*labels.Matcher{selectAllSeriesMatcher},
			expectedResponse:         expectedResponse(0, 1, false, "foo0", "foo1", "foo2", "foo3", "foo4"),
			expectedQueriedIngesters: 2 /* zone-a ingesters (excluding LEAVING one) */ + 3, /* zone-b ingesters as fallback for the LEAVING ones */
		},
		"should succeed if there are ingesters in JOINING state in both zones, but they own different partitions": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 5, happyIngesters: 5, ringStates: []ring.InstanceState{ring.JOINING, ring.ACTIVE, ring.JOINING, ring.ACTIVE, ring.JOINING}},
				"zone-b": {numIngesters: 5, happyIngesters: 5, ringStates: []ring.InstanceState{ring.ACTIVE, ring.JOINING, ring.ACTIVE, ring.JOINING, ring.ACTIVE}},
			},
			ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					makeWriteRequest(0, 1, 0, false, false, "foo0"),
					makeWriteRequest(0, 1, 0, false, false, "foo1"),
					makeWriteRequest(0, 1, 0, false, false, "foo2"),
					makeWriteRequest(0, 1, 0, false, false, "foo3"),
					makeWriteRequest(0, 1, 0, false, false, "foo4"),
				},
				"zone-b": {
					makeWriteRequest(0, 1, 0, false, false, "foo0"),
					makeWriteRequest(0, 1, 0, false, false, "foo1"),
					makeWriteRequest(0, 1, 0, false, false, "foo2"),
					makeWriteRequest(0, 1, 0, false, false, "foo3"),
					makeWriteRequest(0, 1, 0, false, false, "foo4"),
				},
			},
			preferZone:               []string{"zone-a"},
			minimizeIngesterRequests: true,
			matchers:                 []*labels.Matcher{selectAllSeriesMatcher},
			expectedResponse:         expectedResponse(0, 1, false, "foo0", "foo1", "foo2", "foo3", "foo4"),
			expectedQueriedIngesters: 2 /* zone-a ingesters (excluding JOINING one) */ + 3, /* zone-b ingesters as fallback for the JOINING ones */
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			t.Parallel()

			limits := prepareDefaultLimits()
			limits.IngestionPartitionsTenantShardSize = testData.shuffleShardSize

			cfg := prepConfig{
				numDistributors:      1,
				ingestStorageEnabled: true,
				ingesterStateByZone:  testData.ingesterStateByZone,
				ingesterDataByZone:   testData.ingesterDataByZone,
				ingesterDataTenantID: tenantID,
				queryDelay:           250 * time.Millisecond, // Give some time to start the calls to all ingesters before failures are received.
				replicationFactor:    1,                      // Ingest storage is not expected to use it.
				limits:               limits,
				configure: func(config *Config) {
					config.PreferAvailabilityZones = testData.preferZone
					config.MinimizeIngesterRequests = testData.minimizeIngesterRequests
				},
			}

			distributors, ingesters, distributorRegistries, _ := prepare(t, cfg)
			require.Len(t, distributors, 1)
			require.Len(t, distributorRegistries, 1)

			// Query ingesters.
			queryMetrics := stats.NewQueryMetrics(distributorRegistries[0])
			resp, err := distributors[0].QueryStream(ctx, queryMetrics, 0, 10, false, nil, testData.matchers...)

			if testData.expectedErr == nil {
				require.NoError(t, err)
			} else {
				assert.ErrorIs(t, err, testData.expectedErr)

				// Assert that downstream gRPC statuses are passed back upstream.
				_, expectedIsGRPC := grpcutil.ErrorToStatus(testData.expectedErr)
				if expectedIsGRPC {
					_, actualIsGRPC := grpcutil.ErrorToStatus(err)
					assert.True(t, actualIsGRPC, fmt.Sprintf("expected error to be a status error, but got: %T", err))
				}
			}

			responseMatrix, err := ingester_client.StreamingSeriesToMatrix(0, 5, resp.StreamingSeries)
			assert.NoError(t, err)
			assert.Equal(t, testData.expectedResponse.String(), responseMatrix.String())

			// Check how many ingesters have been queried.
			// Because we return immediately on failures, it might take some time for all ingester calls to register.
			test.Poll(t, 4*cfg.queryDelay, testData.expectedQueriedIngesters, func() any { return countMockIngestersCalls(ingesters, "QueryStream") })
		})
	}
}

func TestDistributor_QueryStream_InactivePartitionsLookback(t *testing.T) {
	const (
		tenantID       = "user"
		lookbackPeriod = 12 * time.Hour
	)

	selectAllSeriesMatcher := mustEqualMatcher("bar", "baz")

	shardingConfigs := map[string]struct {
		shuffleShardingEnabled bool
		tenantShardSize        int
	}{
		"shuffle sharding disabled": {
			shuffleShardingEnabled: false,
			tenantShardSize:        0,
		},
		"shuffle sharding enabled with shard size 0": {
			shuffleShardingEnabled: true,
			tenantShardSize:        0,
		},
		"shuffle sharding enabled with shard size > 0": {
			shuffleShardingEnabled: true,
			tenantShardSize:        10, // Larger than partition count, so all partitions are included.
		},
	}

	scenarios := map[string]struct {
		partition3InactiveSince time.Duration // How long ago partition 3 became inactive.
		partition3Healthy       bool          // Whether the ingester owning partition 3 is healthy.
		expectedPartitionIDs    []int         // Expected partition IDs returned by getIngesterReplicationSetsForQuery.
		expectQueryError        bool          // Whether the query should fail.
	}{
		"partition outside lookback with unhealthy ingester": {
			partition3InactiveSince: 24 * time.Hour, // Outside lookback (12h).
			partition3Healthy:       false,
			expectedPartitionIDs:    []int{0, 1, 2}, // Partition 3 excluded due to lookback.
			expectQueryError:        false,          // Query succeeds because partition 3 is not queried.
		},
		"partition outside lookback with healthy ingester": {
			partition3InactiveSince: 24 * time.Hour, // Outside lookback (12h).
			partition3Healthy:       true,
			expectedPartitionIDs:    []int{0, 1, 2}, // Partition 3 excluded due to lookback.
			expectQueryError:        false,          // Query succeeds, partition 3 is not queried anyway.
		},
		"partition within lookback with unhealthy ingester": {
			partition3InactiveSince: 1 * time.Hour, // Within lookback (12h).
			partition3Healthy:       false,
			expectedPartitionIDs:    []int{0, 1, 2, 3}, // Partition 3 included because within lookback.
			expectQueryError:        true,              // Query fails because partition 3 must be queried but ingester is unhealthy.
		},
	}

	for shardingName, shardingCfg := range shardingConfigs {
		for scenarioName, scenario := range scenarios {
			t.Run(fmt.Sprintf("%s, %s", shardingName, scenarioName), func(t *testing.T) {
				t.Parallel()

				ctx := user.InjectOrgID(context.Background(), tenantID)
				ctx = limiter.ContextWithNewUnlimitedMemoryConsumptionTracker(ctx)

				limits := prepareDefaultLimits()
				limits.IngestionPartitionsTenantShardSize = shardingCfg.tenantShardSize

				ingester3State := ingesterStateFailed
				var ingester3Data *mimirpb.WriteRequest
				if scenario.partition3Healthy {
					ingester3State = ingesterStateHappy
					ingester3Data = makeWriteRequest(0, 1, 0, false, false, "foo3")
				}

				cfg := prepConfig{
					numDistributors:         1,
					ingestStorageEnabled:    true,
					ingestStoragePartitions: 4,
					ingesterStateByZone: map[string]ingesterZoneState{
						"zone-a": {states: []ingesterState{ingesterStateHappy, ingesterStateHappy, ingesterStateHappy, ingester3State}},
					},
					ingesterDataByZone: map[string][]*mimirpb.WriteRequest{
						"zone-a": {
							makeWriteRequest(0, 1, 0, false, false, "foo0"), // Partition 0: active
							makeWriteRequest(0, 1, 0, false, false, "foo1"), // Partition 1: active
							makeWriteRequest(0, 1, 0, false, false, "foo2"), // Partition 2: inactive within lookback
							ingester3Data, // Partition 3: nil if ingester is unhealthy
						},
					},
					ingesterDataTenantID: tenantID,
					replicationFactor:    1,
					limits:               limits,
					configure: func(config *Config) {
						config.ShuffleShardingEnabled = shardingCfg.shuffleShardingEnabled
						config.IngestersLookbackPeriod = lookbackPeriod
					},
				}

				distributors, _, distributorRegistries, _ := prepare(t, cfg)
				require.Len(t, distributors, 1)
				d := distributors[0]

				// Wait for the partition ring to discover all 4 partitions.
				test.Poll(t, 5*time.Second, 4, func() interface{} {
					return d.partitionsRing.PartitionRing().PartitionsCount()
				})

				// Update partition states:
				// - Partitions 0, 1: Active (already active)
				// - Partition 2: Inactive since 1h (within lookback)
				// - Partition 3: Inactive since scenario.partition3InactiveSince
				now := time.Now()
				partitionsStore := d.cfg.DistributorRing.Common.KVStore.Mock.(*consul.Client).WithCodec(ring.GetPartitionRingCodec())
				err := partitionsStore.CAS(ctx, ingester.PartitionRingKey, func(in interface{}) (interface{}, bool, error) {
					desc := ring.GetOrCreatePartitionRingDesc(in)
					if _, err := desc.UpdatePartitionState(2, ring.PartitionInactive, now.Add(-1*time.Hour)); err != nil {
						return nil, false, err
					}
					if _, err := desc.UpdatePartitionState(3, ring.PartitionInactive, now.Add(-scenario.partition3InactiveSince)); err != nil {
						return nil, false, err
					}
					return desc, true, nil
				})
				require.NoError(t, err)

				// Wait for the partition ring watcher to see the updated states.
				test.Poll(t, 5*time.Second, 2, func() interface{} {
					return d.partitionsRing.PartitionRing().ActivePartitionsCount()
				})

				// Verify getIngesterReplicationSetsForQuery returns the expected partitions.
				replicationSets, err := d.getIngesterReplicationSetsForQuery(ctx)
				require.NoError(t, err)

				var actualPartitionIDs []int
				for _, rs := range replicationSets {
					require.NotEmpty(t, rs.Instances, "replication set should have at least one instance")
					partitionID, err := ingest.IngesterPartitionID(rs.Instances[0].Addr)
					require.NoError(t, err)
					actualPartitionIDs = append(actualPartitionIDs, int(partitionID))
				}
				sort.Ints(actualPartitionIDs)
				assert.Equal(t, scenario.expectedPartitionIDs, actualPartitionIDs)

				// Query ingesters.
				queryMetrics := stats.NewQueryMetrics(distributorRegistries[0])
				resp, err := d.QueryStream(ctx, queryMetrics, 0, 10, false, nil, selectAllSeriesMatcher)

				if scenario.expectQueryError {
					require.Error(t, err)
				} else {
					require.NoError(t, err)

					responseMatrix, err := ingester_client.StreamingSeriesToMatrix(0, 10, resp.StreamingSeries)
					require.NoError(t, err)

					// Build expected response based on which partitions are queried.
					var expectedLabels []string
					for _, partitionID := range scenario.expectedPartitionIDs {
						expectedLabels = append(expectedLabels, fmt.Sprintf("foo%d", partitionID))
					}
					expectedResponseMatrix := expectedResponse(0, 1, false, expectedLabels...)
					assert.Equal(t, expectedResponseMatrix.String(), responseMatrix.String())
				}
			})
		}
	}
}

func countMockIngestersCalls(ingesters []*mockIngester, name string) int {
	count := 0
	for _, i := range ingesters {
		count += i.countCalls(name)
	}
	return count
}
