// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/grafana/dskit/grpcutil"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/test"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ingester_client "github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/querier/stats"
)

func TestDistributor_QueryStream_ShouldSupportIngestStorage(t *testing.T) {
	const tenantID = "user"

	ctx := user.InjectOrgID(context.Background(), tenantID)
	selectAllSeriesMatcher := mustEqualMatcher("bar", "baz")

	tests := map[string]struct {
		ingesterStateByZone map[string]ingesterZoneState
		// ingesterDataPerZone:
		//   map[zone-a][0] -> ingester-zone-a-0 write request
		//   map[zone-a][1] -> ingester-zone-a-1 write request
		ingesterDataByZone map[string][]*mimirpb.WriteRequest

		preferZone               string
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
			preferZone:               "zone-a",
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
			preferZone:               "zone-a",
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
			preferZone:               "zone-a",
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
			preferZone:               "zone-a",
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
			preferZone:               "zone-a",
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
			preferZone:               "zone-a",
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
			preferZone:               "zone-a",
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
			preferZone:               "zone-a",
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
			preferZone:               "zone-a",
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
			preferZone:               "zone-a",
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
			preferZone:               "zone-a",
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
			preferZone:               "zone-a",
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
			preferZone:               "zone-a",
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
			preferZone:               "zone-a",
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
			preferZone:               "zone-a",
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
			preferZone:               "zone-a",
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
					config.PreferAvailabilityZone = testData.preferZone
					config.MinimizeIngesterRequests = testData.minimizeIngesterRequests
				},
			}

			distributors, ingesters, distributorRegistries, _ := prepare(t, cfg)
			require.Len(t, distributors, 1)
			require.Len(t, distributorRegistries, 1)

			// Query ingesters.
			queryMetrics := stats.NewQueryMetrics(distributorRegistries[0])
			resp, err := distributors[0].QueryStream(ctx, queryMetrics, 0, 10, testData.matchers...)

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

			var responseMatrix model.Matrix
			if len(resp.Chunkseries) == 0 {
				responseMatrix, err = ingester_client.StreamingSeriesToMatrix(0, 5, resp.StreamingSeries)
			} else {
				responseMatrix, err = ingester_client.TimeSeriesChunksToMatrix(0, 5, resp.Chunkseries)
			}
			assert.NoError(t, err)
			assert.Equal(t, testData.expectedResponse.String(), responseMatrix.String())

			// Check how many ingesters have been queried.
			// Because we return immediately on failures, it might take some time for all ingester calls to register.
			test.Poll(t, 4*cfg.queryDelay, testData.expectedQueriedIngesters, func() any { return countMockIngestersCalls(ingesters, "QueryStream") })
		})
	}
}

func countMockIngestersCalls(ingesters []*mockIngester, name string) int {
	count := 0
	for _, i := range ingesters {
		count += i.countCalls(name)
	}
	return count
}
