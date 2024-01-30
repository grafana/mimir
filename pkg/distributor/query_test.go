// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/distributor/query_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package distributor

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/grpcutil"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/test"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ingester_client "github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/util/limiter"
	"github.com/grafana/mimir/pkg/util/validation"
)

func TestDistributor_QueryStream_Partitions(t *testing.T) {
	t.Skip("this test is currently broken, needs to be properly fixed")

	const tenantID = "user"
	ctx := user.InjectOrgID(context.Background(), tenantID)
	selectAllSeriesMatcher := mustEqualMatcher("bar", "baz")

	type testCase struct {
		ingesterStateByZone map[string]ingesterZoneState
		// ingesterDataPerZone:
		//   map[zone-a][0] -> ingester-zone-a-0 write request
		//   map[zone-a][1] -> ingester-zone-a-1 write request
		dataPerIngesterZone map[string][]*mimirpb.WriteRequest

		querierZone              string
		shuffleShardSize         int
		useClassicRing           bool
		matchers                 []*labels.Matcher
		expectedResponse         model.Matrix
		expectedQueriedIngesters int
		expectedError            error
	}

	// We'll programmatically build the test cases now, as we want complete
	// coverage along quite a few different axis.
	testCases := map[string]testCase{
		"3 zones with 5 ingesters each": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 5, happyIngesters: 5},
				"zone-b": {numIngesters: 5, happyIngesters: 5},
				"zone-c": {numIngesters: 5, happyIngesters: 5},
			},
			dataPerIngesterZone: map[string][]*mimirpb.WriteRequest{
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
			querierZone:              "zone-a",
			matchers:                 []*labels.Matcher{selectAllSeriesMatcher},
			expectedResponse:         expectedResponse(0, 1, false, "foo0", "foo1", "foo2", "foo3", "foo4"),
			expectedQueriedIngesters: 5,
		},
		"2 zones with 5 ingesters each": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 5, happyIngesters: 5},
				"zone-b": {numIngesters: 5, happyIngesters: 5},
			},
			dataPerIngesterZone: map[string][]*mimirpb.WriteRequest{
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
			querierZone:              "zone-a",
			matchers:                 []*labels.Matcher{selectAllSeriesMatcher},
			expectedResponse:         expectedResponse(0, 1, false, "foo0", "foo1", "foo2", "foo3", "foo4"),
			expectedQueriedIngesters: 5,
		},
		"2 zones with 5 ingesters returning no series": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 5, happyIngesters: 5},
				"zone-b": {numIngesters: 5, happyIngesters: 5},
			},
			dataPerIngesterZone: map[string][]*mimirpb.WriteRequest{
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
			querierZone:              "zone-a",
			matchers:                 []*labels.Matcher{mustEqualMatcher("not", "found")},
			expectedResponse:         expectedResponse(0, 0, false),
			expectedQueriedIngesters: 5,
		},
		"2 zones with 2 failed ingesters in own zone": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 5, happyIngesters: 3},
				"zone-b": {numIngesters: 5, happyIngesters: 5},
			},
			dataPerIngesterZone: map[string][]*mimirpb.WriteRequest{
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
			querierZone:              "zone-a",
			matchers:                 []*labels.Matcher{selectAllSeriesMatcher},
			expectedResponse:         expectedResponse(0, 1, false, "foo0", "foo1", "foo2", "foo3", "foo4"),
			expectedQueriedIngesters: 5 /* zone-a */ + 2, /* the two failed fall back to zone-b */
		},
		"2 zones with 2 failed ingesters in other zone": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 5, happyIngesters: 5},
				"zone-b": {numIngesters: 5, happyIngesters: 3},
			},
			dataPerIngesterZone: map[string][]*mimirpb.WriteRequest{
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
			querierZone:              "zone-a",
			matchers:                 []*labels.Matcher{selectAllSeriesMatcher},
			expectedResponse:         expectedResponse(0, 1, false, "foo0", "foo1", "foo2", "foo3", "foo4"),
			expectedQueriedIngesters: 5, /* zone-a. zone-b isn't queried because of minimization */
		},
		"2 zones with 2 failed ingesters each, causes an error": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 5, happyIngesters: 3},
				"zone-b": {numIngesters: 5, happyIngesters: 3},
			},
			dataPerIngesterZone: map[string][]*mimirpb.WriteRequest{
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
			querierZone:              "zone-a",
			matchers:                 []*labels.Matcher{selectAllSeriesMatcher},
			expectedResponse:         expectedResponse(0, 0, false),
			expectedQueriedIngesters: 5 /* zone-a */ + 2, /* the two failed fall back to zone-b */
			expectedError:            errFail,
		},
		"2 zones with 1 failed ingester each, but they're different ingesters, so it all works out": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {states: []ingesterState{ingesterStateHappy, ingesterStateHappy, ingesterStateHappy, ingesterStateHappy, ingesterStateFailed}},
				"zone-b": {states: []ingesterState{ingesterStateHappy, ingesterStateHappy, ingesterStateHappy, ingesterStateFailed, ingesterStateHappy}},
			},
			dataPerIngesterZone: map[string][]*mimirpb.WriteRequest{
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
			querierZone:              "zone-a",
			matchers:                 []*labels.Matcher{selectAllSeriesMatcher},
			expectedResponse:         expectedResponse(0, 1, false, "foo0", "foo1", "foo2", "foo3", "foo4"),
			expectedQueriedIngesters: 5 /* zone-a */ + 1, /* fall back one failed request to zone-b */
		},
		"2 zones with shuffle sharding, but the failed ingesters are outside the shuffle shard": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {states: []ingesterState{ingesterStateHappy, ingesterStateFailed, ingesterStateFailed, ingesterStateHappy, ingesterStateHappy}},
				"zone-b": {states: []ingesterState{ingesterStateHappy, ingesterStateFailed, ingesterStateFailed, ingesterStateHappy, ingesterStateHappy}},
			},
			dataPerIngesterZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					makeWriteRequest(0, 1, 0, false, false, "foo0"),
					nil,
					nil,
					makeWriteRequest(0, 1, 0, false, false, "foo3"),
					makeWriteRequest(0, 1, 0, false, false, "foo4"),
				},
				"zone-b": {
					makeWriteRequest(0, 1, 0, false, false, "foo0"),
					nil,
					nil,
					makeWriteRequest(0, 1, 0, false, false, "foo3"),
					makeWriteRequest(0, 1, 0, false, false, "foo4"),
				},
			},
			shuffleShardSize:         2, // shuffle-sharding chooses partitions 0 and 4 for this tenant
			querierZone:              "zone-a",
			matchers:                 []*labels.Matcher{selectAllSeriesMatcher},
			expectedResponse:         expectedResponse(0, 1, false, "foo0", "foo4"),
			expectedQueriedIngesters: 2, /* zone-a only with shuffle-shard of 2 */
		},
		"2 zones with 1 inactive partition owner ": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {
					numIngesters: 5, happyIngesters: 5,
					partitionRingStates: []ring.InstanceState{ring.LEAVING, ring.ACTIVE, ring.ACTIVE, ring.ACTIVE, ring.ACTIVE},
				},
				"zone-b": {
					numIngesters: 5, happyIngesters: 5,
					partitionRingStates: []ring.InstanceState{ring.ACTIVE, ring.ACTIVE, ring.ACTIVE, ring.ACTIVE, ring.ACTIVE},
				},
			},
			dataPerIngesterZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					makeWriteRequest(0, 1, 0, false, false, "metric_should_not_be_queried"),
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
			querierZone:              "zone-a",
			matchers:                 []*labels.Matcher{selectAllSeriesMatcher},
			expectedResponse:         expectedResponse(0, 1, false, "foo0", "foo1", "foo2", "foo3", "foo4"),
			expectedQueriedIngesters: 4 /* zone-a for the ACTIVE instances */ + 1, /* zone-b for the copy of ingester 1 */
		},
		"querying both rings: 2 zones with 5 ingesters each": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 5, happyIngesters: 5},
				"zone-b": {numIngesters: 5, happyIngesters: 5},
			},
			dataPerIngesterZone: map[string][]*mimirpb.WriteRequest{
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
			useClassicRing:           true,
			querierZone:              "zone-a",
			matchers:                 []*labels.Matcher{selectAllSeriesMatcher},
			expectedResponse:         expectedResponse(0, 1, false, "foo0", "foo1", "foo2", "foo3", "foo4"),
			expectedQueriedIngesters: 5 /* zone-a partition ring instances */ + 5, /* either zone querying again for classic ring */
		},
		"querying both rings: 2 zones with 5 ingesters each; one ingester is LEAVING in partition ring": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {
					numIngesters: 5, happyIngesters: 5,
					partitionRingStates: []ring.InstanceState{ring.LEAVING, ring.ACTIVE, ring.ACTIVE, ring.ACTIVE, ring.ACTIVE},
				},
				"zone-b": {numIngesters: 5, happyIngesters: 5},
			},
			dataPerIngesterZone: map[string][]*mimirpb.WriteRequest{
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
			useClassicRing:           true,
			querierZone:              "zone-a",
			matchers:                 []*labels.Matcher{selectAllSeriesMatcher},
			expectedResponse:         expectedResponse(0, 1, false, "foo0", "foo1", "foo2", "foo3", "foo4"),
			expectedQueriedIngesters: 5 /* zone-b partition ring instances */ + 5, /* either zone querying again for classic ring */
		},
		"querying both rings: 2 zones with 5 ingesters each, one ingester is LEAVING in classic ring": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {
					numIngesters: 5, happyIngesters: 5,
					ringStates: []ring.InstanceState{ring.LEAVING, ring.ACTIVE, ring.ACTIVE, ring.ACTIVE, ring.ACTIVE},
				},
				"zone-b": {numIngesters: 5, happyIngesters: 5},
			},
			dataPerIngesterZone: map[string][]*mimirpb.WriteRequest{
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
			useClassicRing:           true,
			querierZone:              "zone-a",
			matchers:                 []*labels.Matcher{selectAllSeriesMatcher},
			expectedResponse:         expectedResponse(0, 1, false, "foo0", "foo1", "foo2", "foo3", "foo4"),
			expectedQueriedIngesters: 5 /* zone-b partition ring instances */ + 5, /* zone-b partition ring instances */
		},
		"querying both rings: 2 zones with 5 ingesters each; LEAVING ingesters in both rings in both zones": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {
					numIngesters: 5, happyIngesters: 5,
					ringStates:          []ring.InstanceState{ring.LEAVING, ring.ACTIVE, ring.ACTIVE, ring.ACTIVE, ring.ACTIVE},
					partitionRingStates: []ring.InstanceState{ring.LEAVING, ring.ACTIVE, ring.ACTIVE, ring.ACTIVE, ring.ACTIVE},
				},
				"zone-b": {
					numIngesters: 5, happyIngesters: 5,
					ringStates:          []ring.InstanceState{ring.LEAVING, ring.ACTIVE, ring.ACTIVE, ring.ACTIVE, ring.ACTIVE},
					partitionRingStates: []ring.InstanceState{ring.LEAVING, ring.ACTIVE, ring.ACTIVE, ring.ACTIVE, ring.ACTIVE},
				},
			},
			dataPerIngesterZone: map[string][]*mimirpb.WriteRequest{
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
			useClassicRing:           true,
			querierZone:              "zone-a",
			matchers:                 []*labels.Matcher{selectAllSeriesMatcher},
			expectedResponse:         expectedResponse(0, 0, false),
			expectedQueriedIngesters: 0, /* since both zones are unhealthy no requests are even attempted */
			expectedError:            ring.ErrTooManyUnhealthyInstances,
		},
		"querying both rings: 2 zones with 5 ingesters each; unhappy ingesters in both zones": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {numIngesters: 5, happyIngesters: 4},
				"zone-b": {numIngesters: 5, happyIngesters: 4},
			},
			dataPerIngesterZone: map[string][]*mimirpb.WriteRequest{
				"zone-a": {
					makeWriteRequest(0, 1, 0, false, false, "foo0"),
					makeWriteRequest(0, 1, 0, false, false, "foo1"),
					makeWriteRequest(0, 1, 0, false, false, "foo2"),
					makeWriteRequest(0, 1, 0, false, false, "foo3"),
				},
				"zone-b": {
					makeWriteRequest(0, 1, 0, false, false, "foo0"),
					makeWriteRequest(0, 1, 0, false, false, "foo1"),
					makeWriteRequest(0, 1, 0, false, false, "foo2"),
					makeWriteRequest(0, 1, 0, false, false, "foo3"),
				},
			},
			useClassicRing:   true,
			querierZone:      "zone-a",
			matchers:         []*labels.Matcher{selectAllSeriesMatcher},
			expectedResponse: expectedResponse(0, 0, false),
			expectedQueriedIngesters: 0 +
				5 /* partitions ring queried zone-a once */ + 1 /* partitions ring fell back on one instance in zone-b */ +
				5 /* classic ring queried either zone once */ + 5, /* classic ring fell back to the other zone */
			expectedError: errFail,
		},
		"querying both rings: two ingesters LEAVING in classic ring": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {
					numIngesters: 5, happyIngesters: 5,
					ringStates: []ring.InstanceState{ring.LEAVING, ring.ACTIVE, ring.ACTIVE, ring.ACTIVE, ring.ACTIVE},
				},
				"zone-b": {
					numIngesters: 5, happyIngesters: 5,
					ringStates: []ring.InstanceState{ring.LEAVING, ring.ACTIVE, ring.ACTIVE, ring.ACTIVE, ring.ACTIVE},
				},
			},
			dataPerIngesterZone: map[string][]*mimirpb.WriteRequest{
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
			useClassicRing:           true,
			querierZone:              "zone-a",
			matchers:                 []*labels.Matcher{selectAllSeriesMatcher},
			expectedResponse:         expectedResponse(0, 0, false),
			expectedQueriedIngesters: 0, /* we couldn't get a subring for the partitions ring, so querying didn't even start */
			expectedError:            ring.ErrTooManyUnhealthyInstances,
		},
		"querying both rings: two ingesters LEAVING in partition ring": {
			ingesterStateByZone: map[string]ingesterZoneState{
				"zone-a": {
					numIngesters: 5, happyIngesters: 5,
					partitionRingStates: []ring.InstanceState{ring.LEAVING, ring.ACTIVE, ring.ACTIVE, ring.ACTIVE, ring.ACTIVE},
				},
				"zone-b": {
					numIngesters: 5, happyIngesters: 5,
					partitionRingStates: []ring.InstanceState{ring.LEAVING, ring.ACTIVE, ring.ACTIVE, ring.ACTIVE, ring.ACTIVE},
				},
			},
			dataPerIngesterZone: map[string][]*mimirpb.WriteRequest{
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
			useClassicRing:           true,
			querierZone:              "zone-a",
			matchers:                 []*labels.Matcher{selectAllSeriesMatcher},
			expectedResponse:         expectedResponse(0, 0, false),
			expectedQueriedIngesters: 0, /* we couldn't get a subring for the partitions ring, so querying didn't even start */
			expectedError:            ring.ErrTooManyUnhealthyInstances,
		},
	}

	for tcName, tc := range testCases {
		tc := tc

		t.Run(tcName, func(t *testing.T) {
			t.Parallel()
			cfg := prepConfig{
				numDistributors:      1,
				useIngestStorage:     true,
				ingesterStateByZone:  tc.ingesterStateByZone,
				ingesterDataPerZone:  tc.dataPerIngesterZone,
				ingesterDataTenantID: tenantID,
				queryDelay:           time.Millisecond, // give the chance of the ring to start the calls to all ingesters before failures surface
				replicationFactor:    len(tc.ingesterStateByZone),
				shuffleShardSize:     tc.shuffleShardSize,
				configure: func(config *Config) {
					config.IngestStorageConfig.Zone = tc.querierZone
					config.MinimizeIngesterRequests = true
					config.IngestStorageConfig.UseClassicRing = tc.useClassicRing
				},
			}

			cfg.shuffleShardSize = tc.shuffleShardSize

			ds, ingesters, reg := prepare(t, cfg)

			queryMetrics := stats.NewQueryMetrics(reg[0])
			resp, err := ds[0].QueryStream(ctx, queryMetrics, 0, 10, tc.matchers...)

			if tc.expectedError == nil {
				require.NoError(t, err)
			} else {
				assert.ErrorIs(t, err, tc.expectedError)

				// Assert that downstream gRPC statuses are passed back upstream.
				_, expectedIsGRPC := grpcutil.ErrorToStatus(tc.expectedError)
				if expectedIsGRPC {
					_, actualIsGRPC := grpcutil.ErrorToStatus(err)
					assert.True(t, actualIsGRPC, fmt.Sprintf("expected error to be a status error, but got: %T", err))
				}
			}

			var responseMatrix model.Matrix
			if len(resp.Chunkseries) == 0 {
				responseMatrix, err = ingester_client.TimeSeriesChunksToMatrix(0, 5, nil)
			} else {
				responseMatrix, err = ingester_client.TimeSeriesChunksToMatrix(0, 5, resp.Chunkseries)
			}
			assert.NoError(t, err)
			assert.Equal(t, tc.expectedResponse.String(), responseMatrix.String())

			// Check how many ingesters have been queried.
			// Because we return immediately on failures, it might take some time for all ingester calls to register.
			test.Poll(t, 100*time.Millisecond, tc.expectedQueriedIngesters, func() any { return countMockIngestersCalls(ingesters, "QueryStream") })
		})
	}
}

func TestDistributor_QueryStream_ShouldReturnErrorIfMaxChunksPerQueryLimitIsReached(t *testing.T) {
	const limit = 30 // Chunks are duplicated due to replication factor.

	testCases := map[string]struct {
		maxChunksLimit          int
		maxEstimatedChunksLimit int
		expectedError           string
	}{
		"max chunks limit": {
			maxChunksLimit: limit,
			expectedError:  "the query exceeded the maximum number of chunks",
		},
		"max estimated chunks limit": {
			maxEstimatedChunksLimit: limit,
			expectedError:           "the estimated number of chunks for the query exceeded the maximum allowed",
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			for _, streamingEnabled := range []bool{true, false} {
				t.Run(fmt.Sprintf("streaming enabled: %v", streamingEnabled), func(t *testing.T) {
					for _, minimizeIngesterRequests := range []bool{true, false} {
						t.Run(fmt.Sprintf("request minimization enabled: %v", minimizeIngesterRequests), func(t *testing.T) {
							userCtx := user.InjectOrgID(context.Background(), "user")
							limits := &validation.Limits{}
							flagext.DefaultValues(limits)
							limits.MaxChunksPerQuery = limit

							// Prepare distributors.
							ds, ingesters, reg := prepare(t, prepConfig{
								numIngesters:    3,
								happyIngesters:  3,
								numDistributors: 1,
								limits:          limits,
								configure: func(config *Config) {
									config.PreferStreamingChunksFromIngesters = streamingEnabled
									config.MinimizeIngesterRequests = minimizeIngesterRequests
								},
							})

							// Push a number of series below the max chunks limit. Each series has 1 sample,
							// so expect 1 chunk per series when querying back.
							initialSeries := limit / 3
							writeReq := makeWriteRequest(0, initialSeries, 0, false, false, "foo")
							writeRes, err := ds[0].Push(userCtx, writeReq)
							require.Equal(t, &mimirpb.WriteResponse{}, writeRes)
							require.Nil(t, err)

							allSeriesMatchers := []*labels.Matcher{
								labels.MustNewMatcher(labels.MatchRegexp, model.MetricNameLabel, ".+"),
							}

							queryCtx := limiter.AddQueryLimiterToContext(userCtx, limiter.NewQueryLimiter(0, 0, testCase.maxChunksLimit, testCase.maxEstimatedChunksLimit, stats.NewQueryMetrics(prometheus.NewPedanticRegistry())))
							queryMetrics := stats.NewQueryMetrics(reg[0])

							// Since the number of series (and thus chunks) is equal to the limit (but doesn't
							// exceed it), we expect a query running on all series to succeed.
							queryRes, err := ds[0].QueryStream(queryCtx, queryMetrics, math.MinInt32, math.MaxInt32, allSeriesMatchers...)
							require.NoError(t, err)

							if streamingEnabled {
								require.Len(t, queryRes.StreamingSeries, initialSeries)
							} else {
								require.Len(t, queryRes.Chunkseries, initialSeries)
							}

							firstRequestIngesterQueryCount := countCalls(ingesters, "QueryStream")

							if minimizeIngesterRequests {
								require.LessOrEqual(t, firstRequestIngesterQueryCount, 2, "should not call third ingester if request minimisation is enabled and first two ingesters return a successful response")
							}

							// Push more series to exceed the limit once we'll query back all series.
							writeReq = &mimirpb.WriteRequest{}
							for i := 0; i < limit; i++ {
								writeReq.Timeseries = append(writeReq.Timeseries,
									makeWriteRequestTimeseries([]mimirpb.LabelAdapter{{Name: model.MetricNameLabel, Value: fmt.Sprintf("another_series_%d", i)}}, 0, 0),
								)
							}

							writeRes, err = ds[0].Push(userCtx, writeReq)
							require.Equal(t, &mimirpb.WriteResponse{}, writeRes)
							require.Nil(t, err)

							// Reset the query limiter in the context.
							queryCtx = limiter.AddQueryLimiterToContext(userCtx, limiter.NewQueryLimiter(0, 0, testCase.maxChunksLimit, testCase.maxEstimatedChunksLimit, stats.NewQueryMetrics(prometheus.NewPedanticRegistry())))

							// Since the number of series (and thus chunks) is exceeding to the limit, we expect
							// a query running on all series to fail.
							_, err = ds[0].QueryStream(queryCtx, queryMetrics, math.MinInt32, math.MaxInt32, allSeriesMatchers...)
							require.Error(t, err)
							require.ErrorContains(t, err, testCase.expectedError)

							if minimizeIngesterRequests {
								secondRequestIngesterQueryCallCount := countCalls(ingesters, "QueryStream") - firstRequestIngesterQueryCount
								require.LessOrEqual(t, secondRequestIngesterQueryCallCount, 2, "should not call third ingester if request minimisation is enabled and either of first two ingesters fail with limits error")
							}
						})
					}
				})
			}
		})
	}
}

func TestDistributor_QueryStream_ShouldReturnErrorIfMaxSeriesPerQueryLimitIsReached(t *testing.T) {
	const maxSeriesLimit = 10

	for _, minimizeIngesterRequests := range []bool{true, false} {
		t.Run(fmt.Sprintf("request minimization enabled: %v", minimizeIngesterRequests), func(t *testing.T) {
			userCtx := user.InjectOrgID(context.Background(), "user")
			limits := &validation.Limits{}
			flagext.DefaultValues(limits)

			// Prepare distributors.
			ds, ingesters, reg := prepare(t, prepConfig{
				numIngesters:    3,
				happyIngesters:  3,
				numDistributors: 1,
				limits:          limits,
				configure: func(config *Config) {
					config.MinimizeIngesterRequests = minimizeIngesterRequests
				},
			})

			// Push a number of series below the max series limit.
			initialSeries := maxSeriesLimit
			writeReq := makeWriteRequest(0, initialSeries, 0, false, true, "foo")
			writeRes, err := ds[0].Push(userCtx, writeReq)
			assert.Equal(t, &mimirpb.WriteResponse{}, writeRes)
			assert.Nil(t, err)

			allSeriesMatchers := []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchRegexp, model.MetricNameLabel, ".+"),
			}

			queryMetrics := stats.NewQueryMetrics(reg[0])

			// Since the number of series is equal to the limit (but doesn't
			// exceed it), we expect a query running on all series to succeed.
			queryCtx := limiter.AddQueryLimiterToContext(userCtx, limiter.NewQueryLimiter(maxSeriesLimit, 0, 0, 0, stats.NewQueryMetrics(prometheus.NewPedanticRegistry())))
			queryRes, err := ds[0].QueryStream(queryCtx, queryMetrics, math.MinInt32, math.MaxInt32, allSeriesMatchers...)
			require.NoError(t, err)
			assert.Len(t, queryRes.Chunkseries, initialSeries)

			firstRequestIngesterQueryCount := countCalls(ingesters, "QueryStream")

			if minimizeIngesterRequests {
				require.LessOrEqual(t, firstRequestIngesterQueryCount, 2, "should not call third ingester if request minimisation is enabled and first two ingesters return a successful response")
			}

			// Push more series to exceed the limit once we'll query back all series.
			writeReq = &mimirpb.WriteRequest{}
			writeReq.Timeseries = append(writeReq.Timeseries,
				makeWriteRequestTimeseries([]mimirpb.LabelAdapter{{Name: model.MetricNameLabel, Value: "another_series"}}, 0, 0),
			)

			writeRes, err = ds[0].Push(userCtx, writeReq)
			assert.Equal(t, &mimirpb.WriteResponse{}, writeRes)
			assert.Nil(t, err)

			// Reset the query limiter in the context.
			queryCtx = limiter.AddQueryLimiterToContext(userCtx, limiter.NewQueryLimiter(maxSeriesLimit, 0, 0, 0, stats.NewQueryMetrics(prometheus.NewPedanticRegistry())))

			// Since the number of series is exceeding the limit, we expect
			// a query running on all series to fail.
			_, err = ds[0].QueryStream(queryCtx, queryMetrics, math.MinInt32, math.MaxInt32, allSeriesMatchers...)
			require.Error(t, err)
			assert.ErrorContains(t, err, "the query exceeded the maximum number of series")

			if minimizeIngesterRequests {
				secondRequestIngesterQueryCallCount := countCalls(ingesters, "QueryStream") - firstRequestIngesterQueryCount
				require.LessOrEqual(t, secondRequestIngesterQueryCallCount, 2, "should not call third ingester if request minimisation is enabled and either of first two ingesters fail with limits error")
			}
		})
	}
}

func TestDistributor_QueryStream_ShouldReturnErrorIfMaxChunkBytesPerQueryLimitIsReached(t *testing.T) {
	const seriesToAdd = 10

	ctx := user.InjectOrgID(context.Background(), "user")
	limits := &validation.Limits{}
	flagext.DefaultValues(limits)

	// Prepare distributors.
	// Use replication factor of 1 so that we always wait the response from all ingesters.
	// This guarantees us to always read the same chunks and have a stable test.
	ds, _, reg := prepare(t, prepConfig{
		numIngesters:      3,
		happyIngesters:    3,
		numDistributors:   1,
		limits:            limits,
		replicationFactor: 1,
	})

	allSeriesMatchers := []*labels.Matcher{
		labels.MustNewMatcher(labels.MatchRegexp, model.MetricNameLabel, ".+"),
	}
	// Push a single series to allow us to calculate the chunk size to calculate the limit for the test.
	writeReq := &mimirpb.WriteRequest{}
	writeReq.Timeseries = append(writeReq.Timeseries,
		makeWriteRequestTimeseries([]mimirpb.LabelAdapter{{Name: model.MetricNameLabel, Value: "another_series"}}, 0, 0),
	)
	writeRes, err := ds[0].Push(ctx, writeReq)
	assert.Equal(t, &mimirpb.WriteResponse{}, writeRes)
	assert.Nil(t, err)

	queryMetrics := stats.NewQueryMetrics(reg[0])
	chunkSizeResponse, err := ds[0].QueryStream(ctx, queryMetrics, math.MinInt32, math.MaxInt32, allSeriesMatchers...)
	require.NoError(t, err)

	// Use the resulting chunks size to calculate the limit as (series to add + our test series) * the response chunk size.
	responseChunkSize := ingester_client.ChunksSize(chunkSizeResponse.Chunkseries)
	maxBytesLimit := (seriesToAdd) * responseChunkSize

	// Update the limiter with the calculated limits.
	ctx = limiter.AddQueryLimiterToContext(ctx, limiter.NewQueryLimiter(0, maxBytesLimit, 0, 0, stats.NewQueryMetrics(prometheus.NewPedanticRegistry())))

	// Push a number of series below the max chunk bytes limit. Subtract one for the series added above.
	writeReq = makeWriteRequest(0, seriesToAdd-1, 0, false, false, "foo")
	writeRes, err = ds[0].Push(ctx, writeReq)
	assert.Equal(t, &mimirpb.WriteResponse{}, writeRes)
	assert.Nil(t, err)

	// Since the number of chunk bytes is equal to the limit (but doesn't
	// exceed it), we expect a query running on all series to succeed.
	queryRes, err := ds[0].QueryStream(ctx, queryMetrics, math.MinInt32, math.MaxInt32, allSeriesMatchers...)
	require.NoError(t, err)
	assert.Len(t, queryRes.Chunkseries, seriesToAdd)

	// Push another series to exceed the chunk bytes limit once we'll query back all series.
	writeReq = &mimirpb.WriteRequest{}
	writeReq.Timeseries = append(writeReq.Timeseries,
		makeWriteRequestTimeseries([]mimirpb.LabelAdapter{{Name: model.MetricNameLabel, Value: "another_series_1"}}, 0, 0),
	)

	writeRes, err = ds[0].Push(ctx, writeReq)
	assert.Equal(t, &mimirpb.WriteResponse{}, writeRes)
	assert.Nil(t, err)

	// Since the aggregated chunk size is exceeding the limit, we expect
	// a query running on all series to fail.
	_, err = ds[0].QueryStream(ctx, queryMetrics, math.MinInt32, math.MaxInt32, allSeriesMatchers...)
	require.Error(t, err)
	assert.Equal(t, err, limiter.NewMaxChunkBytesHitLimitError(uint64(maxBytesLimit)))
}

func TestMergeSamplesIntoFirstDuplicates(t *testing.T) {
	a := []mimirpb.Sample{
		{Value: 1.084537996, TimestampMs: 1583946732744},
		{Value: 1.086111723, TimestampMs: 1583946750366},
		{Value: 1.086111723, TimestampMs: 1583946768623},
		{Value: 1.087776094, TimestampMs: 1583946795182},
		{Value: 1.089301187, TimestampMs: 1583946810018},
		{Value: 1.089301187, TimestampMs: 1583946825064},
		{Value: 1.089301187, TimestampMs: 1583946835547},
		{Value: 1.090722985, TimestampMs: 1583946846629},
		{Value: 1.090722985, TimestampMs: 1583946857608},
		{Value: 1.092038719, TimestampMs: 1583946882302},
	}

	b := []mimirpb.Sample{
		{Value: 1.084537996, TimestampMs: 1583946732744},
		{Value: 1.086111723, TimestampMs: 1583946750366},
		{Value: 1.086111723, TimestampMs: 1583946768623},
		{Value: 1.087776094, TimestampMs: 1583946795182},
		{Value: 1.089301187, TimestampMs: 1583946810018},
		{Value: 1.089301187, TimestampMs: 1583946825064},
		{Value: 1.089301187, TimestampMs: 1583946835547},
		{Value: 1.090722985, TimestampMs: 1583946846629},
		{Value: 1.090722985, TimestampMs: 1583946857608},
		{Value: 1.092038719, TimestampMs: 1583946882302},
	}

	a = mergeSamples(a, b)

	// should be the same
	require.Equal(t, a, b)
}

func TestMergeSamplesIntoFirst(t *testing.T) {
	a := []mimirpb.Sample{
		{Value: 1, TimestampMs: 10},
		{Value: 2, TimestampMs: 20},
		{Value: 3, TimestampMs: 30},
		{Value: 4, TimestampMs: 40},
		{Value: 5, TimestampMs: 45},
		{Value: 5, TimestampMs: 50},
	}

	b := []mimirpb.Sample{
		{Value: 1, TimestampMs: 5},
		{Value: 2, TimestampMs: 15},
		{Value: 3, TimestampMs: 25},
		{Value: 3, TimestampMs: 30},
		{Value: 4, TimestampMs: 35},
		{Value: 5, TimestampMs: 45},
		{Value: 6, TimestampMs: 55},
	}

	a = mergeSamples(a, b)

	require.Equal(t, []mimirpb.Sample{
		{Value: 1, TimestampMs: 5},
		{Value: 1, TimestampMs: 10},
		{Value: 2, TimestampMs: 15},
		{Value: 2, TimestampMs: 20},
		{Value: 3, TimestampMs: 25},
		{Value: 3, TimestampMs: 30},
		{Value: 4, TimestampMs: 35},
		{Value: 4, TimestampMs: 40},
		{Value: 5, TimestampMs: 45},
		{Value: 5, TimestampMs: 50},
		{Value: 6, TimestampMs: 55},
	}, a)
}

func TestMergeSamplesIntoFirstNilA(t *testing.T) {
	b := []mimirpb.Sample{
		{Value: 1, TimestampMs: 5},
		{Value: 2, TimestampMs: 15},
		{Value: 3, TimestampMs: 25},
		{Value: 4, TimestampMs: 35},
		{Value: 5, TimestampMs: 45},
		{Value: 6, TimestampMs: 55},
	}

	a := mergeSamples(nil, b)

	require.Equal(t, b, a)
}

func TestMergeSamplesIntoFirstNilB(t *testing.T) {
	a := []mimirpb.Sample{
		{Value: 1, TimestampMs: 10},
		{Value: 2, TimestampMs: 20},
		{Value: 3, TimestampMs: 30},
		{Value: 4, TimestampMs: 40},
		{Value: 5, TimestampMs: 50},
	}

	b := mergeSamples(a, nil)

	require.Equal(t, b, a)
}

func TestMergeExemplars(t *testing.T) {
	now := timestamp.FromTime(time.Now())
	exemplar1 := mimirpb.Exemplar{Labels: mimirpb.FromLabelsToLabelAdapters(labels.FromStrings("traceID", "trace-1")), TimestampMs: now, Value: 1}
	exemplar2 := mimirpb.Exemplar{Labels: mimirpb.FromLabelsToLabelAdapters(labels.FromStrings("traceID", "trace-2")), TimestampMs: now + 1, Value: 2}
	exemplar3 := mimirpb.Exemplar{Labels: mimirpb.FromLabelsToLabelAdapters(labels.FromStrings("traceID", "trace-3")), TimestampMs: now + 4, Value: 3}
	exemplar4 := mimirpb.Exemplar{Labels: mimirpb.FromLabelsToLabelAdapters(labels.FromStrings("traceID", "trace-4")), TimestampMs: now + 8, Value: 7}
	exemplar5 := mimirpb.Exemplar{Labels: mimirpb.FromLabelsToLabelAdapters(labels.FromStrings("traceID", "trace-4")), TimestampMs: now, Value: 7}
	labels1 := []mimirpb.LabelAdapter{{Name: "label1", Value: "foo1"}}
	labels2 := []mimirpb.LabelAdapter{{Name: "label1", Value: "foo2"}}

	for i, c := range []struct {
		seriesA       []mimirpb.TimeSeries
		seriesB       []mimirpb.TimeSeries
		expected      []mimirpb.TimeSeries
		nonReversible bool
	}{
		{
			seriesA:  []mimirpb.TimeSeries{{Labels: labels1, Exemplars: []mimirpb.Exemplar{}}},
			seriesB:  []mimirpb.TimeSeries{{Labels: labels1, Exemplars: []mimirpb.Exemplar{}}},
			expected: []mimirpb.TimeSeries{{Labels: labels1, Exemplars: []mimirpb.Exemplar{}}},
		},
		{
			seriesA:  []mimirpb.TimeSeries{{Labels: labels1, Exemplars: []mimirpb.Exemplar{exemplar1}}},
			seriesB:  []mimirpb.TimeSeries{{Labels: labels1, Exemplars: []mimirpb.Exemplar{}}},
			expected: []mimirpb.TimeSeries{{Labels: labels1, Exemplars: []mimirpb.Exemplar{exemplar1}}},
		},
		{
			seriesA:  []mimirpb.TimeSeries{{Labels: labels1, Exemplars: []mimirpb.Exemplar{exemplar1}}},
			seriesB:  []mimirpb.TimeSeries{{Labels: labels1, Exemplars: []mimirpb.Exemplar{exemplar1}}},
			expected: []mimirpb.TimeSeries{{Labels: labels1, Exemplars: []mimirpb.Exemplar{exemplar1}}},
		},
		{
			seriesA:  []mimirpb.TimeSeries{{Labels: labels1, Exemplars: []mimirpb.Exemplar{exemplar1, exemplar2, exemplar3}}},
			seriesB:  []mimirpb.TimeSeries{{Labels: labels1, Exemplars: []mimirpb.Exemplar{exemplar1, exemplar3, exemplar4}}},
			expected: []mimirpb.TimeSeries{{Labels: labels1, Exemplars: []mimirpb.Exemplar{exemplar1, exemplar2, exemplar3, exemplar4}}},
		},
		{ // Ensure that when there are exemplars with duplicate timestamps, the first one wins.
			seriesA:       []mimirpb.TimeSeries{{Labels: labels1, Exemplars: []mimirpb.Exemplar{exemplar1, exemplar2, exemplar3}}},
			seriesB:       []mimirpb.TimeSeries{{Labels: labels1, Exemplars: []mimirpb.Exemplar{exemplar5, exemplar3, exemplar4}}},
			expected:      []mimirpb.TimeSeries{{Labels: labels1, Exemplars: []mimirpb.Exemplar{exemplar1, exemplar2, exemplar3, exemplar4}}},
			nonReversible: true,
		},
		{ // Disjoint exemplars on two different series.
			seriesA: []mimirpb.TimeSeries{{Labels: labels1, Exemplars: []mimirpb.Exemplar{exemplar1, exemplar2}}},
			seriesB: []mimirpb.TimeSeries{{Labels: labels2, Exemplars: []mimirpb.Exemplar{exemplar3, exemplar4}}},
			expected: []mimirpb.TimeSeries{
				{Labels: labels1, Exemplars: []mimirpb.Exemplar{exemplar1, exemplar2}},
				{Labels: labels2, Exemplars: []mimirpb.Exemplar{exemplar3, exemplar4}}},
		},
		{ // Second input adds to first on one series.
			seriesA: []mimirpb.TimeSeries{
				{Labels: labels1, Exemplars: []mimirpb.Exemplar{exemplar1, exemplar2}},
				{Labels: labels2, Exemplars: []mimirpb.Exemplar{exemplar3}}},
			seriesB: []mimirpb.TimeSeries{{Labels: labels2, Exemplars: []mimirpb.Exemplar{exemplar4}}},
			expected: []mimirpb.TimeSeries{
				{Labels: labels1, Exemplars: []mimirpb.Exemplar{exemplar1, exemplar2}},
				{Labels: labels2, Exemplars: []mimirpb.Exemplar{exemplar3, exemplar4}}},
		},
	} {
		t.Run(fmt.Sprint("test", i), func(t *testing.T) {
			rA := &ingester_client.ExemplarQueryResponse{Timeseries: c.seriesA}
			rB := &ingester_client.ExemplarQueryResponse{Timeseries: c.seriesB}
			e := mergeExemplarQueryResponses([]*ingester_client.ExemplarQueryResponse{rA, rB})
			require.Equal(t, c.expected, e.Timeseries)
			if !c.nonReversible {
				// Check the other way round too
				e = mergeExemplarQueryResponses([]*ingester_client.ExemplarQueryResponse{rB, rA})
				require.Equal(t, c.expected, e.Timeseries)
			}
		})
	}
}

func makeExemplarQueryResponse(numSeries int) *ingester_client.ExemplarQueryResponse {
	now := time.Now()
	ts := make([]mimirpb.TimeSeries, numSeries)
	for i := 0; i < numSeries; i++ {
		lbls := labels.NewBuilder(labels.EmptyLabels())
		lbls.Set(model.MetricNameLabel, "foo")
		for i := 0; i < 10; i++ {
			lbls.Set(fmt.Sprintf("name_%d", i), fmt.Sprintf("value_%d_%d", i, rand.Intn(10)))
		}
		ts[i].Labels = mimirpb.FromLabelsToLabelAdapters(lbls.Labels())
		ts[i].Exemplars = []mimirpb.Exemplar{{
			Labels:      []mimirpb.LabelAdapter{{Name: "traceid", Value: "trace1"}},
			Value:       float64(i),
			TimestampMs: now.Add(time.Hour).UnixNano() / int64(time.Millisecond),
		}}
	}

	return &ingester_client.ExemplarQueryResponse{Timeseries: ts}
}

func BenchmarkMergeExemplars(b *testing.B) {
	input := makeExemplarQueryResponse(1000)

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		// Merge input with itself three times
		mergeExemplarQueryResponses([]*ingester_client.ExemplarQueryResponse{input, input, input})
	}
}

func TestMergingAndSortingSeries(t *testing.T) {
	ingester1 := &ingester_client.SeriesChunksStreamReader{}
	ingester2 := &ingester_client.SeriesChunksStreamReader{}
	ingester3 := &ingester_client.SeriesChunksStreamReader{}

	testCases := map[string]struct {
		results  []ingesterQueryResult
		expected []ingester_client.StreamingSeries
	}{
		"no ingesters": {
			results:  []ingesterQueryResult{},
			expected: []ingester_client.StreamingSeries{},
		},
		"single ingester, no streaming series": {
			results: []ingesterQueryResult{
				{},
			},
			expected: []ingester_client.StreamingSeries{},
		},
		"single ingester, no series": {
			results: []ingesterQueryResult{
				{streamingSeries: seriesChunksStream{StreamReader: ingester1, Series: []labels.Labels{}}},
			},
			expected: []ingester_client.StreamingSeries{},
		},
		"single ingester, single series": {
			results: []ingesterQueryResult{
				{streamingSeries: seriesChunksStream{StreamReader: ingester1, Series: []labels.Labels{labels.FromStrings("some-label", "some-value")}}},
			},
			expected: []ingester_client.StreamingSeries{
				{
					Labels: labels.FromStrings("some-label", "some-value"),
					Sources: []ingester_client.StreamingSeriesSource{
						{StreamReader: ingester1, SeriesIndex: 0},
					},
				},
			},
		},
		"multiple ingesters, each with single series": {
			results: []ingesterQueryResult{
				{streamingSeries: seriesChunksStream{StreamReader: ingester1, Series: []labels.Labels{labels.FromStrings("some-label", "some-value")}}},
				{streamingSeries: seriesChunksStream{StreamReader: ingester2, Series: []labels.Labels{labels.FromStrings("some-label", "some-value")}}},
				{streamingSeries: seriesChunksStream{StreamReader: ingester3, Series: []labels.Labels{labels.FromStrings("some-label", "some-value")}}},
			},
			expected: []ingester_client.StreamingSeries{
				{
					Labels: labels.FromStrings("some-label", "some-value"),
					Sources: []ingester_client.StreamingSeriesSource{
						{StreamReader: ingester1, SeriesIndex: 0},
						{StreamReader: ingester2, SeriesIndex: 0},
						{StreamReader: ingester3, SeriesIndex: 0},
					},
				},
			},
		},
		"multiple ingesters, each with different series": {
			results: []ingesterQueryResult{
				{streamingSeries: seriesChunksStream{StreamReader: ingester1, Series: []labels.Labels{labels.FromStrings("some-label", "value-a")}}},
				{streamingSeries: seriesChunksStream{StreamReader: ingester2, Series: []labels.Labels{labels.FromStrings("some-label", "value-b")}}},
				{streamingSeries: seriesChunksStream{StreamReader: ingester3, Series: []labels.Labels{labels.FromStrings("some-label", "value-c")}}},
			},
			expected: []ingester_client.StreamingSeries{
				{
					Labels: labels.FromStrings("some-label", "value-a"),
					Sources: []ingester_client.StreamingSeriesSource{
						{StreamReader: ingester1, SeriesIndex: 0},
					},
				},
				{
					Labels: labels.FromStrings("some-label", "value-b"),
					Sources: []ingester_client.StreamingSeriesSource{
						{StreamReader: ingester2, SeriesIndex: 0},
					},
				},
				{
					Labels: labels.FromStrings("some-label", "value-c"),
					Sources: []ingester_client.StreamingSeriesSource{
						{StreamReader: ingester3, SeriesIndex: 0},
					},
				},
			},
		},
		"multiple ingesters, each with different series, with earliest ingesters having last series": {
			results: []ingesterQueryResult{
				{streamingSeries: seriesChunksStream{StreamReader: ingester3, Series: []labels.Labels{labels.FromStrings("some-label", "value-c")}}},
				{streamingSeries: seriesChunksStream{StreamReader: ingester2, Series: []labels.Labels{labels.FromStrings("some-label", "value-b")}}},
				{streamingSeries: seriesChunksStream{StreamReader: ingester1, Series: []labels.Labels{labels.FromStrings("some-label", "value-a")}}},
			},
			expected: []ingester_client.StreamingSeries{
				{
					Labels: labels.FromStrings("some-label", "value-a"),
					Sources: []ingester_client.StreamingSeriesSource{
						{StreamReader: ingester1, SeriesIndex: 0},
					},
				},
				{
					Labels: labels.FromStrings("some-label", "value-b"),
					Sources: []ingester_client.StreamingSeriesSource{
						{StreamReader: ingester2, SeriesIndex: 0},
					},
				},
				{
					Labels: labels.FromStrings("some-label", "value-c"),
					Sources: []ingester_client.StreamingSeriesSource{
						{StreamReader: ingester3, SeriesIndex: 0},
					},
				},
			},
		},
		"multiple ingesters, each with multiple series": {
			results: []ingesterQueryResult{
				{streamingSeries: seriesChunksStream{StreamReader: ingester1, Series: []labels.Labels{labels.FromStrings("label-a", "value-a"), labels.FromStrings("label-b", "value-a")}}},
				{streamingSeries: seriesChunksStream{StreamReader: ingester2, Series: []labels.Labels{labels.FromStrings("label-a", "value-b"), labels.FromStrings("label-b", "value-a")}}},
				{streamingSeries: seriesChunksStream{StreamReader: ingester3, Series: []labels.Labels{labels.FromStrings("label-a", "value-c"), labels.FromStrings("label-b", "value-a")}}},
			},
			expected: []ingester_client.StreamingSeries{
				{
					Labels: labels.FromStrings("label-a", "value-a"),
					Sources: []ingester_client.StreamingSeriesSource{
						{StreamReader: ingester1, SeriesIndex: 0},
					},
				},
				{
					Labels: labels.FromStrings("label-a", "value-b"),
					Sources: []ingester_client.StreamingSeriesSource{
						{StreamReader: ingester2, SeriesIndex: 0},
					},
				},
				{
					Labels: labels.FromStrings("label-a", "value-c"),
					Sources: []ingester_client.StreamingSeriesSource{
						{StreamReader: ingester3, SeriesIndex: 0},
					},
				},
				{
					Labels: labels.FromStrings("label-b", "value-a"),
					Sources: []ingester_client.StreamingSeriesSource{
						{StreamReader: ingester1, SeriesIndex: 1},
						{StreamReader: ingester2, SeriesIndex: 1},
						{StreamReader: ingester3, SeriesIndex: 1},
					},
				},
			},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			zoneCount := 1 // The exact value of this only matters for performance (it's used to pre-allocate a slice of the correct size)
			actual := mergeSeriesChunkStreams(testCase.results, zoneCount)
			require.Lenf(t, actual, len(testCase.expected), "should be same length as %v", testCase.expected)

			for i := 0; i < len(actual); i++ {
				actualSeries := actual[i]
				expectedSeries := testCase.expected[i]

				require.Equal(t, expectedSeries.Labels, actualSeries.Labels)

				// We don't care about the order.
				require.ElementsMatch(t, expectedSeries.Sources, actualSeries.Sources, "series %v", actualSeries.Labels.String())
			}
		})
	}
}

func BenchmarkMergingAndSortingSeries(b *testing.B) {
	for _, ingestersPerZone := range []int{1, 2, 4, 10, 100} {
		for _, zones := range []int{1, 2, 3} {
			for _, seriesPerIngester := range []int{1, 10, 100, 1000, 10000} {
				seriesSets := generateSeriesSets(ingestersPerZone, zones, seriesPerIngester)

				b.Run(fmt.Sprintf("%v ingesters per zone, %v zones, %v series per ingester", ingestersPerZone, zones, seriesPerIngester), func(b *testing.B) {
					for i := 0; i < b.N; i++ {
						mergeSeriesChunkStreams(seriesSets, zones)
					}
				})
			}
		}
	}
}

func generateSeriesSets(ingestersPerZone int, zones int, seriesPerIngester int) []ingesterQueryResult {
	seriesPerZone := ingestersPerZone * seriesPerIngester
	zoneSeries := make([]labels.Labels, seriesPerZone)

	for seriesIdx := 0; seriesIdx < seriesPerZone; seriesIdx++ {
		zoneSeries[seriesIdx] = labels.FromStrings("the-label", strconv.Itoa(seriesIdx))
	}

	results := make([]ingesterQueryResult, 0, zones*ingestersPerZone)

	for zone := 1; zone <= zones; zone++ {
		rand.Shuffle(len(zoneSeries), func(i, j int) { zoneSeries[i], zoneSeries[j] = zoneSeries[j], zoneSeries[i] })

		for ingester := 1; ingester <= ingestersPerZone; ingester++ {
			streamReader := &ingester_client.SeriesChunksStreamReader{}
			series := zoneSeries[(ingester-1)*seriesPerIngester : ingester*seriesPerIngester]
			sort.Sort(byLabels(series))

			results = append(results, ingesterQueryResult{streamingSeries: seriesChunksStream{StreamReader: streamReader, Series: series}})
		}
	}

	return results
}

type byLabels []labels.Labels

func (b byLabels) Len() int           { return len(b) }
func (b byLabels) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }
func (b byLabels) Less(i, j int) bool { return labels.Compare(b[i], b[j]) < 0 }
