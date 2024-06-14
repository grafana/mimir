// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import "testing"

func TestXXX(t *testing.T) {
	
}


/*
- XXX config validation because we don't want config validation bypassed if error cache is on and results cache is off XXX just use the error cache only if results cache enabled
- cache gets hit (hit and miss metrics?)
- cortex_query_frontend_rejected_queries_total increments (grab this from blocked)
- integration level that probably won't add value here: sg + queriers _don't_ have to run query again. could just validate this once deployed with traces

*/
