// SPDX-License-Identifier: AGPL-3.0-only

package queue

// Test-only queue-dimension labels mirroring the values the scheduler attaches
// to queries. The queue itself treats dimensions as opaque strings; these are
// defined here only so the queue's tests can reuse them.
const (
	ingesterQueueDimension                = "ingester"
	storeGatewayQueueDimension            = "store-gateway"
	ingesterAndStoreGatewayQueueDimension = "ingester-and-store-gateway"
)
