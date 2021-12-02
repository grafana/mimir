// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/integration/e2e/images/images.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package images

var (
	// If you change the image tag, remember to update it in the preloading done
	// by GitHub actions (see .github/workflows/*).

	// These are variables so that they can be modified.

	Memcached        = "memcached:1.6.12"
	Minio            = "minio/minio:RELEASE.2021-02-19T04-38-02Z"
	Consul           = "consul:1.8.15"
	ETCD             = "gcr.io/etcd-development/etcd:v3.4.13"
	DynamoDB         = "amazon/dynamodb-local:1.17.0"
	BigtableEmulator = "shopify/bigtable-emulator:0.1.0"
	Cassandra        = "rinscy/cassandra:3.11.0"
	SwiftEmulator    = "bouncestorage/swift-aio:55ba4331"
)
