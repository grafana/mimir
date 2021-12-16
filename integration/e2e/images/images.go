// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/integration/e2e/images/images.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package images

var (
	// If you change the image tag, remember to update it in the preloading done
	// by GitHub actions (see .github/workflows/*).

	// These are variables so that they can be modified.

	Memcached = "memcached:1.6.12"
	Minio     = "minio/minio:RELEASE.2021-10-13T00-23-17Z"
	Consul    = "consul:1.8.15"
	ETCD      = "gcr.io/etcd-development/etcd:v3.4.13"
	KES       = "minio/kes:v0.17.1"
)
