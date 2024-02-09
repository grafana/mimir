// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"fmt"

	"github.com/grafana/e2e/images"

	"github.com/grafana/mimir/integration"
)

func main() {
	// This little script outputs the required images for the integration tests, which should be preloaded.

	// images from e2e components
	fmt.Println(images.Minio)
	fmt.Println(images.Consul)
	fmt.Println(images.ETCD)
	fmt.Println(images.Memcached)
	fmt.Println(images.Kafka)

	// vault image
	fmt.Println(integration.VaultImage)

	// images from previous releases
	for image := range integration.DefaultPreviousVersionImages {
		fmt.Println(image)
	}
}
