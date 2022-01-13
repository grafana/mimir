package main

import (
	"fmt"

	"github.com/grafana/e2e/images"
)

func main() {
	// This little script outputs the required images for the integration tests, which should be preloaded.
	fmt.Println(images.Minio)
	fmt.Println(images.Consul)
	fmt.Println(images.ETCD)
	fmt.Println("quay.io/cortexproject/cortex:v1.11.0")
	fmt.Println(images.Memcached)
}
