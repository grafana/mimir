// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"fmt"
	"os"

	"github.com/grafana/mimir/pkg/mimir"
	"github.com/grafana/mimir/pkg/mimirtool/config"
)

func main() {
	bytes, err := config.DefaultValueInspector.Describe(&mimir.Config{})
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	fmt.Println(string(bytes))
}
