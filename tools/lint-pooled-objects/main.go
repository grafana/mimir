// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"golang.org/x/tools/go/analysis/singlechecker"
)

// https://dustinspecker.com/posts/build-go-linting-rule/ was helpful for understanding how to put this together.

func main() { singlechecker.Main(Analyzer) }
