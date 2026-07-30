# automemlimit

[![Go Reference](https://pkg.go.dev/badge/github.com/KimMachineGun/automemlimit.svg)](https://pkg.go.dev/github.com/KimMachineGun/automemlimit)
[![Go Report Card](https://goreportcard.com/badge/github.com/KimMachineGun/automemlimit)](https://goreportcard.com/report/github.com/KimMachineGun/automemlimit)
[![Test](https://github.com/KimMachineGun/automemlimit/actions/workflows/test.yml/badge.svg?branch=main)](https://github.com/KimMachineGun/automemlimit/actions/workflows/test.yml)

Automatically set `GOMEMLIMIT` to match Linux [cgroups(7)](https://man7.org/linux/man-pages/man7/cgroups.7.html) memory limit.

See more details about `GOMEMLIMIT` [here](https://tip.golang.org/doc/gc-guide#Memory_limit).

## Notice

Version `v0.5.0` introduces a fallback to system memory limits as an experimental feature when cgroup limits are unavailable. Activate this by setting `AUTOMEMLIMIT_EXPERIMENT=system`.
You can also use system memory limits via `memlimit.FromSystem` provider directly.

This feature is under evaluation and might become a default or be removed based on user feedback.
If you have any feedback about this feature, please open an issue.

## Installation

```shell
go get github.com/KimMachineGun/automemlimit@latest
```

## Usage

```go
package main

// By default, it sets `GOMEMLIMIT` to 90% of cgroup's memory limit.
// This is equivalent to `memlimit.SetGoMemLimitWithOpts(memlimit.WithLogger(slog.Default()))`
// To disable logging, use `memlimit.SetGoMemLimitWithOpts` directly.
import _ "github.com/KimMachineGun/automemlimit"
```

or

```go
package main

import "github.com/KimMachineGun/automemlimit/memlimit"

func init() {
	memlimit.SetGoMemLimitWithOpts(
		memlimit.WithRatio(0.9),
		memlimit.WithProvider(memlimit.FromCgroup),
		memlimit.WithLogger(slog.Default()),
		memlimit.WithRefreshInterval(1*time.Minute),
	)
	memlimit.SetGoMemLimitWithOpts(
		memlimit.WithRatio(0.9),
		memlimit.WithProvider(
			memlimit.ApplyFallback(
				memlimit.FromCgroup,
				memlimit.FromSystem,
			),
		),
		memlimit.WithRefreshInterval(1*time.Minute),
	)
	memlimit.SetGoMemLimit(0.9)
	memlimit.SetGoMemLimitWithProvider(memlimit.Limit(1024*1024), 0.9)
	memlimit.SetGoMemLimitWithProvider(memlimit.FromCgroup, 0.9)
	memlimit.SetGoMemLimitWithProvider(memlimit.FromCgroupV1, 0.9)
	memlimit.SetGoMemLimitWithProvider(memlimit.FromCgroupHybrid, 0.9)
	memlimit.SetGoMemLimitWithProvider(memlimit.FromCgroupV2, 0.9)
}
```
