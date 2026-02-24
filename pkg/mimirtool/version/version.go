// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/grafana/cortex-tools/blob/main/pkg/version/version.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package version

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/google/go-github/v83/github"
)

const (
	githubReleasePrefix = "mimir-"
)

var (
	errUnableToRetrieveLatestVersion = errors.New("unable to fetch the latest version from GitHub")
)

// CheckLatest asks GitHub
func CheckLatest(version string, logger log.Logger) {
	if version != "" {
		latest, latestURL, err := getLatestFromGitHub(logger)
		if err != nil {
			fmt.Println(err)
			return
		}

		latest = strings.TrimPrefix(latest, githubReleasePrefix)
		if latest != "" && (latest != strings.TrimPrefix(version, githubReleasePrefix)) {
			fmt.Printf("A newer version of mimirtool is available, please update to %s %s\n", latest, latestURL)
		} else {
			fmt.Println("You are on the latest version")
		}
	}
}

func getLatestFromGitHub(logger log.Logger) (string, string, error) {
	fmt.Print("checking latest version... ")
	c := github.NewClient(nil)
	repoRelease, resp, err := c.Repositories.GetLatestRelease(context.Background(), "grafana", "mimir")
	if err != nil {
		level.Debug(logger).Log("msg", "error while retrieving the latest version", "err", err)
		return "", "", errUnableToRetrieveLatestVersion
	}

	if resp.StatusCode/100 != 2 {
		level.Debug(logger).Log("msg", "non-2xx status code while contacting the GitHub API", "status", resp.StatusCode)
		return "", "", errUnableToRetrieveLatestVersion
	}

	return repoRelease.GetTagName(), repoRelease.GetHTMLURL(), nil
}
