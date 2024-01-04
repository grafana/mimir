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

	"github.com/google/go-github/v57/github"
	log "github.com/sirupsen/logrus"
)

const (
	githubReleasePrefix = "mimir-"
)

var (
	errUnableToRetrieveLatestVersion = errors.New("unable to fetch the latest version from GitHub")
)

// CheckLatest asks GitHub
func CheckLatest(version string) {
	if version != "" {
		latest, latestURL, err := getLatestFromGitHub()
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

func getLatestFromGitHub() (string, string, error) {
	fmt.Print("checking latest version... ")
	c := github.NewClient(nil)
	repoRelease, resp, err := c.Repositories.GetLatestRelease(context.Background(), "grafana", "mimir")
	if err != nil {
		log.WithFields(log.Fields{"err": err}).Debugln("error while retrieving the latest version")
		return "", "", errUnableToRetrieveLatestVersion
	}

	if resp.StatusCode/100 != 2 {
		log.WithFields(log.Fields{"status": resp.StatusCode}).Debugln("non-2xx status code while contacting the GitHub API")
		return "", "", errUnableToRetrieveLatestVersion
	}

	return repoRelease.GetTagName(), repoRelease.GetHTMLURL(), nil
}
