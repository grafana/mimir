package version

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/go-github/v32/github"
	log "github.com/sirupsen/logrus"
)

// Version defines the version for the binary, this is actually set by GoReleaser.
var Version = "master"

// Template controls how the version is displayed
var Template = fmt.Sprintf("version %s\n", Version)

// CheckLatest asks GitHub
func CheckLatest() {
	if Version != "master" {
		latest := getLatestFromGitHub()
		version := Version
		if latest != "" && (strings.TrimPrefix(latest, "v") != strings.TrimPrefix(version, "v")) {
			fmt.Printf("A newer version of cortextool is available, please update to %s\n", latest)
		} else {
			fmt.Println("You are on the latest version")
		}
	}
}

func getLatestFromGitHub() string {
	fmt.Print("checking latest version... ")
	c := github.NewClient(nil)
	resp, _, err := c.Repositories.GetLatestRelease(context.Background(), "grafana", "cortex-tools")

	if err != nil {
		log.WithFields(log.Fields{"err": err}).Debugln("failed to retrieve latest version")
	}

	return *resp.TagName
}
