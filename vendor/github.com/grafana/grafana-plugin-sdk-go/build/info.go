package build

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

// set from -X
var buildInfoJSON string

// exposed for testing.
var now = time.Now

// Info See also PluginBuildInfo in https://github.com/grafana/grafana/blob/master/pkg/plugins/models.go
type Info struct {
	Time     int64  `json:"time,omitempty"`
	PluginID string `json:"pluginID,omitempty"`
	Version  string `json:"version,omitempty"`
	Repo     string `json:"repo,omitempty"`
	Branch   string `json:"branch,omitempty"`
	Hash     string `json:"hash,omitempty"`
	Build    int64  `json:"build,omitempty"`
	PR       int64  `json:"pr,omitempty"`
}

// this will append build flags -- the keys are picked to match existing
// grafana build flags from bra
func (v Info) appendFlags(flags map[string]string) {
	if v.PluginID != "" {
		flags["main.pluginID"] = v.PluginID
	}
	if v.Version != "" {
		flags["main.version"] = v.Version
	}
	if v.Branch != "" {
		flags["main.branch"] = v.Branch
	}
	if v.Hash != "" {
		flags["main.commit"] = v.Hash
	}

	out, err := json.Marshal(v)
	if err == nil {
		flags["github.com/grafana/grafana-plugin-sdk-go/build.buildInfoJSON"] = string(out)
	}
}

func getEnvironment(check ...string) string {
	for _, key := range check {
		if strings.HasPrefix(key, "> ") {
			parts := strings.Split(key, " ")
			cmd := exec.Command(parts[1], parts[2:]...) // #nosec G204
			out, err := cmd.CombinedOutput()
			if err == nil && len(out) > 0 {
				str := strings.TrimSpace(string(out))
				if strings.Index(str, " ") > 0 {
					continue // skip any output that has spaces
				}
				return str
			}
			continue
		}

		val := os.Getenv(key)
		if val != "" {
			return strings.TrimSpace(val)
		}
	}
	return ""
}

// getBuildInfoFromEnvironment reads the
func getBuildInfoFromEnvironment() Info {
	v := Info{
		Time: now().UnixNano() / int64(time.Millisecond),
	}

	v.Repo = getEnvironment(
		"DRONE_REPO_LINK",
		"CIRCLE_PROJECT_REPONAME",
		"CI_REPONAME",
		"> git remote get-url origin")
	v.Branch = getEnvironment(
		"DRONE_BRANCH",
		"CIRCLE_BRANCH",
		"CI_BRANCH",
		"> git branch --show-current")
	v.Hash = getEnvironment(
		"DRONE_COMMIT_SHA",
		"CIRCLE_SHA1",
		"CI_COMMIT_SHA",
		"> git rev-parse HEAD")
	val, err := strconv.ParseInt(getEnvironment(
		"DRONE_BUILD_NUMBER",
		"CIRCLE_BUILD_NUM",
		"CI_BUILD_NUM"), 10, 64)
	if err == nil {
		v.Build = val
	}
	val, err = strconv.ParseInt(getEnvironment(
		"DRONE_PULL_REQUEST",
		"CI_PULL_REQUEST"), 10, 64)
	if err == nil {
		v.PR = val
	}
	return v
}

// InfoGetter is an interface with a method for returning the build info.
type InfoGetter interface {
	// GetInfo returns the build info.
	GetInfo() (Info, error)
}

// InfoGetterFunc can be used to adapt ordinary functions into types satisfying the InfoGetter interface .
type InfoGetterFunc func() (Info, error)

func (f InfoGetterFunc) GetInfo() (Info, error) {
	return f()
}

// GetBuildInfo is the default InfoGetter that returns the build information that was compiled into the binary using:
// -X `github.com/grafana/grafana-plugin-sdk-go/build.buildInfoJSON={...}`
var GetBuildInfo = InfoGetterFunc(func() (Info, error) {
	v := Info{}
	if buildInfoJSON == "" {
		return v, fmt.Errorf("build info was now set when this was compiled")
	}
	err := json.Unmarshal([]byte(buildInfoJSON), &v)
	return v, err
})
