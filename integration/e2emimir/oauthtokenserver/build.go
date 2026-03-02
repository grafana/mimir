package oauthtokenserver

import (
	"fmt"
	"path/filepath"
	"runtime"

	"github.com/grafana/e2e"
)

const Image = "oauth-token-server:latest"

// BuildImage builds and returns the Docker image name for the OAuth
// token server used in integration tests.
func BuildImage() (string, error) {
	_, thisFile, _, _ := runtime.Caller(0)
	dockerContext := filepath.Join(filepath.Dir(thisFile), "..", "oauthtokenserver")

	out, err := e2e.RunCommandAndGetOutput("docker", "build", "-t", Image, dockerContext)
	if err != nil {
		return "", fmt.Errorf("failed to build oauth-token-server image: %w. Output: %s", err, string(out))
	}
	return Image, nil
}
