package e2emimir

import (
	"fmt"

	"github.com/grafana/e2e"
)

const ShellImage = "alpine:3.23.2"

func RunInContainerShell(s *e2e.Scenario, cmd string) (output []byte, err error) {
	return e2e.RunCommandAndGetOutput("docker", "run", "--rm",
		"-v", fmt.Sprintf("%s:/shared:z", s.SharedDir()),
		ShellImage,
		"sh", "-c", cmd)
}
