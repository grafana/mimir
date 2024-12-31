package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"os/exec"
	"strings"
	"time"
)

// processPortForwarded handles port-forwarding, HTTP request, and cleanup for a single pod
func processPortForwarded(pod string, namespace string, podPort int, process func(pod string, localPort int)) error {
	localPort, err := getFreePort()
	if err != nil {
		return fmt.Errorf("failed to get free port: %v", err)
	}

	// Start port-forwarding
	cmd := exec.Command("kubectl", "port-forward", "--namespace", namespace,
		fmt.Sprintf("pod/%s", pod), fmt.Sprintf("%d:%d", localPort, podPort))

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("failed to get stdout pipe: %v", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("failed to get stderr pipe: %v", err)
	}

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start kubectl port-forward: %v", err)
	}

	defer cmd.Process.Kill()

	// Wait for port-forward to be ready
	if err := waitForPortForward(stdout, stderr); err != nil {
		return fmt.Errorf("port-forward failed: %v", err)
	}

	process(pod, localPort)

	// Stop the port-forward
	if err := cmd.Process.Kill(); err != nil {
		return fmt.Errorf("failed to kill port-forward process: %v", err)
	}

	return nil
}

// getFreePort finds an available TCP port
func getFreePort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return 0, fmt.Errorf("resolve tcp addr failed: %v", err)
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, fmt.Errorf("tcp listen failed: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

// waitForPortForward waits until port-forward is ready
func waitForPortForward(stdout, stderr io.ReadCloser) error {
	done := make(chan error)
	go func() {
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			line := scanner.Text()
			log.Printf("DEBUG STDERR: %s", line)
			if line != "" {
				done <- fmt.Errorf("port-forward error: %s", line)
				return
			}
		}
	}()

	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			line := scanner.Text()
			if line != "" {
				if strings.Contains(line, "Forwarding from") {
					done <- nil
					return
				} else {
					log.Printf("DEBUG STDOUT: %s", line)
				}
			}
		}
	}()

	select {
	case err := <-done:
		return err
	case <-time.After(5 * time.Second):
		return fmt.Errorf("timeout waiting for port-forward")
	}
}
