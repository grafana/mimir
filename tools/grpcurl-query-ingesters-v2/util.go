package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"time"
)

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
