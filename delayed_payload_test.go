package main

import (
	"bufio"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDelayedPayload(t *testing.T) {
	// Read password from token file
	homeDir, err := os.UserHomeDir()
	if err != nil {
		t.Fatalf("Failed to get home directory: %v", err)
	}
	
	tokenPath := filepath.Join(homeDir, "secrets", "dev-metrics-write.token")
	passwordBytes, err := ioutil.ReadFile(tokenPath)
	if err != nil {
		t.Fatalf("Failed to read token file %s: %v", tokenPath, err)
	}
	password := strings.TrimSpace(string(passwordBytes))
	
	// Create basic auth header
	auth := base64.StdEncoding.EncodeToString([]byte("9960:" + password))

	// Connect to the service with TLS
	conn, err := tls.Dial("tcp", "prometheus-dev-01-dev-us-central-0.grafana-dev.net:443", &tls.Config{})
	if err != nil {
		t.Fatalf("Failed to connect to prometheus-dev-01-dev-us-central-0.grafana-dev.net:443: %v", err)
	}
	defer conn.Close()

	// Prepare HTTP headers
	headers := "POST /api/prom/push HTTP/1.1\r\n" +
		"Host: prometheus-dev-01-dev-us-central-0.grafana-dev.net\r\n" +
		"Content-Type: application/x-protobuf\r\n" +
		"Content-Encoding: snappy\r\n" +
		"User-Agent: prometheus/2.45.0\r\n" +
		"X-Prometheus-Remote-Write-Version: 0.1.0\r\n" +
		"Authorization: Basic " + auth + "\r\n" +
		"Content-Length: 156\r\n" +
		"\r\n"

	// Send headers immediately
	fmt.Printf("Sending headers at: %s\n", time.Now().Format("15:04:05"))
	_, err = conn.Write([]byte(headers))
	if err != nil {
		t.Fatalf("Failed to send headers: %v", err)
	}

	// Set up reader to monitor server responses
	go func() {
		reader := bufio.NewReader(conn)
		for {
			response, err := reader.ReadString('\n')
			if err != nil {
				fmt.Printf("Connection closed or error reading response: %v\n", err)
				break
			}
			fmt.Printf("Server response: %s", response)
		}
	}()

	// Wait for 1 minute before sending payload
	fmt.Printf("Waiting 1 minute before sending payload...\n")
	time.Sleep(1 * time.Minute)

	// Send dummy payload (156 bytes of zeros to match Content-Length)
	payload := make([]byte, 156)
	fmt.Printf("Sending payload at: %s\n", time.Now().Format("15:04:05"))
	_, err = conn.Write(payload)
	if err != nil {
		t.Fatalf("Failed to send payload: %v", err)
	}

	// Wait a bit for server response
	time.Sleep(5 * time.Second)
	fmt.Printf("Test completed at: %s\n", time.Now().Format("15:04:05"))
}