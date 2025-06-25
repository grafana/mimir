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

func TestSlowPayload(t *testing.T) {
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

	// Example payload bytes (simple protobuf-like data)
	// This represents a minimal WriteRequest with one TimeSeries
	payload := []byte{
		0x0a, 0x4e, 0x0a, 0x22, 0x0a, 0x09, 0x5f, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x5f, 0x5f, 0x12, 0x15,
		0x63, 0x70, 0x75, 0x5f, 0x75, 0x73, 0x61, 0x67, 0x65, 0x5f, 0x73, 0x65, 0x63, 0x6f, 0x6e, 0x64,
		0x73, 0x5f, 0x74, 0x6f, 0x74, 0x61, 0x6c, 0x0a, 0x08, 0x69, 0x6e, 0x73, 0x74, 0x61, 0x6e, 0x63,
		0x65, 0x12, 0x0e, 0x6c, 0x6f, 0x63, 0x61, 0x6c, 0x68, 0x6f, 0x73, 0x74, 0x3a, 0x39, 0x30, 0x39,
		0x30, 0x0a, 0x03, 0x6a, 0x6f, 0x62, 0x12, 0x0a, 0x70, 0x72, 0x6f, 0x6d, 0x65, 0x74, 0x68, 0x65,
		0x75, 0x73, 0x12, 0x10, 0x11, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, 0x45, 0x40, 0x18, 0x80, 0x80,
		0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01,
	}

	// Prepare HTTP headers with correct content length
	headers := fmt.Sprintf("POST /api/prom/push HTTP/1.1\r\n"+
		"Host: prometheus-dev-01-dev-us-central-0.grafana-dev.net\r\n"+
		"Content-Type: application/x-protobuf\r\n"+
		"Content-Encoding: snappy\r\n"+
		"User-Agent: prometheus/2.45.0\r\n"+
		"X-Prometheus-Remote-Write-Version: 0.1.0\r\n"+
		"Authorization: Basic %s\r\n"+
		"Content-Length: %d\r\n"+
		"\r\n", auth, len(payload))

	// Send headers immediately
	fmt.Printf("Sending headers at: %s\n", time.Now().Format("15:04:05"))
	_, err = conn.Write([]byte(headers))
	if err != nil {
		t.Fatalf("Failed to send headers: %v", err)
	}

	// Set up reader to monitor server responses
	responseChan := make(chan string, 10)
	go func() {
		reader := bufio.NewReader(conn)
		for {
			response, err := reader.ReadString('\n')
			if err != nil {
				responseChan <- fmt.Sprintf("Connection closed or error: %v", err)
				break
			}
			responseChan <- fmt.Sprintf("Server response: %s", response)
		}
	}()

	// Send payload one byte per second
	fmt.Printf("Starting to send %d bytes at 1 byte/second at: %s\n", len(payload), time.Now().Format("15:04:05"))
	
	for i, b := range payload {
		_, err = conn.Write([]byte{b})
		if err != nil {
			fmt.Printf("Failed to send byte %d: %v\n", i, err)
			break
		}
		fmt.Printf("Sent byte %d/%d at: %s\n", i+1, len(payload), time.Now().Format("15:04:05"))
		
		// Check for server responses
		select {
		case response := <-responseChan:
			fmt.Printf("%s\n", response)
			if response != "" {
				// Server responded, likely with timeout
				break
			}
		case <-time.After(1 * time.Second):
			// Continue to next byte
		}
	}

	// Wait a bit for any remaining server responses
	select {
	case response := <-responseChan:
		fmt.Printf("%s\n", response)
	case <-time.After(2 * time.Second):
		// No more responses
	}

	fmt.Printf("Test completed at: %s\n", time.Now().Format("15:04:05"))
}