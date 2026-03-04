// SPDX-License-Identifier: AGPL-3.0-only

// Command oauthtokenserver is a small HTTP server that listens on a Unix domain
// socket and responds with a fresh OAuth token fetched from a Dex instance.
// It is used in integration tests to test the HTTP socket OAuth authentication
// flow.
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
)

func main() {
	socketPath := os.Getenv("SOCKET_PATH")
	dexURL := os.Getenv("DEX_URL")
	clientID := os.Getenv("CLIENT_ID")
	clientSecret := os.Getenv("CLIENT_SECRET")
	username := os.Getenv("USERNAME")
	password := os.Getenv("PASSWORD")

	tokenURL := strings.TrimRight(dexURL, "/") + "/dex/token"

	ln, err := net.Listen("unix", socketPath)
	if err != nil {
		log.Fatalf("Failed to listen on %s: %v", socketPath, err)
	}
	// Make the socket accessible to all containers sharing the volume.
	if err := os.Chmod(socketPath, 0777); err != nil {
		log.Fatalf("Failed to chmod socket: %v", err)
	}

	log.Printf("Listening on %s", socketPath)

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		token, err := fetchToken(tokenURL, clientID, clientSecret, username, password)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			log.Printf("Error fetching token: %v", err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		err = json.NewEncoder(w).Encode(map[string]string{"token": token})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			log.Printf("Error encoding token: %v", err)
		}
	})

	log.Fatal(http.Serve(ln, mux))
}

func fetchToken(tokenURL, clientID, clientSecret, username, password string) (string, error) {
	resp, err := http.PostForm(tokenURL, url.Values{
		"grant_type":    {"password"},
		"username":      {username},
		"password":      {password},
		"client_id":     {clientID},
		"client_secret": {clientSecret},
		"scope":         {"openid"},
	})
	if err != nil {
		return "", fmt.Errorf("requesting token from %s: %w", tokenURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("unexpected status from %s: %s", tokenURL, resp.Status)
	}

	var result struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("decoding token response: %w", err)
	}
	if result.AccessToken == "" {
		return "", fmt.Errorf("empty access_token in response from %s", tokenURL)
	}
	return result.AccessToken, nil
}
