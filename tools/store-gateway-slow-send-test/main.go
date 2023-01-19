package main

import (
	"log"
)

// TODO run with different targets (server and client) so we can easily distinguish between client and server logs
func main() {
	_, err := newServer()
	if err != nil {
		log.Fatal(err.Error())
	}

	c, err := newClient()
	if err != nil {
		log.Fatal(err.Error())
	}

	err = c.runRequest()
	if err != nil {
		log.Fatal(err.Error())
	}
}
