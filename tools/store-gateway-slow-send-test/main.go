package main

import (
	"flag"
	"log"
	"os"
	"runtime/pprof"
)

// TODO run with different targets (server and client) so we can easily distinguish between client and server logs
func main() {
	target := flag.String("target", "", "The target to run: server or client.")
	clientRequests := flag.Int("client.requests", 20, "The number of requests to run.")
	clientConcurrency := flag.Int("client.concurrency", 1, "The number of concurrent requests to run.")
	if err := flag.CommandLine.Parse(os.Args[1:]); err != nil {
		log.Fatal(err.Error())
	}

	switch *target {
	case "server":
		s, err := newServer()
		if err != nil {
			log.Fatal(err.Error())
		}

		log.Fatal(s.Run())

	case "client":
		// Profile.
		startProfiling()
		defer stopProfiling()

		c, err := newClient()
		if err != nil {
			log.Fatal(err.Error())
		}

		if err := c.runRequests(*clientRequests, *clientConcurrency); err != nil {
			log.Fatal(err)
		}

	default:
		log.Fatalf("Unsupported target %q", *target)
	}
}

func startProfiling() {
	f, err := os.Create("cpu.pprof")
	if err != nil {
		log.Fatal(err)
	}
	if err := pprof.StartCPUProfile(f); err != nil {
		log.Fatal(err)
	}
}

func stopProfiling() {
	pprof.StopCPUProfile()

	f, err := os.Create("mem.pprof")
	if err != nil {
		log.Fatal(err)
	}
	if err := pprof.WriteHeapProfile(f); err != nil {
		log.Fatal(err)
	}
	f.Close()
}
