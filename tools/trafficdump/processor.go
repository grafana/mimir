// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"encoding/json"
	"flag"
	"log"
	"os"

	"github.com/google/gopacket"
)

type processorConfig struct {
	matchStatusCode int
	successOnly     bool
}

func (cfg *processorConfig) RegisterFlags(f *flag.FlagSet) {
	f.IntVar(&cfg.matchStatusCode, "status-code", 0, "If not 0, only output requests that ended with this status code")
	f.BoolVar(&cfg.successOnly, "success-only", true, "Only report requests and responses that were parsed successfully.")
}

// processor stores each unidirectional side of a bidirectional stream.
type processor struct {
	cfg *processorConfig

	net, transport gopacket.Flow // from client to server

	req  *requestStream  // stream from client to server
	resp *responseStream // stream from server to client
}

var enc = json.NewEncoder(os.Stdout)

func newProcessor(cfg *processorConfig, net, transport gopacket.Flow) *processor {
	return &processor{
		cfg:       cfg,
		net:       net,
		transport: transport,
	}
}

// Started when either requestStream or responseStream is found.
func (p *processor) run() {
	// We rely on synchronization between requests and responses. Generally this is true, but if there are errors decoding
	// requests or responses, they may desynchronize. Not sure if we can do anything about it.
	for req := range p.req.out {
		resp := <-p.resp.out

		p.print(req, resp)
	}

	// Drain responses
	for resp := range p.resp.out {
		p.print(nil, resp)
	}
}

func (p *processor) print(req *request, resp *response) {
	if req != nil && req.PushRequest != nil && req.PushRequest.cleanup != nil {
		defer req.PushRequest.cleanup()
	}

	if req != nil && req.ignored {
		return
	}

	if req != nil && p.cfg.matchStatusCode != 0 && resp != nil && p.cfg.matchStatusCode != resp.StatusCode {
		return
	}

	if p.cfg.successOnly && (req.Error != "" || resp.Error != "") {
		return
	}

	output := &output{
		Client: endpoint{
			Address: p.net.Src().String(),
			Port:    p.transport.Src().String(),
		},
		Server: endpoint{
			Address: p.net.Dst().String(),
			Port:    p.transport.Dst().String(),
		},

		Request:  req,
		Response: resp,
	}

	err := enc.Encode(output)
	if err != nil {
		log.Println("error while writing output", err)
	}
}
