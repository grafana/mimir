// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"encoding/json"
	"log"
	"os"

	"github.com/google/gopacket"
)

// processor stores each unidirectional side of a bidirectional stream.
type processor struct {
	net, transport gopacket.Flow // from client to server

	req  *requestStream  // stream from client to server
	resp *responseStream // stream from server to client
}

var enc = json.NewEncoder(os.Stdout)

func newProcessor(net, transport gopacket.Flow) *processor {
	return &processor{
		net:       net,
		transport: transport,
	}
}

// Started when either requestStream or responseStream is found.
func (p *processor) run() {
	//log.Printf("start: processing requests for %s:%s -> %s:%s\n", p.net.Src().String(), p.transport.Src().String(), p.net.Dst().String(), p.transport.Dst().String())

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

	//log.Printf("finish: processing requests for %s:%s -> %s%s\n", p.net.Src().String(), p.transport.Src().String(), p.net.Dst().String(), p.transport.Dst().String())
}

func (p *processor) print(req *request, resp *response) {
	if req != nil && req.PushRequest != nil && req.PushRequest.cleanup != nil {
		defer req.PushRequest.cleanup()
	}

	// print the request/response pair.
	if req != nil && req.ignored {
		return
	}

	if req != nil && req.matchStatusCode != 0 && resp != nil && req.matchStatusCode != resp.StatusCode {
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
