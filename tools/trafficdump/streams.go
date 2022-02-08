// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"bufio"
	"io"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/google/gopacket/tcpassembly/tcpreader"
)

type requestStream struct {
	r *tcpreader.ReaderStream
	p *parser

	out chan *request
}

func newRequestStream(r *tcpreader.ReaderStream, p *parser) *requestStream {
	rs := &requestStream{
		r:   r,
		p:   p,
		out: make(chan *request, 1024),
	}
	go rs.parseRequests()
	return rs
}

func (rs *requestStream) parseRequests() {
	defer close(rs.out)

	buf := bufio.NewReader(rs.r)
	for {
		req, err := http.ReadRequest(buf)
		if err == io.EOF {
			// We must read until we see an EOF... very important!
			return
		}

		var r *request
		if err != nil {
			r = &request{Error: err.Error()}
		} else {
			body, err := ioutil.ReadAll(req.Body)
			_ = req.Body.Close()

			if err != nil {
				r = &request{Error: err.Error()}
			} else {
				r = rs.p.processHTTPRequest(req, body)
			}
		}

		select {
		case rs.out <- r:
			continue
		default:
			log.Println("dropping request")

		}
	}
}

type responseStream struct {
	r *tcpreader.ReaderStream
	p *parser

	out chan *response
}

func newResponseStream(r *tcpreader.ReaderStream, p *parser) *responseStream {
	rs := &responseStream{
		r:   r,
		p:   p,
		out: make(chan *response, 1024),
	}
	go rs.parseResponses()
	return rs
}

func (rs *responseStream) parseResponses() {
	defer close(rs.out)

	buf := bufio.NewReader(rs.r)
	for {
		req, err := http.ReadResponse(buf, nil)
		if err == io.ErrUnexpectedEOF {
			return
		}

		var r *response
		if err != nil {
			r = &response{Error: err.Error()}
		} else {
			body, err := ioutil.ReadAll(req.Body)
			_ = req.Body.Close()

			if err != nil {
				r = &response{Error: err.Error()}
			} else {
				r = rs.p.processHTTPResponse(req, body)
			}
		}

		select {
		case rs.out <- r:
			continue
		default:
			log.Println("dropping response")
		}
	}
}
