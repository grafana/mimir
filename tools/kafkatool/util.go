// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"crypto/tls"
	"fmt"
	"net"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
)

type OptsMutator func([]kgo.Opt) []kgo.Opt

func SASLPlainOpts(user, pass string) OptsMutator {
	return func(opts []kgo.Opt) []kgo.Opt {
		if user != "" && pass != "" {
			mechanism := plain.Auth{User: user, Pass: pass}.AsMechanism()
			opts = append(opts, kgo.SASL(mechanism))

			// TLS is required with SASL/PLAIN.
			tlsDialer := &tls.Dialer{NetDialer: &net.Dialer{Timeout: 10 * time.Second}}
			opts = append(opts, kgo.Dialer(tlsDialer.DialContext))
		}

		return opts
	}
}

func CreateKafkaClient(kafkaAddress, kafkaClientID string, optsMutators ...OptsMutator) (*kgo.Client, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(kafkaAddress),
		kgo.ClientID(kafkaClientID),
	}

	for _, mutator := range optsMutators {
		opts = mutator(opts)
	}

	return kgo.NewClient(opts...)
}

type Printer interface {
	PrintLine(string)
}

type StdoutPrinter struct{}

func (StdoutPrinter) PrintLine(line string) {
	fmt.Println(line)
}

type BufferedPrinter struct {
	Lines []string
}

func (p *BufferedPrinter) Reset() {
	p.Lines = nil
}

func (p *BufferedPrinter) PrintLine(line string) {
	p.Lines = append(p.Lines, line)
}
