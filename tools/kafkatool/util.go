// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"fmt"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
)

func CreateKafkaClient(kafkaAddress, kafkaClientID string, auth sasl.Mechanism) (*kgo.Client, error) {
	options := []kgo.Opt{
		kgo.SeedBrokers(kafkaAddress),
		kgo.ClientID(kafkaClientID),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.DisableIdempotentWrite(),
		kgo.AllowAutoTopicCreation(),
		kgo.BrokerMaxWriteBytes(268_435_456),
		kgo.MaxBufferedBytes(268_435_456),
	}

	if auth != nil {
		options = append(options, kgo.SASL(auth))
	}

	return kgo.NewClient(options...)
}

type Printer interface {
	PrintLine(string)
}

type StdoutPrinter struct{}

func (StdoutPrinter) PrintLine(line string) {
	fmt.Println(line)
}

type BufferedPrinter struct {
	mtx   sync.Mutex
	Lines []string
}

func (p *BufferedPrinter) GetLines() []string {
	p.mtx.Lock()
	defer p.mtx.Unlock()
	return p.Lines
}

func (p *BufferedPrinter) Reset() {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	p.Lines = nil
}

func (p *BufferedPrinter) PrintLine(line string) {
	p.mtx.Lock()
	defer p.mtx.Unlock()
	p.Lines = append(p.Lines, line)
}
