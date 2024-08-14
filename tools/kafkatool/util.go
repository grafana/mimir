// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

func CreateKafkaClient(kafkaAddress, kafkaClientID string) (*kgo.Client, error) {
	return kgo.NewClient(kgo.SeedBrokers(kafkaAddress), kgo.ClientID(kafkaClientID))
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
