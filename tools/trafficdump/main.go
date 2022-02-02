// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/google/gopacket"
	"github.com/google/gopacket/layers"
	"github.com/google/gopacket/pcap"
	"github.com/google/gopacket/tcpassembly"
	"github.com/google/gopacket/tcpassembly/tcpreader"
	"go.uber.org/atomic"
)

func main() {
	parser := &parser{
		ignorePathRegexpStr: "^/$",
	}

	parser.RegisterFlags(flag.CommandLine)

	iface := flag.String("i", "eth0", "Interface to get packets from")
	fname := flag.String("r", "", "Filename to read from, overrides -i")
	snaplen := flag.Int("s", 262144, "SnapLen for pcap packet capture")
	filter := flag.String("f", "tcp and port 80", "BPF filter for pcap")

	flag.Parse()

	var handle *pcap.Handle
	var err error

	// Set up pcap packet capture
	if *fname != "" {
		log.Printf("Reading from pcap dump %q", *fname)
		handle, err = pcap.OpenOffline(*fname)
	} else {
		log.Printf("Starting capture on interface %q", *iface)
		handle, err = pcap.OpenLive(*iface, int32(*snaplen), true, pcap.BlockForever)
	}
	if err != nil {
		log.Fatal(err)
	}

	if err := handle.SetBPFFilter(*filter); err != nil {
		log.Fatal(err)
	}

	parser.prepare()

	// Set up assembly
	streamFactory := &httpStreamFactory{
		parser:       parser,
		processorMap: map[processorKey]*processor{},
	}

	// Wait until all processors finish before exiting.
	defer streamFactory.processorWG.Wait()

	streamPool := tcpassembly.NewStreamPool(streamFactory)

	const assemblerCount = 16

	assemblers := make([]*tcpassembly.Assembler, assemblerCount)
	for ix := 0; ix < assemblerCount; ix++ {
		assemblers[ix] = tcpassembly.NewAssembler(streamPool)
	}

	log.Println("Reading packets")

	// Read in packets, pass to assembler.
	packetSource := gopacket.NewPacketSource(handle, handle.LinkType())
	packets := packetSource.Packets()
	ticker := time.Tick(time.Minute)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT)
	defer cancel()

	packetsCount := 0

	go func() {
		for {
			time.Sleep(1 * time.Second)
			log.Println("Tracking", streamFactory.runningProcessors.Load(), "TCP connections")
		}
	}()

stop:
	for {
		select {
		case packet := <-packets:
			packetsCount++
			// A nil packet indicates the end of a pcap file.
			if packet == nil {
				break stop
			}

			if packet.NetworkLayer() == nil || packet.TransportLayer() == nil || packet.TransportLayer().LayerType() != layers.LayerTypeTCP {
				log.Println("Unusable packet")
				continue
			}

			tcp := packet.TransportLayer().(*layers.TCP)
			netFlow := packet.NetworkLayer().NetworkFlow()
			transportFlow := tcp.TransportFlow()

			shard := (netFlow.FastHash() ^ transportFlow.FastHash()) % assemblerCount
			assemblers[shard].AssembleWithTimestamp(netFlow, tcp, packet.Metadata().Timestamp)

		case <-ticker:
			// Every minute, flush connections that haven't seen activity in the past 2 minutes.
			flushed, closed := 0, 0
			for i := 0; i < assemblerCount; i++ {
				f, c := assemblers[i].FlushOlderThan(time.Now().Add(time.Minute * -2))
				flushed += f
				closed += c
			}
			log.Println("Flushed", flushed, "and closed", closed, "connections")

		case <-ctx.Done():
			log.Println("CTRL-C, exiting")
			os.Exit(1)
		}
	}

	log.Println("Read", packetsCount, "packets, closing remaining connections")

	closeCh := make(chan int)
	for i := 0; i < assemblerCount; i++ {
		go func(ix int) {
			closeCh <- assemblers[ix].FlushAll()
		}(i)
	}

	closed := 0
	for i := 0; i < assemblerCount; i++ {
		closed += <-closeCh
	}
	log.Println("Closed", closed, "connections")
}

// processorKey is used to map bidirectional streams to each other.
type processorKey struct {
	net, transport gopacket.Flow
}

// String prints out the key in a human-readable fashion.
func (k processorKey) String() string {
	return fmt.Sprintf("%v:%v", k.net, k.transport)
}

// httpStreamFactory implements tcpassembly.StreamFactory
type httpStreamFactory struct {
	parser *parser

	// processorMap maps keys to bidirectional stream pairs. Only used when matching request and response streams,
	// key is deleted after matching succeeds.
	processorMap map[processorKey]*processor

	processorWG       sync.WaitGroup
	runningProcessors atomic.Int64
}

func (h *httpStreamFactory) New(net, transport gopacket.Flow) tcpassembly.Stream {
	readerStream := tcpreader.NewReaderStream()

	// Find if this stream will be request or response stream, and find existing processor, if it exists.
	// If not, start a new one.

	k := processorKey{net, transport}
	p := h.processorMap[k]
	if p == nil {
		// Assume that first stream is from client to server (ie. request stream).
		// TODO: decide whether this is client->server or vice versa based on port, and perhaps configurable IP.

		p = newProcessor(net, transport)
		p.req = newRequestStream(&readerStream, h.parser)

		h.processorMap[processorKey{net.Reverse(), transport.Reverse()}] = p
	} else { // processor already exists, fill in the second stream.
		delete(h.processorMap, k)

		p.resp = newResponseStream(&readerStream, h.parser)

		// We have both directions now, start the request/response processor. It can rely on having both streams set.
		h.processorWG.Add(1)
		h.runningProcessors.Inc()
		go func() {
			defer h.processorWG.Done()
			defer h.runningProcessors.Dec()

			p.run()
		}()
	}

	return &readerStream
}
