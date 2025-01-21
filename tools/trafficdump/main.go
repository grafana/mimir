// SPDX-License-Identifier: AGPL-3.0-only
//
// This tool uses requires_libpcap tag to avoid compilation problems on machines that
// don't have libpcap installed (eg. when running "go test ./..." from Mimir root).
//
//go:build requires_libpcap

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
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
	"github.com/grafana/dskit/flagext"
	"go.uber.org/atomic"
)

func main() {
	// Clean up all flags registered via init() methods of 3rd-party libraries.
	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ExitOnError)

	parser := &parser{}

	parser.RegisterFlags(flag.CommandLine)

	fname := flag.String("r", "", "Filename to read traffic dump from")
	filter := flag.String("f", "tcp and port 80", "BPF filter for pcap")
	filterEndpointPort := flag.Int("p", 80, "Only process packets with one of the endpoint ports equal to this value. This should match BPF filter (-f option), if used. 0 to disable.")
	assemblersCount := flag.Uint("assembler.concurrency", 16, "How many TCP Assemblers to run concurrently")
	assemblersMaxPagesPerConnection := flag.Int("assembler.max-pages-per-connection", 0, "Upper limit on the number of pages buffered for a single connection. If this limit is reached for a connection, the smallest sequence number will be flushed, along with any contiguous data. If <= 0, this is ignored.")
	httpServer := flag.String("http-listen", ":18080", "Listen address for HTTP server (useful for profiling of this tool)")

	// Parse CLI arguments.
	if err := flagext.ParseFlagsWithoutArguments(flag.CommandLine); err != nil {
		log.Fatalln(err.Error())
	}

	if *httpServer != "" {
		go func() {
			log.Println("HTTP server running on", *httpServer)
			server := &http.Server{
				Addr:         *httpServer,
				ReadTimeout:  10 * time.Second,
				WriteTimeout: 10 * time.Second,
			}
			log.Println(server.ListenAndServe())
		}()
	}

	var handle *pcap.Handle
	var err error

	// Set up pcap packet capture
	if *fname != "" {
		log.Printf("Reading from pcap dump %q", *fname)
		handle, err = pcap.OpenOffline(*fname)
	} else {
		log.Println("No dump file specified")
		os.Exit(1)
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

	assemblers := make([]*tcpassembly.Assembler, *assemblersCount)
	for ix := uint(0); ix < *assemblersCount; ix++ {
		assemblers[ix] = tcpassembly.NewAssembler(streamPool)
		assemblers[ix].AssemblerOptions.MaxBufferedPagesPerConnection = *assemblersMaxPagesPerConnection
	}

	log.Println("Reading packets")

	// Read in packets, pass to assembler.
	packetSource := gopacket.NewPacketSource(handle, handle.LinkType())
	packets := packetSource.Packets()
	ticker := time.Tick(time.Minute)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT)
	defer cancel()

	var packetsCount atomic.Int64

	go func() {
		for {
			time.Sleep(1 * time.Second)
			log.Println("Processed", packetsCount.Load(), "packets, tracking", streamFactory.runningProcessors.Load(), "TCP connections")
		}
	}()

	filterPort := layers.TCPPort(*filterEndpointPort)

stop:
	for {
		select {
		case packet := <-packets:
			packetsCount.Inc()
			// A nil packet indicates the end of a pcap file.
			if packet == nil {
				break stop
			}

			if packet.NetworkLayer() == nil || packet.TransportLayer() == nil || packet.TransportLayer().LayerType() != layers.LayerTypeTCP {
				log.Println("Unusable packet")
				continue
			}

			tcp := packet.TransportLayer().(*layers.TCP)
			if filterPort != 0 && tcp.SrcPort != filterPort && tcp.DstPort != filterPort {
				// Ignored packet.
				continue
			}

			netFlow := packet.NetworkLayer().NetworkFlow()
			transportFlow := tcp.TransportFlow()

			// Use netFlow (IP address) and transportFlow (TCP ports) to find the shard.
			// FastHash guarantees that flow and its reverse (src->dest, dest->src) have the same hash.
			shard := (netFlow.FastHash() ^ transportFlow.FastHash()) % uint64(*assemblersCount)
			assemblers[shard].AssembleWithTimestamp(netFlow, tcp, packet.Metadata().Timestamp)

		case <-ticker:
			// Every minute, flush connections that haven't seen activity in the past 2 minutes.
			flushed, closed := 0, 0
			for i := uint(0); i < *assemblersCount; i++ {
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
	for i := 0; uint(i) < *assemblersCount; i++ {
		go func(ix int) {
			closeCh <- assemblers[ix].FlushAll()
		}(i)
	}

	closed := 0
	for i := 0; uint(i) < *assemblersCount; i++ {
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

		p = newProcessor(&h.parser.processorConfig, net, transport)
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
