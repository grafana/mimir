// SPDX-License-Identifier: AGPL-3.0-only

// Command go-kafka-ingester is a Mimir ingester that consumes a partition of the ingest storage
// Kafka topic into its own store, a port of cmd/rust-kafka-ingester with the same flags, APIs and
// files.
package main

import (
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"github.com/grafana/mimir/pkg/storage/seriesstore/chunks"
	"github.com/grafana/mimir/pkg/storage/seriesstore/limits"
	"github.com/grafana/mimir/pkg/storage/seriesstore/record"
)

func main() {
	// Like the Rust ingester's eprintln lines: the log collector adds the time.
	log.SetFlags(0)
	if err := run(os.Args[1:]); err != nil {
		log.Printf("Error: %v", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: %s <serve|decode-record|encode-xor|serve-fixture> [flags]", os.Args[0])
	}
	command, args := args[0], args[1:]
	switch command {
	case "serve":
		serveArgs, err := parseServeArgs(args)
		if err != nil {
			return err
		}
		return serve(serveArgs)
	case "decode-record":
		flags := flag.NewFlagSet(command, flag.ContinueOnError)
		version := flags.Uint("version", 0, "")
		if err := parseFlags(flags, args); err != nil {
			return err
		}
		input, err := io.ReadAll(os.Stdin)
		if err != nil {
			return err
		}
		request, err := record.DecodeRecord(uint32(*version), input)
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(summarize(request))
	case "encode-xor":
		var samples [][2]float64
		if err := json.NewDecoder(os.Stdin).Decode(&samples); err != nil {
			return err
		}
		floats := make([]chunks.FloatSample, len(samples))
		for index, sample := range samples {
			floats[index] = chunks.FloatSample{T: int64(sample[0]), V: sample[1]}
		}
		_, err := io.WriteString(os.Stdout, hex.EncodeToString(chunks.EncodeXOR(floats)))
		return err
	case "serve-fixture":
		flags := flag.NewFlagSet(command, flag.ContinueOnError)
		fixture := fixtureArgs{limits: limits.DefaultArgs()}
		flags.StringVar(&fixture.listen, "listen", "127.0.0.1:9095", "")
		flags.StringVar(&fixture.profileListen, "profile-listen", "", "")
		flags.UintVar(&fixture.version, "version", 1, "")
		flags.StringVar(&fixture.tenant, "tenant", "tenant-a", "")
		flags.StringVar(&fixture.dataDir, "data-dir", "", "")
		flags.BoolVar(&fixture.restoreOnly, "restore-only", false, "")
		flags.Int64Var(&fixture.offset, "offset", 0, "")
		flags.Int64Var(&fixture.timestampMs, "timestamp-ms", 1, "")
		fixture.limits.RegisterFlags(flags)
		if err := parseFlags(flags, args); err != nil {
			return err
		}
		return serveFixture(fixture)
	default:
		return fmt.Errorf("unknown command %q", command)
	}
}

// parseFlags parses clap-style arguments: a boolean flag may take its value as the next argument,
// like `--ingester.push-circuit-breaker.enabled true`, which Go's flag package would take for a
// positional argument.
func parseFlags(flags *flag.FlagSet, args []string) error {
	flags.SetOutput(io.Discard)
	joined := make([]string, 0, len(args))
	for index := 0; index < len(args); index++ {
		arg := args[index]
		if strings.HasPrefix(arg, "-") && !strings.Contains(arg, "=") && index+1 < len(args) {
			next := args[index+1]
			if found := flags.Lookup(strings.TrimLeft(arg, "-")); found != nil && isBoolFlag(found) && (next == "true" || next == "false") {
				joined = append(joined, arg+"="+next)
				index++
				continue
			}
		}
		joined = append(joined, arg)
	}
	if err := flags.Parse(joined); err != nil {
		return err
	}
	if flags.NArg() > 0 {
		return fmt.Errorf("unexpected argument %q", flags.Arg(0))
	}
	return nil
}

func isBoolFlag(f *flag.Flag) bool {
	boolFlag, ok := f.Value.(interface{ IsBoolFlag() bool })
	return ok && boolFlag.IsBoolFlag()
}

type seriesSummary struct {
	Labels              [][2]string       `json:"labels"`
	Samples             [][2]any          `json:"samples"`
	HistogramTimestamps []int64           `json:"histogram_timestamps"`
	Exemplars           []exemplarSummary `json:"exemplars"`
	CreatedTimestamp    int64             `json:"created_timestamp"`
}

type exemplarSummary struct {
	Timestamp int64       `json:"timestamp"`
	Value     float64     `json:"value"`
	Labels    [][2]string `json:"labels"`
}

type metadataSummary struct {
	Metric string `json:"metric"`
	Type   int32  `json:"type"`
	Help   string `json:"help"`
	Unit   string `json:"unit"`
}

type requestSummary struct {
	Source   int32             `json:"source"`
	Series   []seriesSummary   `json:"series"`
	Metadata []metadataSummary `json:"metadata"`
}

func summarize(request record.DecodedRequest) requestSummary {
	summary := requestSummary{Source: request.Source, Series: []seriesSummary{}, Metadata: []metadataSummary{}}
	for _, series := range request.Series {
		item := seriesSummary{
			Labels:              series.Labels,
			Samples:             [][2]any{},
			HistogramTimestamps: []int64{},
			Exemplars:           []exemplarSummary{},
			CreatedTimestamp:    series.CreatedTimestamp,
		}
		if item.Labels == nil {
			item.Labels = [][2]string{}
		}
		for _, sample := range series.Samples {
			item.Samples = append(item.Samples, [2]any{sample.TimestampMs, sample.Value})
		}
		for _, histogram := range series.Histograms {
			item.HistogramTimestamps = append(item.HistogramTimestamps, histogram.Timestamp)
		}
		for _, exemplar := range series.Exemplars {
			labels := [][2]string{}
			for _, label := range exemplar.Labels {
				labels = append(labels, [2]string{label.Name, label.Value})
			}
			item.Exemplars = append(item.Exemplars, exemplarSummary{Timestamp: exemplar.TimestampMs, Value: exemplar.Value, Labels: labels})
		}
		summary.Series = append(summary.Series, item)
	}
	for _, metadata := range request.Metadata {
		summary.Metadata = append(summary.Metadata, metadataSummary{
			Metric: metadata.MetricFamilyName, Type: int32(metadata.Type), Help: metadata.Help, Unit: metadata.Unit,
		})
	}
	return summary
}
