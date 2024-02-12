package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net/textproto"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	keysFile, err := os.Open(os.Args[1])
	if err != nil {
		fmt.Printf("Cannot open keys file: %s\n", err)
		os.Exit(1)
	}

	blocksFile, err := os.Open(os.Args[2])
	if err != nil {
		fmt.Printf("Cannot open blocks file: %s\n", err)
		os.Exit(1)
	}

	keysReader := bufio.NewReader(keysFile)
	keysText := textproto.NewReader(keysReader)
	sizes, err := sizeByBlock(keysText)
	if err != nil {
		fmt.Printf("Error parsing keys: %s\n", err)
		os.Exit(1)
	}

	durationsReader := bufio.NewReader(blocksFile)
	durationsText := textproto.NewReader(durationsReader)
	durations, err := durationByBlock(durationsText)
	if err != nil {
		fmt.Printf("Error parsing blocks: %s\n", err)
		os.Exit(1)
	}

	merged := mergeIndexes(sizes, durations)
	for k, v := range merged {
		fmt.Printf("%s\t%d\n", k, v)
	}
}

func sizeByBlock(text *textproto.Reader) (map[string]uint64, error) {
	byBlock := make(map[string]uint64)

	for {
		line, err := text.ReadLine()
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return nil, err
		}

		parts := strings.Fields(line)
		if len(parts) != 3 {
			return nil, fmt.Errorf("unexpected line format: %s", line)
		}

		keyParts := strings.Split(parts[0], ":")
		if len(keyParts) < 3 {
			return nil, fmt.Errorf("unexpected key format: %s", parts[0])
		}

		block := keyParts[2]
		size, err := strconv.ParseUint(parts[2], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("cannot parse item size: %s: %w", parts[2], err)
		}

		byBlock[block] += size
	}

	return byBlock, nil
}

func durationByBlock(text *textproto.Reader) (map[string]time.Duration, error) {
	byBlock := make(map[string]time.Duration)

	// Throw away the first format line
	_, _ = text.ReadLine()

	for {
		line, err := text.ReadLine()
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return nil, err
		}

		parts := strings.Fields(line)
		block := parts[0]
		duration, err := time.ParseDuration(parts[4])
		if err != nil {
			return nil, fmt.Errorf("cannot parse block duration: %s: %w", parts[4], err)
		}

		duration = duration.Round(time.Minute)
		if duration < time.Hour {
			fmt.Printf("DURATION: %s\n", duration)
		}


		byBlock[block] = duration
	}

	return byBlock, nil
}

func mergeIndexes(sizes map[string]uint64, durations map[string]time.Duration) map[time.Duration]uint64 {
	out := make(map[time.Duration]uint64)


	for block, size := range sizes {
		duration, ok := durations[block]
		if !ok {
			fmt.Printf("can't find duration for block %s\n", block)
		}

		out[duration] += size
	}

	return out
}
