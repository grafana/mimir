package main

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/runutil"
	"golang.org/x/exp/slices"

	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storage/tsdb/bucketindex"
)

func analyzeStoreGatewayActualBlocksOwnership(blocks bucketindex.Blocks, ringDesc *ring.Desc, logger log.Logger) error {
	const (
		replicationFactor    = 3
		zoneAwarenessEnabled = true
	)

	var (
		ringTokens          = ringDesc.GetTokens()
		ringInstanceByToken = getRingInstanceByToken(ringDesc)
	)

	result, err := getStoreGatewayActualBlocksOwnership(blocks, ringDesc, ringTokens, ringInstanceByToken, replicationFactor, zoneAwarenessEnabled)
	if err != nil {
		return err
	}

	w := newCSVWriter[instanceOwnership]()
	w.setHeader([]string{"pod", fmt.Sprintf("Blocks ownership zone-aware=%s RF=%d", formatEnabled(zoneAwarenessEnabled), replicationFactor)})
	w.setData(result, func(entry instanceOwnership) []string {
		// To make the percentage easy to compare with different RFs, we divide it by the RF.
		return []string{entry.id, fmt.Sprintf("%.3f", entry.percentage/float64(replicationFactor))}
	})
	if err := w.writeCSV(fmt.Sprintf("store-gateway-blocks-ownership-with-zone-aware-%s-and-rf-%d.csv", formatEnabled(zoneAwarenessEnabled), replicationFactor)); err != nil {
		return err
	}

	return nil
}

func getStoreGatewayActualBlocksOwnership(blocks bucketindex.Blocks, ringDesc *ring.Desc, ringTokens []uint32, ringInstanceByToken map[uint32]instanceInfo, replicationFactor int, zoneAwarenessEnabled bool) ([]instanceOwnership, error) {
	var (
		bufDescs [ring.GetBufferSize]string
		bufHosts [ring.GetBufferSize]string
		bufZones [ring.GetBufferSize]string
	)

	ownedBlocks := map[string]int{}

	for _, block := range blocks {
		key := mimir_tsdb.HashBlockID(block.ID)

		instanceIDs, err := ringGet(key, ringDesc, ringTokens, ringInstanceByToken, ring.WriteNoExtend, replicationFactor, zoneAwarenessEnabled, bufDescs[:0], bufHosts[:0], bufZones[:0])
		if err != nil {
			return nil, err
		}

		for _, instanceID := range instanceIDs {
			ownedBlocks[instanceID]++
		}
	}

	// Compute the per-instance % of owned blocks.
	var result []instanceOwnership

	for id, numBlocks := range ownedBlocks {
		result = append(result, instanceOwnership{
			id:         id,
			percentage: (float64(numBlocks) / float64(len(blocks))) * 100,
		})
	}

	slices.SortFunc(result, func(a, b instanceOwnership) bool {
		return a.id < b.id
	})

	return result, nil
}

func readBucketIndex(bucketIndexFilepath string, logger log.Logger) (*bucketindex.Index, error) {
	content, err := os.ReadFile(bucketIndexFilepath)
	if err != nil {
		return nil, err
	}

	// Read all the content.
	gzipReader, err := gzip.NewReader(strings.NewReader(string(content)))
	if err != nil {
		return nil, err
	}
	defer runutil.CloseWithLogOnErr(logger, gzipReader, "close bucket index gzip reader")

	// Deserialize it.
	index := &bucketindex.Index{}
	d := json.NewDecoder(gzipReader)
	if err := d.Decode(index); err != nil {
		return nil, err
	}

	return index, nil
}
