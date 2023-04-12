package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"time"

	"github.com/grafana/dskit/ring"
	"github.com/pkg/errors"
)

func getRingStatus(mimirDistributorURL string) (ringStatusDesc, error) {
	req, err := http.NewRequest("GET", mimirDistributorURL+"/ingester/ring?tokens=true", nil)
	if err != nil {
		return ringStatusDesc{}, errors.Wrap(err, "unable to prepare request to read ingesters ring status")
	}

	req.Header.Set("Accept", "application/json")

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return ringStatusDesc{}, errors.Wrap(err, "unable to read ingesters ring status")
	}

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return ringStatusDesc{}, errors.Wrap(err, "unable to read ingesters ring status")
	}

	dec := json.NewDecoder(bytes.NewReader(body))
	data := ringStatusDesc{}
	if err := dec.Decode(&data); err != nil {
		return ringStatusDesc{}, errors.Wrap(err, "unable to decode ingesters ring status")
	}

	return data, nil
}

type ringStatusDesc struct {
	Ingesters []ringIngesterDesc `json:"shards"`
	Now       time.Time          `json:"now"`
}

func (d ringStatusDesc) toRingModel() *ring.Desc {
	desc := &ring.Desc{
		Ingesters: make(map[string]ring.InstanceDesc, len(d.Ingesters)),
	}

	for _, ingester := range d.Ingesters {
		desc.Ingesters[ingester.ID] = ring.InstanceDesc{
			Addr:                ingester.Address,
			Timestamp:           time.Now().Unix(),
			State:               ring.ACTIVE,
			Tokens:              ingester.Tokens,
			Zone:                ingester.Zone,
			RegisteredTimestamp: ingester.RegisteredTimestamp.Unix(),
		}
	}

	return desc
}

type ringIngesterDesc struct {
	ID                  string    `json:"id"`
	State               string    `json:"state"`
	Address             string    `json:"address"`
	HeartbeatTimestamp  time.Time `json:"timestamp"`
	RegisteredTimestamp time.Time `json:"registered_timestamp"`
	Zone                string    `json:"zone"`
	Tokens              []uint32  `json:"tokens"`
	NumTokens           int       `json:"-"`
	Ownership           float64   `json:"-"`
}
