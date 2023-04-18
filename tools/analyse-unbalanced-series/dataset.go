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

func getRingStatus(mimirRingPageURL string) (ringStatusDesc, error) {
	req, err := http.NewRequest("GET", mimirRingPageURL+"?tokens=true", nil)
	if err != nil {
		return ringStatusDesc{}, errors.Wrap(err, "unable to prepare request to read ring status")
	}

	req.Header.Set("Accept", "application/json")

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return ringStatusDesc{}, errors.Wrap(err, "unable to read ring status")
	}

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return ringStatusDesc{}, errors.Wrap(err, "unable to read ring status")
	}

	dec := json.NewDecoder(bytes.NewReader(body))
	data := ringStatusDesc{}
	if err := dec.Decode(&data); err != nil {
		return ringStatusDesc{}, errors.Wrap(err, "unable to decode ring status")
	}

	return data, nil
}

type ringStatusDesc struct {
	Ingesters []ringInstanceDesc `json:"shards"`
	Now       time.Time          `json:"now"`
}

func (d ringStatusDesc) toRingModel() *ring.Desc {
	desc := &ring.Desc{
		Ingesters: make(map[string]ring.InstanceDesc, len(d.Ingesters)),
	}

	for _, instance := range d.Ingesters {
		desc.Ingesters[instance.ID] = ring.InstanceDesc{
			Addr:                instance.Address,
			Timestamp:           time.Now().Unix(),
			State:               ring.ACTIVE,
			Tokens:              instance.Tokens,
			Zone:                instance.Zone,
			RegisteredTimestamp: instance.RegisteredTimestamp.Unix(),
		}
	}

	return desc
}

type ringInstanceDesc struct {
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
