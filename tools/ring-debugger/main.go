package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/kv/consul"
	"github.com/grafana/dskit/ring"

	"github.com/grafana/mimir/pkg/compactor"
	utillog "github.com/grafana/mimir/pkg/util/log"
)

func main() {
	f, err := ioutil.ReadFile("ring.json")
	if err != nil {
		panic(err)
	}

	log.Println("Loading ring")
	rd := &ring.Desc{}
	err = json.Unmarshal(f, rd)
	if err != nil {
		panic(err)
	}

	kvc, _ := consul.NewInMemoryClient(ring.GetCodec(), utillog.Logger, nil)

	r, err := ring.New(ring.Config{
		KVStore: kv.Config{
			Mock: kvc,
		},
		HeartbeatTimeout:     1000 * time.Minute,
		ReplicationFactor:    1,
		ZoneAwarenessEnabled: false,
		ExcludedZones:        nil,
		SubringCacheDisabled: false,
	}, "compactor", "compactor", utillog.Logger, nil)
	if err != nil {
		panic(err)
	}
	r.UpdateRingState(rd)

	s := bufio.NewScanner(os.Stdin)
	for s.Scan() {
		t := s.Text()
		v, err := strconv.ParseInt(s.Text(), 10, 64)
		if err != nil {
			log.Println("invalid number:", t)
			continue
		}

		v32 := uint32(v)
		if int64(v32) != v {
			log.Println("invalid conversion:", v32, v)
		}

		rs, err := r.Get(uint32(v), compactor.RingOp, nil, nil, nil)
		if err != nil {
			log.Println("invalid ring op:", err)
			continue
		}

		for _, i := range rs.Instances {
			fmt.Println(i.Addr)
		}
	}
}
