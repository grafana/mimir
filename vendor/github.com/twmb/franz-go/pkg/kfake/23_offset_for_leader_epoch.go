package kfake

import (
	"sort"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func init() { regKey(23, 3, 4) }

func (c *Cluster) handleOffsetForLeaderEpoch(b *broker, kreq kmsg.Request) (kmsg.Response, error) {
	req := kreq.(*kmsg.OffsetForLeaderEpochRequest)
	resp := req.ResponseKind().(*kmsg.OffsetForLeaderEpochResponse)

	if err := checkReqVersion(req.Key(), req.Version); err != nil {
		return nil, err
	}

	tidx := make(map[string]int)
	donet := func(t string, errCode int16) *kmsg.OffsetForLeaderEpochResponseTopic {
		if i, ok := tidx[t]; ok {
			return &resp.Topics[i]
		}
		tidx[t] = len(resp.Topics)
		st := kmsg.NewOffsetForLeaderEpochResponseTopic()
		st.Topic = t
		resp.Topics = append(resp.Topics, st)
		return &resp.Topics[len(resp.Topics)-1]
	}
	donep := func(t string, p int32, errCode int16) *kmsg.OffsetForLeaderEpochResponseTopicPartition {
		sp := kmsg.NewOffsetForLeaderEpochResponseTopicPartition()
		sp.Partition = p
		sp.ErrorCode = errCode
		st := donet(t, 0)
		st.Partitions = append(st.Partitions, sp)
		return &st.Partitions[len(st.Partitions)-1]
	}

	for _, rt := range req.Topics {
		ps, ok := c.data.tps.gett(rt.Topic)
		for _, rp := range rt.Partitions {
			if req.ReplicaID != -1 {
				donep(rt.Topic, rp.Partition, kerr.UnknownServerError.Code)
				continue
			}
			if !ok {
				donep(rt.Topic, rp.Partition, kerr.UnknownTopicOrPartition.Code)
				continue
			}
			pd, ok := ps[rp.Partition]
			if !ok {
				donep(rt.Topic, rp.Partition, kerr.UnknownTopicOrPartition.Code)
				continue
			}
			if pd.leader != b {
				donep(rt.Topic, rp.Partition, kerr.NotLeaderForPartition.Code)
				continue
			}
			if rp.CurrentLeaderEpoch < pd.epoch {
				donep(rt.Topic, rp.Partition, kerr.FencedLeaderEpoch.Code)
				continue
			} else if rp.CurrentLeaderEpoch > pd.epoch {
				donep(rt.Topic, rp.Partition, kerr.UnknownLeaderEpoch.Code)
				continue
			}

			sp := donep(rt.Topic, rp.Partition, 0)

			// If the user is requesting our current epoch, we return the HWM.
			if rp.LeaderEpoch == pd.epoch {
				sp.LeaderEpoch = pd.epoch
				sp.EndOffset = pd.highWatermark
				continue
			}

			// What is the largest epoch after the requested epoch?
			idx, _ := sort.Find(len(pd.batches), func(idx int) int {
				batchEpoch := pd.batches[idx].epoch
				switch {
				case rp.LeaderEpoch <= batchEpoch:
					return -1
				default:
					return 1
				}
			})

			// Requested epoch is not yet known: keep -1 returns.
			if idx == len(pd.batches) {
				sp.LeaderEpoch = -1
				sp.EndOffset = -1
				continue
			}

			// Requested epoch is before the LSO: return the requested
			// epoch and the LSO.
			if idx == 0 && pd.batches[0].epoch > rp.LeaderEpoch {
				sp.LeaderEpoch = rp.LeaderEpoch
				sp.EndOffset = pd.logStartOffset
				continue
			}

			// The requested epoch exists and is not the latest
			// epoch, we return the end offset being the first
			// offset of the next epoch.
			sp.LeaderEpoch = pd.batches[idx].epoch
			sp.EndOffset = pd.batches[idx+1].FirstOffset
		}
	}
	return resp, nil
}
