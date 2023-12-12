package kfake

import (
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func init() { regKey(8, 0, 8) }

func (c *Cluster) handleOffsetCommit(creq clientReq) (kmsg.Response, error) {
	req := creq.kreq.(*kmsg.OffsetCommitRequest)
	resp := req.ResponseKind().(*kmsg.OffsetCommitResponse)

	if err := checkReqVersion(req.Key(), req.Version); err != nil {
		return nil, err
	}

	if c.groups.handleOffsetCommit(creq) {
		return nil, nil
	}

	fillOffsetCommit(req, resp, kerr.GroupIDNotFound.Code)
	return resp, nil
}
