// SPDX-License-Identifier: AGPL-3.0-only

package cardinalitypb

import (
	"errors"
	"io"
)

// A streamed answer is sent in messages of at most MessageGroups groups and
// about MessageBytes. A group is never split, so one with hashes can exceed it.
const (
	MessageGroups = 10_000
	MessageBytes  = 1 << 20
)

// GroupsMessage is one message of a streamed series count answer.
type GroupsMessage interface {
	Size() int
	GetGroups() []*SeriesCountGroup
	SetGroups([]*SeriesCountGroup)
}

// SendGroups streams response with send in batches of at most maxGroups groups
// and about maxBytes. Other fields go on the first message, which is sent even
// with no groups. response is not modified.
func SendGroups[T any, M interface {
	*T
	GroupsMessage
}](send func(M) error, response M, maxGroups, maxBytes int) error {
	groups := response.GetGroups()
	first := *response
	msg := M(&first)
	msg.SetGroups(nil)
	size := msg.Size()
	for _, g := range groups {
		// Own size plus field tag and length.
		gs := g.Size()
		gs += 1 + sovCardinality(uint64(gs))
		if n := len(msg.GetGroups()); n > 0 && (size+gs > maxBytes || n >= maxGroups) {
			if err := send(msg); err != nil {
				return err
			}
			msg = M(new(T))
			size = 0
		}
		msg.SetGroups(append(msg.GetGroups(), g))
		size += gs
	}
	return send(msg)
}

// ReceiveGroups reads recv until io.EOF and returns the first message with
// every other's groups appended. each, if set, is called on every message
// before its groups are appended, and its error ends the read.
func ReceiveGroups[T any, M interface {
	*T
	GroupsMessage
}](recv func() (M, error), each func(M) error) (M, error) {
	var out M
	for {
		msg, err := recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		if each != nil {
			if err := each(msg); err != nil {
				return nil, err
			}
		}
		if out == nil {
			out = msg
			continue
		}
		out.SetGroups(append(out.GetGroups(), msg.GetGroups()...))
	}
	if out == nil {
		return nil, errors.New("SeriesCounts stream ended without a message")
	}
	return out, nil
}
