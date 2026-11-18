// SPDX-License-Identifier: AGPL-3.0-only

package cardinalitypb

import (
	"errors"
	"io"
)

// A streamed answer is sent in messages of at most MessageGroups groups and
// about MessageBytes. A group with more hashes than fit in one message is split.
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
// and about maxBytes. A group whose hashes are over maxBytes is sent as several
// groups of the same value, each with some of the hashes. Other fields go on the
// first message, which is sent even with no groups. response is not modified.
func SendGroups[T any, M interface {
	*T
	GroupsMessage
}](send func(M) error, response M, maxGroups, maxBytes int) error {
	groups := response.GetGroups()
	first := *response
	msg := M(&first)
	msg.SetGroups(nil)
	size := msg.Size()
	for _, whole := range groups {
		for _, g := range splitHashes(whole, maxBytes) {
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
	}
	return send(msg)
}

// splitHashes returns g, or for a group of hashes over maxBytes, parts of g's
// value that each hold at most about maxBytes of its hashes. g is not modified.
func splitHashes(g *SeriesCountGroup, maxBytes int) []*SeriesCountGroup {
	// A packed hash takes at most 10 bytes.
	perPart := max(1, (maxBytes-len(g.Value)-16)/10)
	if len(g.Hashes) <= perPart {
		return []*SeriesCountGroup{g}
	}
	parts := make([]*SeriesCountGroup, 0, (len(g.Hashes)+perPart-1)/perPart)
	for hashes := g.Hashes; len(hashes) > 0; {
		n := min(perPart, len(hashes))
		parts = append(parts, &SeriesCountGroup{Value: g.Value, Hashes: hashes[:n:n]})
		hashes = hashes[n:]
	}
	return parts
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
