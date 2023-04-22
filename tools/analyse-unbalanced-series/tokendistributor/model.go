package tokendistributor

import (
	"fmt"
	"math"
)

const (
	SingleZone Zone = "zone-unique"
	LastZone   Zone = "zone-#"
)

var LastZoneInfo = *newZoneInfo(LastZone)

type zoneInfo struct {
	zone       Zone
	precededBy *zoneInfo
}

func newZoneInfo(zone Zone) *zoneInfo {
	return &zoneInfo{
		zone: zone,
	}
}

func (z *zoneInfo) String() string {
	if z.precededBy == nil {
		return string(z.zone)
	}
	return fmt.Sprintf("%s[precededBy %s]", string(z.zone), string(z.precededBy.zone))
}

type instanceInfo struct {
	instanceId        Instance
	zone              *zoneInfo
	ownership         uint32
	adjustedOwnership float64
	tokenCount        int
}

func newInstanceInfo(instanceId Instance, zone *zoneInfo, tokenCount int) *instanceInfo {
	return &instanceInfo{
		instanceId:        instanceId,
		zone:              zone,
		tokenCount:        tokenCount,
		ownership:         0,
		adjustedOwnership: 0,
	}
}

func (i *instanceInfo) addTokens(tokenCount int) {
	i.tokenCount += tokenCount
}

func (i *instanceInfo) String() string {
	return fmt.Sprintf("instanceInfo{%s-%s-%d-%d}", i.instanceId, i.zone, i.tokenCount, i.ownership)
}

func (i *instanceInfo) toStringVerbose() string {
	return fmt.Sprintf("instanceInfo{instanceId:%s,zone:%s,tokenCount:%d,ownership:%d,adjustedOwnership:%f}", i.instanceId, i.zone, i.tokenCount, i.ownership, i.adjustedOwnership)
}

type tokenInfoInterface interface {
	// getToken returns the token related to this tokenInfoInterface
	getToken() Token

	// getOwiningInstance returns the instance related to this tokenInfoInterface
	getOwningInstance() *instanceInfo

	// getReplicaStart returns the farthest token in the ring whose replication ends in the given instance with
	// the given key it is the token that succeeds the closest token of the same zone as this tokenInfoInterface
	getReplicaStart() Token

	// setReplicaStart sets the replica set for this tokenInfoInterface. Replica set is the farthest away token
	// in the ring whose replication ends in the given instance with the given key it is the token that succeeds
	// the closest token of the same zone as this tokenInfoInterface
	setReplicaStart(token Token)

	// isExpandable is true if it is possible to put a new token in front of the replica start of this tokenInfoInterface
	isExpandable() bool

	// setExpandable sets the expandability of this tokenInfoInterface. A tokenInfoInterface is true if it is possible
	// to put a new token in front of the replica start of this tokenInfoInterface
	setExpandable(expandable bool)

	// getReplicatedOwnership returns the size of the range of tokens that are owned by this token
	getReplicatedOwnership() uint32

	// setReplicatedOwnership sets the replicatedOwnership, i.r., the size of the range of tokens that are owned by
	// this tokenInfoInterface
	setReplicatedOwnership(replicatedOwnership uint32)
}

type navigableTokenInfo struct {
	instance            *instanceInfo
	token               Token
	replicaStart        Token
	expandable          bool
	replicatedOwnership uint32
	prev, next          *navigableTokenInfo
}

type tokenInfo struct {
	instance            *instanceInfo
	token               Token
	replicaStart        Token
	expandable          bool
	replicatedOwnership uint32
	navigableToken      *navigableToken[navigableTokenInterface]
	prev, next          *navigableTokenInfo
}

func newTokenInfo(instance *instanceInfo, token Token) *tokenInfo {
	return &tokenInfo{
		instance: instance,
		token:    token,
	}
}

// implementation of navigableTokenInterface
func (ti *tokenInfo) getNavigableToken() *navigableToken[navigableTokenInterface] {
	return ti.navigableToken
}

func (ti *tokenInfo) setNavigableToken(navigableToken *navigableToken[navigableTokenInterface]) {
	ti.navigableToken = navigableToken
}

// implementation of tokenInfoInterface
func (ti *tokenInfo) getToken() Token {
	return ti.token
}

func (ti *tokenInfo) getOwningInstance() *instanceInfo {
	return ti.instance
}

func (ti *tokenInfo) getReplicaStart() Token {
	return ti.replicaStart
}

func (ti *tokenInfo) setReplicaStart(replicaStart Token) {
	ti.replicaStart = replicaStart
}

func (ti *tokenInfo) isExpandable() bool {
	return ti.expandable
}

func (ti *tokenInfo) setExpandable(expandable bool) {
	ti.expandable = expandable
}

func (ti *tokenInfo) getReplicatedOwnership() uint32 {
	return ti.replicatedOwnership
}

func (ti *tokenInfo) setReplicatedOwnership(replicatedOwnership uint32) {
	ti.replicatedOwnership = replicatedOwnership
}

func (ti *tokenInfo) getPrevious() navigableTokenInterface {
	return ti.getNavigableToken().getPrev()
}

func (ti *tokenInfo) String() string {
	return fmt.Sprintf("tokenInfo{%s,t:%d,replicaStart:%d,replicatedOwnership:%d,exp:%v", ti.instance, ti.token, ti.replicaStart, ti.replicatedOwnership, ti.expandable)
}

type candidateTokenInfo struct {
	tokenInfo
	host *tokenInfo
}

func newCandidateTokenInfo(instance *instanceInfo, token Token, host *tokenInfo) *candidateTokenInfo {
	tokenInfo := newTokenInfo(instance, token)
	return &candidateTokenInfo{
		tokenInfo: *tokenInfo,
		host:      host,
	}
}

func (ci *candidateTokenInfo) getPrevious() navigableTokenInterface {
	return ci.host
}

func (ci candidateTokenInfo) String() string {
	return fmt.Sprintf("candidateTokenInfo{%s,t:%d,hostToken:%d,replicaStart:%d,replicatedOwnership:%d,exp:%v", ci.instance, ci.token, ci.host.getToken(), ci.replicaStart, ci.replicatedOwnership, ci.expandable)
}

type Token uint32

const maxTokenValue = math.MaxUint32

func (t Token) distance(next, maxTokenValue Token) uint32 {
	if next < t {
		return uint32(maxTokenValue - t + next)
	}
	return uint32(next - t)
}

func (t Token) split(next, maxTokenValue Token) Token {
	dist := (t.distance(next, maxTokenValue) + 1) / 2
	return Token(uint32(t) + dist)
}

type Instance string

type Zone string
