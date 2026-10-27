// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"errors"
	"sync"

	"github.com/grafana/dskit/ring"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// The querier proxy mode sends the target store-gateway of each call in these gRPC metadata keys.
// The keys must be the same as GRPCTeeTargetAddrMetadataKey and GRPCTeeTargetIDMetadataKey in pkg/querier.
const (
	targetAddrMetadataKey = "x-grpctee-target-addr"
	targetIDMetadataKey   = "x-grpctee-target-id"
)

// target is the backend instance that the client selected for a call.
type target struct {
	addr string
	id   string
}

// targetFromMetadata returns the target from the metadata, and removes the target keys from the metadata.
// If the metadata has no target address, ok is false.
func targetFromMetadata(md metadata.MD) (_ target, ok bool) {
	addrs := md.Get(targetAddrMetadataKey)
	ids := md.Get(targetIDMetadataKey)
	delete(md, targetAddrMetadataKey)
	delete(md, targetIDMetadataKey)

	if len(addrs) == 0 || addrs[0] == "" {
		return target{}, false
	}
	t := target{addr: addrs[0]}
	if len(ids) > 0 {
		t.id = ids[0]
	}
	return t, true
}

// instanceLookup finds an instance of a ring by its ID.
type instanceLookup interface {
	GetInstance(instanceID string) (ring.InstanceDesc, error)
}

// targetRouter resolves the backends of the calls that have a target.
type targetRouter struct {
	conns *connPool
	// secondaryRing is the ring of the secondary backend. It is nil if there is no secondary backend.
	secondaryRing instanceLookup
}

// backends returns the primary backend at the target address, and the secondary backend at the instance with
// the target ID in the secondary ring. The secondary backend is nil if that ring has no instance with that ID.
func (r *targetRouter) backends(t target, primaryName, secondaryName string) (primary, secondary teeBackend, err error) {
	conn, err := r.conns.get(t.addr)
	if err != nil {
		return nil, nil, err
	}
	primary = poolBackend{name: primaryName, conn: conn}

	if r.secondaryRing == nil || t.id == "" {
		return primary, nil, nil
	}
	inst, err := r.secondaryRing.GetInstance(t.id)
	if err != nil {
		// The target has no mirror in the secondary ring.
		return primary, nil, nil
	}
	conn, err = r.conns.get(inst.Addr)
	if err != nil {
		return nil, nil, err
	}
	return primary, poolBackend{name: secondaryName, conn: conn}, nil
}

// connPool holds one gRPC connection for each backend address. It creates the connections when they are first used.
// It does not close the connections to the addresses that are not used anymore.
type connPool struct {
	mtx   sync.Mutex
	conns map[string]*grpc.ClientConn
}

func newConnPool() *connPool {
	return &connPool{conns: map[string]*grpc.ClientConn{}}
}

func (p *connPool) get(addr string) (*grpc.ClientConn, error) {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	if conn, ok := p.conns[addr]; ok {
		return conn, nil
	}
	// The client uses the raw codec, so response frames return as opaque bytes.
	conn, err := grpc.NewClient(
		addr,
		grpc.WithDefaultCallOptions(grpc.ForceCodecV2(newFrameCodec())),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, err
	}
	p.conns[addr] = conn
	return conn, nil
}

// Close closes all connections.
func (p *connPool) Close() error {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	var errs []error
	for addr, conn := range p.conns {
		errs = append(errs, conn.Close())
		delete(p.conns, addr)
	}
	return errors.Join(errs...)
}

// poolBackend is a backend instance at a connection of the connPool.
type poolBackend struct {
	name string
	conn *grpc.ClientConn
}

func (b poolBackend) Name() string           { return b.name }
func (b poolBackend) Conn() *grpc.ClientConn { return b.conn }
