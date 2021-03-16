package contentpubsub

import (
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/peerstore"

	swarmt "github.com/libp2p/go-libp2p-swarm/testing"
	bhost "github.com/libp2p/go-libp2p/p2p/host/basic"

	ma "github.com/multiformats/go-multiaddr"

	dht "github.com/libp2p/go-libp2p-kad-dht"
)

// The following functions and types were extracted from
// go-libp2p-kad-dht to test the dht operations of this work
type blankValidator struct{}

func (blankValidator) Validate(_ string, _ []byte) error        { return nil }
func (blankValidator) Select(_ string, _ [][]byte) (int, error) { return 0, nil }

var testPrefix = dht.ProtocolPrefix("/test")

func setupDHT(ctx context.Context, t *testing.T, client bool, options ...dht.Option) *dht.IpfsDHT {
	baseOpts := []dht.Option{
		testPrefix,
		dht.NamespacedValidator("v", blankValidator{}),
		dht.DisableAutoRefresh(),
	}

	if client {
		baseOpts = append(baseOpts, dht.Mode(dht.ModeClient))
	} else {
		baseOpts = append(baseOpts, dht.Mode(dht.ModeServer))
	}

	d, err := dht.New(
		ctx,
		bhost.New(swarmt.GenSwarm(t, ctx, swarmt.OptDisableReuseport)),
		append(baseOpts, options...)...,
	)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func setupDHTS(t *testing.T, ctx context.Context, n int, options ...dht.Option) []*dht.IpfsDHT {
	addrs := make([]ma.Multiaddr, n)
	dhts := make([]*dht.IpfsDHT, n)
	peers := make([]peer.ID, n)

	sanityAddrsMap := make(map[string]struct{})
	sanityPeersMap := make(map[string]struct{})

	for i := 0; i < n; i++ {
		dhts[i] = setupDHT(ctx, t, false, options...)
		peers[i] = dhts[i].PeerID()
		addrs[i] = dhts[i].Host().Addrs()[0]

		if _, lol := sanityAddrsMap[addrs[i].String()]; lol {
			t.Fatal("While setting up DHTs address got duplicated.")
		} else {
			sanityAddrsMap[addrs[i].String()] = struct{}{}
		}
		if _, lol := sanityPeersMap[peers[i].String()]; lol {
			t.Fatal("While setting up DHTs peerid got duplicated.")
		} else {
			sanityPeersMap[peers[i].String()] = struct{}{}
		}
	}

	return dhts
}

func connectNoSync(t *testing.T, ctx context.Context, a, b *dht.IpfsDHT) {
	t.Helper()

	idB := b.Host().ID()
	addrB := b.Host().Peerstore().Addrs(idB)
	if len(addrB) == 0 {
		t.Fatal("peers setup incorrectly: no local address")
	}

	a.Host().Peerstore().AddAddrs(idB, addrB, peerstore.TempAddrTTL)
	pi := peer.AddrInfo{ID: idB}
	if err := a.Host().Connect(ctx, pi); err != nil {
		t.Fatal(err)
	}
}

func wait(t *testing.T, ctx context.Context, a, b *dht.IpfsDHT) {
	t.Helper()

	for a.RoutingTable().Find(b.Host().ID()) == "" {
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Millisecond * 5):
		}
	}
}

func connect(t *testing.T, ctx context.Context, a, b *dht.IpfsDHT) {
	t.Helper()
	connectNoSync(t, ctx, a, b)
	wait(t, ctx, a, b)
	wait(t, ctx, b, a)
}

// TestIpfsDHTSimpleInitialization only attempts to show DHT operation
func TestIpfsDHTSimpleInitialization(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()

	dhts := setupDHTS(t, ctx, 10)

	connect(t, ctx, dhts[8], dhts[9])
	connect(t, ctx, dhts[7], dhts[8])
	connect(t, ctx, dhts[7], dhts[9])

	connect(t, ctx, dhts[5], dhts[6])
	connect(t, ctx, dhts[4], dhts[5])
	connect(t, ctx, dhts[6], dhts[4])

	connect(t, ctx, dhts[2], dhts[3])
	connect(t, ctx, dhts[1], dhts[2])
	connect(t, ctx, dhts[1], dhts[3])

	connect(t, ctx, dhts[2], dhts[0])
	connect(t, ctx, dhts[5], dhts[0])
	connect(t, ctx, dhts[8], dhts[0])

	if dhts[0].RoutingTable().Size() != 3 ||
		dhts[1].RoutingTable().Size() != 2 ||
		dhts[2].RoutingTable().Size() != 3 ||
		dhts[3].RoutingTable().Size() != 2 ||
		dhts[4].RoutingTable().Size() != 2 ||
		dhts[5].RoutingTable().Size() != 3 {

		t.Fatal("Failed to initialize dhts")
	}
}
